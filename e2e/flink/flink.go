// Package flink builds the normalizer job jars and runs them on the Flink cluster.
package flink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// jobModules are the normalizer modules in submission order. The order is
// DOWNSTREAM-FIRST because every source reads from `latest`: a job started after
// its upstream would miss whatever the upstream produced in between.
var jobModules = []string{
	"job-aggregator",
	"job-book-builder",
	"job-precision",
	"job-rebaser",
	"job-type-validator",
	"job-pair-extractor",
}

const (
	cancelTimeout = time.Minute
	startTimeout  = 2 * time.Minute
	pollInterval  = time.Second
	// checkpointTimeout bounds WaitForCheckpoint. Generous relative to
	// CheckpointingConfigurer's 10s interval + 120s timeout — a slow first
	// checkpoint under test-harness load should not flake the caller.
	checkpointTimeout = 3 * time.Minute
)

// RunJobs builds the normalizer modules and submits every job jar,
// downstream-first. The cluster must already be idle: call CancelJobs first.
func RunJobs(ctx context.Context, api, normalizerDir string) error {
	if err := build(ctx, normalizerDir); err != nil {
		return err
	}

	for _, module := range jobModules {
		jar, err := jarPath(normalizerDir, module)
		if err != nil {
			return err
		}
		if err := submit(ctx, api, module, jar); err != nil {
			return fmt.Errorf("%s: %w", module, err)
		}
	}
	return nil
}

// build runs one reactor build for all modules. run-job.sh builds each module
// separately with -am, which rebuilds common six times for the same jars.
//
// clean is not optional. Without it a stale target/ fails as WRONG DATA rather
// than as an error: an incremental compile skips whenever the class mtimes are
// not older than the sources (any copy/rsync/tar that preserves mtimes leaves a
// checkout in that state), so the harness ships a jar built from pre-change
// source, the job runs fine, and the only symptom is a new field arriving as its
// Avro default on every scenario. That cost a full debugging session once.
//
// Maven runs once per scenario and is loud even with -q, so its output is held
// back and replayed with the error when the build fails. Debug streams it live.
func build(ctx context.Context, normalizerDir string) error {
	slog.Debug("building normalizer jobs")

	cmd := exec.CommandContext(ctx, "mvn", "-f", filepath.Join(normalizerDir, "pom.xml"), "clean", "package", "-q", "-DskipTests")

	var out bytes.Buffer
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	} else {
		cmd.Stdout, cmd.Stderr = &out, &out
	}

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mvn clean package: %w\n%s", err, out.String())
	}
	return nil
}

// jarPath finds a module's shaded jar, skipping the original-* copy shade
// leaves behind. The artifactId may differ from the module directory name.
func jarPath(normalizerDir, module string) (string, error) {
	matches, err := filepath.Glob(filepath.Join(normalizerDir, module, "target", "*-1.0-SNAPSHOT.jar"))
	if err != nil {
		return "", err
	}
	for _, jar := range matches {
		if !strings.HasPrefix(filepath.Base(jar), "original-") {
			return jar, nil
		}
	}
	return "", fmt.Errorf("no jar found in %s/target", module)
}

// submit uploads a jar and starts it. The entry point is the jar manifest's
// Main-Class, set by each module's shade config, so no class is passed.
func submit(ctx context.Context, api, module, jar string) error {
	jarID, err := upload(ctx, api, jar)
	if err != nil {
		return err
	}

	var run struct {
		JobID string `json:"jobid"`
	}
	if err := do(ctx, http.MethodPost, api+"/jars/"+jarID+"/run", nil, "", &run); err != nil {
		return err
	}
	if run.JobID == "" {
		return fmt.Errorf("submit returned no job id")
	}

	if err := waitRunning(ctx, api, run.JobID); err != nil {
		return err
	}
	slog.Debug("flink job running", "module", module, "job_id", run.JobID)
	return nil
}

func upload(ctx context.Context, api, jar string) (string, error) {
	f, err := os.Open(jar)
	if err != nil {
		return "", err
	}
	defer f.Close()

	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	part, err := form.CreateFormFile("jarfile", filepath.Base(jar))
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(part, f); err != nil {
		return "", err
	}
	if err := form.Close(); err != nil {
		return "", err
	}

	// Flink answers with the jar's path on the cluster; its last segment is the id.
	var resp struct {
		Filename string `json:"filename"`
	}
	if err := do(ctx, http.MethodPost, api+"/jars/upload", &body, form.FormDataContentType(), &resp); err != nil {
		return "", err
	}
	if resp.Filename == "" {
		return "", fmt.Errorf("upload returned no filename")
	}
	return path.Base(resp.Filename), nil
}

type jobSummary struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

func listJobs(ctx context.Context, api string) ([]jobSummary, error) {
	var list struct {
		Jobs []jobSummary `json:"jobs"`
	}
	if err := do(ctx, http.MethodGet, api+"/jobs", nil, "", &list); err != nil {
		return nil, err
	}
	return list.Jobs, nil
}

// RunningJobIDs returns the ids of every job currently RUNNING or RESTARTING.
// A caller that is about to inject a crash captures this beforehand and
// compares it against what WaitAllRunning finds afterwards — the ids must be
// the SAME ones, because a Flink job recovering in place from a checkpoint
// keeps its id; only a fresh submission through warmup.Run would mint new
// ones, and that is not what a TaskManager crash does.
func RunningJobIDs(ctx context.Context, api string) ([]string, error) {
	jobs, err := listJobs(ctx, api)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, job := range jobs {
		if job.Status == "RUNNING" || job.Status == "RESTARTING" {
			ids = append(ids, job.ID)
		}
	}
	return ids, nil
}

// JobIDsByName returns the id of every RUNNING or RESTARTING job, keyed by
// its Flink job name (the string each job's main() passes to env.execute).
// /jobs/overview carries the name; /jobs (used by listJobs / CancelJobs)
// does not, which is why this hits a different endpoint rather than adding
// a field there.
func JobIDsByName(ctx context.Context, api string) (map[string]string, error) {
	var list struct {
		Jobs []struct {
			ID     string `json:"jid"`
			Name   string `json:"name"`
			Status string `json:"state"`
		} `json:"jobs"`
	}
	if err := do(ctx, http.MethodGet, api+"/jobs/overview", nil, "", &list); err != nil {
		return nil, err
	}
	ids := make(map[string]string, len(list.Jobs))
	for _, job := range list.Jobs {
		if job.Status == "RUNNING" || job.Status == "RESTARTING" {
			ids[job.Name] = job.ID
		}
	}
	return ids, nil
}

// RestartTaskManagers simulates a TaskManager crash without touching the
// JobManager or resubmitting anything: `docker compose restart` on the named
// services kills every task running there mid-stream, the same way a host
// OOM or container reschedule would. Flink's own failover — not this harness
// — is what recovers the affected jobs once the container reconnects and
// re-registers, restoring each one from its own last completed checkpoint.
// The dev stack's single TaskManager service is "taskmanager"; the prod
// stack's four are "taskmanager-1".."taskmanager-4".
func RestartTaskManagers(ctx context.Context, composeFile string, services ...string) error {
	cmd := exec.CommandContext(ctx, "docker",
		append([]string{"compose", "-f", composeFile, "restart"}, services...)...)

	var out bytes.Buffer
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	} else {
		cmd.Stdout, cmd.Stderr = &out, &out
	}
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose restart %s: %w\n%s", strings.Join(services, " "), err, out.String())
	}
	return nil
}

// WaitForCheckpoint waits until id has completed at least one checkpoint —
// proof that a subsequent crash actually has committed state to recover,
// rather than restarting into emptiness that would look identical to a
// successful recovery.
func WaitForCheckpoint(ctx context.Context, api, id string) error {
	return poll(ctx, checkpointTimeout, func() (bool, error) {
		var resp struct {
			Latest struct {
				Completed *struct {
					ID int64 `json:"id"`
				} `json:"completed"`
			} `json:"latest"`
		}
		if err := do(ctx, http.MethodGet, api+"/jobs/"+id+"/checkpoints", nil, "", &resp); err != nil {
			return false, err
		}
		return resp.Latest.Completed != nil, nil
	})
}

// WaitAllRunning waits until every id in ids is back to RUNNING — the shape a
// job takes once Flink's own failover has restored it from its last
// checkpoint after a TaskManager crash. A job that instead reaches FAILED or
// CANCELED is recovery failing, not succeeding, and is reported as an error.
func WaitAllRunning(ctx context.Context, api string, ids []string) error {
	for _, id := range ids {
		if err := waitRunning(ctx, api, id); err != nil {
			return fmt.Errorf("job %s did not recover: %w", id, err)
		}
	}
	return nil
}

// CancelJobs cancels every running job and waits for each to reach a terminal
// state, so their task slots are free before new jobs are submitted — and so
// nothing is still consuming the topics when they are deleted.
func CancelJobs(ctx context.Context, api string) error {
	jobs, err := listJobs(ctx, api)
	if err != nil {
		return err
	}

	var ids []string
	for _, job := range jobs {
		if job.Status == "RUNNING" || job.Status == "RESTARTING" {
			ids = append(ids, job.ID)
		}
	}
	if len(ids) == 0 {
		slog.Debug("no running flink jobs to cancel")
		return nil
	}

	for _, id := range ids {
		slog.Debug("cancelling flink job", "job_id", id)
		if err := do(ctx, http.MethodPatch, api+"/jobs/"+id+"?mode=cancel", nil, "", nil); err != nil {
			return err
		}
	}
	for _, id := range ids {
		if err := waitTerminal(ctx, api, id); err != nil {
			return err
		}
	}
	return nil
}

func waitTerminal(ctx context.Context, api, id string) error {
	return poll(ctx, cancelTimeout, func() (bool, error) {
		state, err := jobState(ctx, api, id)
		if err != nil {
			return false, err
		}
		switch state {
		case "CANCELED", "FAILED", "FINISHED":
			slog.Debug("flink job terminal", "job_id", id, "state", state)
			return true, nil
		}
		return false, nil
	})
}

// waitRunning waits until the job is RUNNING. Streaming jobs never reach
// FINISHED, so any terminal state here means the job did not come up.
func waitRunning(ctx context.Context, api, id string) error {
	return poll(ctx, startTimeout, func() (bool, error) {
		state, err := jobState(ctx, api, id)
		if err != nil {
			return false, err
		}
		switch state {
		case "RUNNING":
			return true, nil
		case "FAILED", "CANCELED", "RESTARTING":
			return false, fmt.Errorf("job %s entered state %s: %s", id, state, rootException(ctx, api, id))
		}
		return false, nil
	})
}

func jobState(ctx context.Context, api, id string) (string, error) {
	var job struct {
		State string `json:"state"`
	}
	if err := do(ctx, http.MethodGet, api+"/jobs/"+id, nil, "", &job); err != nil {
		return "", err
	}
	return job.State, nil
}

func rootException(ctx context.Context, api, id string) string {
	var ex struct {
		RootException string `json:"root-exception"`
		History       struct {
			Entries []struct {
				Stacktrace string `json:"stacktrace"`
			} `json:"entries"`
		} `json:"exceptionHistory"`
	}
	if err := do(ctx, http.MethodGet, api+"/jobs/"+id+"/exceptions", nil, "", &ex); err != nil {
		return "no exception detail available"
	}
	if ex.RootException != "" {
		return ex.RootException
	}
	if len(ex.History.Entries) > 0 {
		return ex.History.Entries[0].Stacktrace
	}
	return "no exception detail available"
}

func poll(ctx context.Context, timeout time.Duration, done func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	for {
		ok, err := done()
		if err != nil || ok {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s", timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// do sends a request and decodes the JSON response into out, if out is non-nil.
func do(ctx context.Context, method, url string, body io.Reader, contentType string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s %s: %s: %s", method, url, resp.Status, data)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}
