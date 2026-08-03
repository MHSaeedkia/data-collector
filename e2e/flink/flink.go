// Package flink builds the normalizer job jars and runs them on the Flink cluster.
package flink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
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
func build(ctx context.Context, normalizerDir string) error {
	log.Print("building normalizer jobs...")

	cmd := exec.CommandContext(ctx, "mvn", "-f", filepath.Join(normalizerDir, "pom.xml"), "clean", "package", "-q", "-DskipTests")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mvn clean package: %w", err)
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
	log.Printf("%s running (job %s)", module, run.JobID)
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

// CancelJobs cancels every running job and waits for each to reach a terminal
// state, so their task slots are free before new jobs are submitted — and so
// nothing is still consuming the topics when they are deleted.
func CancelJobs(ctx context.Context, api string) error {
	var list struct {
		Jobs []struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"jobs"`
	}
	if err := do(ctx, http.MethodGet, api+"/jobs", nil, "", &list); err != nil {
		return err
	}

	var ids []string
	for _, job := range list.Jobs {
		if job.Status == "RUNNING" || job.Status == "RESTARTING" {
			ids = append(ids, job.ID)
		}
	}
	if len(ids) == 0 {
		log.Print("no running flink jobs to cancel")
		return nil
	}

	for _, id := range ids {
		log.Printf("cancelling job %s", id)
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
			log.Printf("    %s: %s", id, state)
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
