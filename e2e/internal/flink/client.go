// Package flink drives the Flink JobManager's REST API: upload a jar, run it,
// wait for it to reach RUNNING. Adapter for ports.JobSubmitter.
//
// The cluster runs in session mode, which is what makes uploading jars at
// runtime possible at all.
package flink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"path"
	"time"

	"orderbook-e2e/internal/domain"
)

const (
	pollInterval   = 500 * time.Millisecond
	runningTimeout = 2 * time.Minute
	uploadTimeout  = 2 * time.Minute
)

// Client talks to one JobManager.
type Client struct {
	baseURL string
	http    *http.Client
}

// New returns a client for a JobManager REST endpoint, e.g. http://localhost:32903.
func New(baseURL string) *Client {
	return &Client{baseURL: baseURL, http: &http.Client{Timeout: uploadTimeout}}
}

// Submit uploads a jar, starts it, and returns once the job is RUNNING.
// Streaming jobs never reach FINISHED, so RUNNING is the success state.
func (c *Client) Submit(ctx context.Context, jar domain.Jar) error {
	jarID, err := c.uploadJar(ctx, jar)
	if err != nil {
		return fmt.Errorf("upload: %w", err)
	}

	jobID, err := c.runJar(ctx, jarID)
	if err != nil {
		return fmt.Errorf("run: %w", err)
	}

	if err := c.waitRunning(ctx, jobID); err != nil {
		return fmt.Errorf("job %s: %w", jobID, err)
	}
	return nil
}

// WaitForTaskManagers blocks until at least n task managers have registered.
// The JobManager answers REST requests long before it has any slots, so
// without this the first submission fails to schedule.
func (c *Client) WaitForTaskManagers(ctx context.Context, n int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		var overview struct {
			TaskManagers []json.RawMessage `json:"taskmanagers"`
		}
		err := c.getJSON(ctx, "/taskmanagers", &overview)
		if err == nil && len(overview.TaskManagers) >= n {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("only %d of %d task managers registered within %s (last error: %v)",
				len(overview.TaskManagers), n, timeout, err)
		}
		if err := sleep(ctx, pollInterval); err != nil {
			return err
		}
	}
}

// Job is one job as the cluster sees it.
type Job struct {
	ID    string `json:"jid"`
	Name  string `json:"name"`
	State string `json:"state"`
}

// Running reports whether the job is in the only state a streaming job should
// settle in.
func (j Job) Running() bool { return j.State == "RUNNING" }

// Jobs lists every job the cluster knows about, in whatever state. Unlike
// RunningJobs this keeps the failed ones, which are the interesting ones when
// something has gone wrong.
func (c *Client) Jobs(ctx context.Context) ([]Job, error) {
	var overview struct {
		Jobs []Job `json:"jobs"`
	}
	if err := c.getJSON(ctx, "/jobs/overview", &overview); err != nil {
		return nil, err
	}
	return overview.Jobs, nil
}

// RunningJobs names the jobs currently in state RUNNING.
func (c *Client) RunningJobs(ctx context.Context) ([]string, error) {
	jobs, err := c.Jobs(ctx)
	if err != nil {
		return nil, err
	}

	var running []string
	for _, job := range jobs {
		if job.Running() {
			running = append(running, job.Name)
		}
	}
	return running, nil
}

// Exceptions returns the job's root-cause stack trace, which is where a job
// that failed at runtime says why. An empty string means the cluster had
// nothing to report.
//
// Two shapes are read because Flink serves both: the long-deprecated flat
// root-exception, and exceptionHistory.entries which replaced it.
func (c *Client) Exceptions(ctx context.Context, jobID string) (string, error) {
	var exceptions struct {
		RootException    string `json:"root-exception"`
		ExceptionHistory struct {
			Entries []struct {
				Stacktrace string `json:"stacktrace"`
				Name       string `json:"exceptionName"`
				Task       string `json:"taskName"`
			} `json:"entries"`
		} `json:"exceptionHistory"`
	}
	if err := c.getJSON(ctx, "/jobs/"+jobID+"/exceptions", &exceptions); err != nil {
		return "", err
	}

	for _, e := range exceptions.ExceptionHistory.Entries {
		if e.Stacktrace != "" {
			if e.Task != "" {
				return "task " + e.Task + ": " + e.Stacktrace, nil
			}
			return e.Stacktrace, nil
		}
		if e.Name != "" {
			return e.Name, nil
		}
	}
	return exceptions.RootException, nil
}

// uploadJar posts the jar bytes and returns the id Flink filed them under.
func (c *Client) uploadJar(ctx context.Context, jar domain.Jar) (string, error) {
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	// Flink rejects an upload whose filename does not end in .jar.
	part, err := form.CreateFormFile("jarfile", jar.Name+".jar")
	if err != nil {
		return "", err
	}
	if _, err := part.Write(jar.Bytes); err != nil {
		return "", err
	}
	if err := form.Close(); err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/jars/upload", &body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", form.FormDataContentType())

	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("flink returned %d: %s", resp.StatusCode, bytes.TrimSpace(raw))
	}

	var uploaded struct {
		Filename string `json:"filename"`
		Status   string `json:"status"`
	}
	if err := json.Unmarshal(raw, &uploaded); err != nil {
		return "", fmt.Errorf("decode upload response: %w", err)
	}
	if uploaded.Status != "success" || uploaded.Filename == "" {
		return "", fmt.Errorf("upload not accepted: %s", bytes.TrimSpace(raw))
	}
	// Flink answers with the server-side path; the id is its last segment.
	return path.Base(uploaded.Filename), nil
}

// runJar starts an uploaded jar. Each job's Main-Class comes from its shade
// manifest, so no entry class has to be passed.
func (c *Client) runJar(ctx context.Context, jarID string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/jars/"+jarID+"/run", nil)
	if err != nil {
		return "", err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("flink returned %d: %s", resp.StatusCode, bytes.TrimSpace(raw))
	}

	var started struct {
		JobID string `json:"jobid"`
	}
	if err := json.Unmarshal(raw, &started); err != nil {
		return "", fmt.Errorf("decode run response: %w", err)
	}
	if started.JobID == "" {
		return "", fmt.Errorf("no job id in response: %s", bytes.TrimSpace(raw))
	}
	return started.JobID, nil
}

// waitRunning polls until the job is RUNNING, or fails fast on a terminal state.
func (c *Client) waitRunning(ctx context.Context, jobID string) error {
	deadline := time.Now().Add(runningTimeout)
	for {
		var job struct {
			State string `json:"state"`
		}
		err := c.getJSON(ctx, "/jobs/"+jobID, &job)
		if err == nil {
			switch job.State {
			case "RUNNING":
				return nil
			case "FAILED", "CANCELED", "FINISHED":
				return fmt.Errorf("reached terminal state %s", job.State)
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("not RUNNING within %s (last state %q, last error: %v)", runningTimeout, job.State, err)
		}
		if err := sleep(ctx, pollInterval); err != nil {
			return err
		}
	}
}

func (c *Client) getJSON(ctx context.Context, endpoint string, into any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+endpoint, nil)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s returned %d: %s", endpoint, resp.StatusCode, bytes.TrimSpace(raw))
	}
	return json.Unmarshal(raw, into)
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
