// Package diagnostics collects what a failing stack can still tell you, while
// it is still alive.
//
// This exists because the failure mode it addresses is the expensive one: a job
// that never reaches RUNNING reports "not RUNNING within 2m (last state ...)"
// and the actual stack trace dies with the container. The harness's dominant
// cost is triage, not implementation.
package diagnostics

import (
	"context"
	"fmt"
	"strings"

	"orderbook-e2e/internal/flink"
	"orderbook-e2e/internal/stack"
)

// logTailLines is how much of each container log to keep. Flink containers are
// extremely chatty at startup and the interesting part is always the end.
const logTailLines = 120

// Collect gathers the job states, the exception behind anything not RUNNING,
// and the tail of the Flink containers' logs.
//
// It never returns an error: diagnostics run when something has already gone
// wrong, and a failure to collect them must not replace the real failure. Each
// section reports its own problem inline instead.
func Collect(ctx context.Context, flinkAPI string, s *stack.Stack) string {
	var b strings.Builder

	writeJobs(ctx, &b, flink.New(flinkAPI))
	writeLogs(ctx, &b, s)

	return b.String()
}

func writeJobs(ctx context.Context, b *strings.Builder, client *flink.Client) {
	jobs, err := client.Jobs(ctx)
	if err != nil {
		fmt.Fprintf(b, "jobs: could not list (%v)\n", err)
		return
	}
	if len(jobs) == 0 {
		fmt.Fprintf(b, "jobs: none submitted\n")
		return
	}

	fmt.Fprintf(b, "jobs:\n")
	for _, job := range jobs {
		fmt.Fprintf(b, "  %-24s %s\n", job.Name, job.State)
	}

	for _, job := range jobs {
		if job.Running() {
			continue
		}
		trace, err := client.Exceptions(ctx, job.ID)
		if err != nil {
			fmt.Fprintf(b, "\nexception for %s: could not fetch (%v)\n", job.Name, err)
			continue
		}
		if trace == "" {
			continue
		}
		fmt.Fprintf(b, "\nexception for %s (%s):\n%s\n", job.Name, job.State, indent(trace))
	}
}

func writeLogs(ctx context.Context, b *strings.Builder, s *stack.Stack) {
	if s == nil {
		return
	}

	// Only the Flink containers: a job failure shows up in the taskmanager, and
	// a submission failure in the jobmanager. Kafka, the registry and postgres
	// have their own wait strategies and fail loudly on the way up.
	logs := s.Logs(ctx, stack.JobManager, stack.TaskManager)

	for _, name := range []string{stack.JobManager, stack.TaskManager} {
		body, ok := logs[name]
		if !ok {
			continue
		}
		fmt.Fprintf(b, "\n%s log (last %d lines):\n%s\n", name, logTailLines, indent(tail(body, logTailLines)))
	}
}

// tail keeps the last n lines, which is where a container says why it stopped.
func tail(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) <= n {
		return strings.Join(lines, "\n")
	}
	return strings.Join(lines[len(lines)-n:], "\n")
}

func indent(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i, line := range lines {
		lines[i] = "  " + line
	}
	return strings.Join(lines, "\n")
}
