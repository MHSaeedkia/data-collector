// Package report renders scenario results for a human reading a terminal.
//
// Deliberately not JUnit/XML: nothing consumes a machine-readable report yet,
// and the exit code is what CI actually needs. Add a format when something
// asks for one.
package report

import (
	"fmt"
	"io"
	"strings"
	"time"

	"orderbook-e2e/internal/domain"
)

// Write prints one line per scenario, the detail of anything that went wrong,
// and a tally. It reports whether every scenario passed.
func Write(w io.Writer, results []domain.Result) bool {
	passed := true

	fmt.Fprintln(w)
	for _, r := range results {
		switch {
		case r.Err != nil:
			passed = false
			fmt.Fprintf(w, "ERROR %s (%s)\n", r.Scenario, round(r.Duration))
			fmt.Fprintf(w, "      the stack did not come up: %v\n", r.Err)
		case len(r.Failures) > 0:
			passed = false
			fmt.Fprintf(w, "FAIL  %s (%s) — %s\n", r.Scenario, round(r.Duration), plural(len(r.Failures), "failure"))
			for _, f := range r.Failures {
				fmt.Fprintf(w, "      ✗ %s\n", f.Check)
				fmt.Fprintf(w, "        %s\n", f.Detail)
			}
		default:
			fmt.Fprintf(w, "PASS  %s (%s)\n", r.Scenario, round(r.Duration))
		}

		writeDiagnostics(w, r.Diagnostics)
	}

	fmt.Fprintf(w, "\n%s\n", tally(results))
	return passed
}

// writeDiagnostics indents the collected stack diagnostics under their
// scenario so a failure and its evidence read as one block.
func writeDiagnostics(w io.Writer, diagnostics string) {
	diagnostics = strings.TrimSpace(diagnostics)
	if diagnostics == "" {
		return
	}

	fmt.Fprintf(w, "\n      ── diagnostics ──\n")
	for _, line := range strings.Split(diagnostics, "\n") {
		if line == "" {
			fmt.Fprintln(w)
			continue
		}
		fmt.Fprintf(w, "      %s\n", line)
	}
	fmt.Fprintln(w)
}

func tally(results []domain.Result) string {
	var passed, failed, errored int
	for _, r := range results {
		switch {
		case r.Err != nil:
			errored++
		case len(r.Failures) > 0:
			failed++
		default:
			passed++
		}
	}

	summary := fmt.Sprintf("%d passed, %d failed", passed, failed)
	if errored > 0 {
		summary += fmt.Sprintf(", %d errored", errored)
	}
	return summary
}

func plural(n int, word string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, word)
	}
	return fmt.Sprintf("%d %ss", n, word)
}

// round keeps durations readable; nobody needs nanoseconds on a run measured
// in minutes.
func round(d time.Duration) time.Duration { return d.Round(time.Second) }
