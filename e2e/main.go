// Command e2e drives the normalizer pipeline end to end: it provisions its own
// five-container stack per scenario, deploys the six Flink jobs into it,
// asserts the scenario's expectations, and tears the stack down.
//
// It is an application rather than a `go test` package so the lifecycle is
// explicit and interruptible — Ctrl-C runs teardown instead of leaking five
// containers and a Docker network, which is what a killed test binary does.
//
//	go run .                          # every scenario
//	go run . -scenario provisioning   # just one
//	go run . -keep                    # leave the stack up to poke at
//
// Unit tests are still `go test ./...` and need no Docker.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"orderbook-e2e/internal/checks"
	"orderbook-e2e/internal/domain"
	"orderbook-e2e/internal/harness"
	"orderbook-e2e/internal/report"
	"orderbook-e2e/internal/runner"
)

// defaultTimeout covers a cold run: two images get built, one of which runs a
// full Maven build pulling the whole Flink dependency closure.
const defaultTimeout = 40 * time.Minute

func main() {
	scenario := flag.String("scenario", "", "run only the scenario with this name (default: all)")
	keep := flag.Bool("keep", false, "leave each stack running after its scenario, for hand triage")
	timeout := flag.Duration("timeout", defaultTimeout, "budget for the whole run")
	flag.Parse()

	if err := run(*scenario, *keep, *timeout); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: %v\n", err)
		os.Exit(1)
	}
}

func run(scenario string, keep bool, timeout time.Duration) error {
	selected, err := runner.Filter(scenarios(), scenario)
	if err != nil {
		return err
	}

	// NotifyContext is the whole reason teardown is reliable here: on Ctrl-C
	// the context cancels, the runner stops between scenarios, and the deferred
	// teardown still runs against a fresh context.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	logf := func(format string, args ...any) {
		fmt.Fprintf(os.Stdout, format+"\n", args...)
	}

	results := runner.New(harness.NewProvisioner(), keep, logf).Run(ctx, selected)

	if !report.Write(os.Stdout, results) {
		return fmt.Errorf("%d of %d scenarios did not pass", notPassed(results), len(results))
	}
	return nil
}

// scenarios is the suite. Scenarios are Go values, so adding one is adding a
// struct literal here — there is no discovery step and nothing to register.
func scenarios() []domain.Scenario {
	provisioning := domain.Scope{ExchangeID: 8, PairID: 1}

	return []domain.Scenario{
		{
			// Asserts nothing about the pipeline's behaviour, on purpose: when
			// a real scenario fails, this says whether the environment or the
			// pipeline is to blame.
			Name:   "provisioning",
			Scope:  provisioning,
			Checks: checks.Provisioning(provisioning),
		},
	}
}

func notPassed(results []domain.Result) int {
	var n int
	for _, r := range results {
		if !r.OK() {
			n++
		}
	}
	return n
}
