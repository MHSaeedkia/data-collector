// Package runner is the harness's driving use case: provision a stack per
// scenario, assert the scenario's expectations against it, tear it down, move
// on. It talks only to ports, so the whole lifecycle is unit-testable with
// fakes and nothing running.
package runner

import (
	"context"
	"fmt"
	"time"

	"orderbook-e2e/internal/domain"
	"orderbook-e2e/internal/ports"
)

// Clock is time, injected so the runner's durations are testable. Nothing else
// in the runner touches the outside world.
type Clock func() time.Time

// Runner executes scenarios.
type Runner struct {
	provisioner ports.Provisioner
	now         Clock
	// keep skips teardown, leaving the stack up for hand triage.
	keep bool
	// log reports progress while the run is in flight; a run takes minutes and
	// silence is indistinguishable from a hang.
	log func(format string, args ...any)
}

// New returns a Runner. Pass keep=true to leave each stack running after its
// scenario.
func New(p ports.Provisioner, keep bool, log func(string, ...any)) *Runner {
	return &Runner{provisioner: p, now: time.Now, keep: keep, log: log}
}

// Run executes every scenario in order and returns one Result each.
//
// Scenarios run SEQUENTIALLY and never in parallel: each one owns five
// containers including a Flink cluster, and two at once does not fit on a
// typical Docker Desktop allocation.
//
// A scenario that fails does not stop the run — the whole point of a suite is
// the full picture. A cancelled context does stop it: the remaining scenarios
// would only fail on the way up.
func (r *Runner) Run(ctx context.Context, scenarios []domain.Scenario) []domain.Result {
	results := make([]domain.Result, 0, len(scenarios))

	for _, scenario := range scenarios {
		if err := ctx.Err(); err != nil {
			results = append(results, domain.Result{Scenario: scenario.Name, Err: err})
			continue
		}
		results = append(results, r.runOne(ctx, scenario))
	}

	return results
}

func (r *Runner) runOne(ctx context.Context, scenario domain.Scenario) domain.Result {
	started := r.now()
	result := domain.Result{Scenario: scenario.Name}

	r.log("=== %s: provisioning (scope ex%d/p%d)", scenario.Name, scenario.Scope.ExchangeID, scenario.Scope.PairID)

	stack, err := r.provisioner.Start(ctx, scenario.Scope)
	if stack != nil {
		defer r.teardown(ctx, scenario.Name, stack)
	}
	if err != nil {
		result.Err = err
		// Diagnostics are only reachable while the containers are alive, and
		// the deferred teardown above is about to remove them.
		result.Diagnostics = r.diagnose(ctx, stack)
		result.Duration = r.now().Sub(started)
		return result
	}

	r.log("=== %s: running %d checks", scenario.Name, len(scenario.Checks))

	endpoints := stack.Endpoints()
	for _, check := range scenario.Checks {
		result.Failures = append(result.Failures, check(ctx, endpoints)...)
	}

	if len(result.Failures) > 0 {
		result.Diagnostics = r.diagnose(ctx, stack)
	}

	result.Duration = r.now().Sub(started)
	return result
}

// diagnose collects whatever the stack can tell us about why this went wrong.
// It must never be the reason a run fails, so an absent stack is simply no
// diagnostics.
func (r *Runner) diagnose(ctx context.Context, stack ports.Stack) string {
	if stack == nil {
		return ""
	}
	// The run's context may already be cancelled or past its deadline — which
	// is exactly when diagnostics matter most — so collect on a fresh one.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), diagnosticsTimeout)
	defer cancel()

	return stack.Diagnostics(ctx)
}

const diagnosticsTimeout = 30 * time.Second

func (r *Runner) teardown(ctx context.Context, scenario string, stack ports.Stack) {
	if r.keep {
		r.log("=== %s: -keep set, leaving the stack up", scenario)
		return
	}

	// Teardown has to run even when the run was interrupted, which is the case
	// where it matters most: without this, Ctrl-C leaks five containers.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), teardownTimeout)
	defer cancel()

	if err := stack.Close(ctx); err != nil {
		r.log("=== %s: teardown: %v", scenario, err)
	}
}

const teardownTimeout = 2 * time.Minute

// Filter selects scenarios by exact name. An empty name selects everything.
// Returns an error rather than an empty run when the name matches nothing — a
// typo'd filter that silently passes is worse than no filter at all.
func Filter(scenarios []domain.Scenario, name string) ([]domain.Scenario, error) {
	if name == "" {
		return scenarios, nil
	}

	for _, scenario := range scenarios {
		if scenario.Name == name {
			return []domain.Scenario{scenario}, nil
		}
	}

	known := make([]string, 0, len(scenarios))
	for _, scenario := range scenarios {
		known = append(known, scenario.Name)
	}
	return nil, fmt.Errorf("no scenario named %q (have %v)", name, known)
}
