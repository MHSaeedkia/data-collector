package domain

import (
	"context"
	"time"
)

// Failure is one unmet expectation. The harness collects these rather than
// stopping at the first one: when a scenario goes red, every failed check is
// evidence about what the pipeline did, and the second one is often the
// informative one.
type Failure struct {
	// Check names the expectation, e.g. "all six jobs are running".
	Check string
	// Detail is what was actually observed.
	Detail string
}

// Check is one expectation asserted against a provisioned stack. It returns
// every failure it found, or nothing if the expectation held.
//
// A func rather than an interface: checks are stateless and single-purpose, and
// this keeps the runner free of a port per kind of thing being checked.
type Check func(ctx context.Context, e Endpoints) []Failure

// Scenario is one exercise of the pipeline: a scope to provision for, and the
// expectations to assert once it is up.
//
// Scenarios are values, not test functions, so the runner can filter, order and
// report on them without a test framework.
type Scenario struct {
	Name   string
	Scope  Scope
	Checks []Check
}

// Result is what a scenario produced.
//
// Err and Failures mean different things and must not be collapsed: Err is the
// environment failing to come up (nothing was asserted), Failures are the
// pipeline behaving differently from what was expected (everything was
// asserted, some of it was wrong).
type Result struct {
	Scenario    string
	Failures    []Failure
	Err         error
	Duration    time.Duration
	Diagnostics string
}

// OK reports whether the scenario provisioned and met every expectation.
func (r Result) OK() bool { return r.Err == nil && len(r.Failures) == 0 }
