// Package harness is the composition root: it wires the adapters to the
// provisioning use case and hands a scenario a stack it can produce into.
//
// One stack per scenario. That is what makes scenarios independent — the
// pipeline keeps operator state and never checkpoints, so a shared cluster
// would carry one scenario's order book into the next.
package harness

import (
	"context"
	"fmt"
	"sync"
	"time"

	"orderbook-e2e/internal/diagnostics"
	"orderbook-e2e/internal/domain"
	"orderbook-e2e/internal/flink"
	"orderbook-e2e/internal/jobs"
	"orderbook-e2e/internal/kafka"
	"orderbook-e2e/internal/ports"
	"orderbook-e2e/internal/provision"
	"orderbook-e2e/internal/registry"
	"orderbook-e2e/internal/repo"
	"orderbook-e2e/internal/schemas"
	"orderbook-e2e/internal/stack"
)

// settle is the pause between "every job reports RUNNING" and "the scenario may
// produce".
//
// A job is RUNNING before its Kafka sources have been assigned their
// partitions, and every source starts at latest() — anything produced in that
// window is silently dropped. Nothing in the REST API exposes source readiness,
// so this is a wait, inherited from the shell scripts' SETTLE=8.
const settle = 8 * time.Second

// jarBuilder is process-wide on purpose: the stack is per scenario, but
// building the jars is a cold Maven build and must happen once per run.
var (
	jarBuilderMu sync.Mutex
	jarBuilder   *jobs.Builder
)

// Provisioner starts stacks. It carries no state beyond the shared jar
// builder, and exists so the runner can depend on ports.Provisioner rather
// than on this package.
type Provisioner struct{}

// NewProvisioner returns the real provisioner.
func NewProvisioner() *Provisioner { return &Provisioner{} }

// Start satisfies ports.Provisioner.
func (*Provisioner) Start(ctx context.Context, scope domain.Scope) (ports.Stack, error) {
	env, err := Start(ctx, scope)
	if env == nil {
		// A typed nil in an interface is not nil, and the caller's "did
		// anything come up?" check depends on this being a true nil.
		return nil, err
	}
	return env, err
}

// Env is a provisioned stack, ready for a scenario to produce into.
type Env struct {
	endpoints domain.Endpoints
	stack     *stack.Stack
}

// Start boots the stack, warms it up for this scope, builds the job jars and
// deploys the pipeline. It returns only once the pipeline can be produced into.
//
// On failure it returns a non-nil Env whenever containers actually started, so
// the caller can collect diagnostics from a half-provisioned stack before
// closing it — the exception behind a job that never reached RUNNING lives in
// the taskmanager, and terminating here would throw it away. THE CALLER ALWAYS
// OWNS TEARDOWN: close a non-nil Env whether or not an error came with it.
func Start(ctx context.Context, scope domain.Scope) (*Env, error) {
	root, err := repo.Root()
	if err != nil {
		return nil, err
	}

	// Build the jars before booting anything. Provisioning would build them
	// anyway, but a cold Maven build takes minutes: doing it first keeps five
	// containers from idling through it, and a build failure costs no stack.
	builder := sharedJarBuilder(root)
	if _, err := builder.Jars(ctx); err != nil {
		return nil, err
	}

	running, err := stack.Start(ctx, root)
	if err != nil {
		// stack.Start terminates its own partial stack, so there is nothing
		// here for the caller to close or read.
		return nil, fmt.Errorf("start stack: %w", err)
	}
	env := &Env{endpoints: running.Endpoints, stack: running}

	deps := provision.Deps{
		Subjects: schemas.New(root),
		Schemas:  registry.New(running.Endpoints.SchemaRegistryURL),
		Topics:   kafka.NewAdmin(running.Endpoints.KafkaBroker),
		Jars:     builder,
		Jobs:     flink.New(running.Endpoints.FlinkAPI),
	}

	if err := provision.Run(ctx, deps, scope); err != nil {
		return env, fmt.Errorf("provision: %w", err)
	}

	select {
	case <-ctx.Done():
		return env, ctx.Err()
	case <-time.After(settle):
	}

	return env, nil
}

// Endpoints are the addresses of the running stack.
func (e *Env) Endpoints() domain.Endpoints { return e.endpoints }

// Diagnostics collects what the stack can say about a failure. Must be called
// before Close — it reads from the containers Close destroys.
func (e *Env) Diagnostics(ctx context.Context) string {
	if e.stack == nil {
		return ""
	}
	return diagnostics.Collect(ctx, e.endpoints.FlinkAPI, e.stack)
}

// Close tears the stack down. Safe to call more than once.
func (e *Env) Close(ctx context.Context) error {
	if e.stack == nil {
		return nil
	}
	s := e.stack
	e.stack = nil
	return s.Terminate(ctx)
}

func sharedJarBuilder(repoRoot string) *jobs.Builder {
	jarBuilderMu.Lock()
	defer jarBuilderMu.Unlock()

	if jarBuilder == nil {
		jarBuilder = jobs.NewBuilder(repoRoot)
	}
	return jarBuilder
}
