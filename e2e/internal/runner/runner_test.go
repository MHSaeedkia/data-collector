package runner_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"orderbook-e2e/internal/domain"
	"orderbook-e2e/internal/ports"
	"orderbook-e2e/internal/runner"
)

// fakeStack records the lifecycle calls made against one provisioned stack.
type fakeStack struct {
	closed      int
	diagnosed   int
	diagnostics string
}

func (f *fakeStack) Endpoints() domain.Endpoints { return domain.Endpoints{FlinkAPI: "http://fake"} }

func (f *fakeStack) Diagnostics(context.Context) string {
	f.diagnosed++
	return f.diagnostics
}

func (f *fakeStack) Close(context.Context) error {
	f.closed++
	return nil
}

type fakeProvisioner struct {
	stack  *fakeStack
	err    error
	scopes []domain.Scope
}

func (f *fakeProvisioner) Start(_ context.Context, scope domain.Scope) (ports.Stack, error) {
	f.scopes = append(f.scopes, scope)
	if f.stack == nil {
		return nil, f.err
	}
	return f.stack, f.err
}

func passing(name string) domain.Check {
	return func(context.Context, domain.Endpoints) []domain.Failure { return nil }
}

func failing(name, detail string) domain.Check {
	return func(context.Context, domain.Endpoints) []domain.Failure {
		return []domain.Failure{{Check: name, Detail: detail}}
	}
}

func newRunner(p ports.Provisioner, keep bool) *runner.Runner {
	return runner.New(p, keep, func(string, ...any) {})
}

func TestRunPassesWhenEveryCheckPasses(t *testing.T) {
	stack := &fakeStack{}
	r := newRunner(&fakeProvisioner{stack: stack}, false)

	results := r.Run(context.Background(), []domain.Scenario{{
		Name:   "provisioning",
		Scope:  domain.Scope{ExchangeID: 8, PairID: 1},
		Checks: []domain.Check{passing("a"), passing("b")},
	}})

	require.Len(t, results, 1)
	assert.True(t, results[0].OK())
	assert.Equal(t, 1, stack.closed, "the stack must be torn down")
	assert.Equal(t, 0, stack.diagnosed, "a passing scenario must not pay for diagnostics")
}

func TestRunCollectsEveryFailureNotJustTheFirst(t *testing.T) {
	stack := &fakeStack{}
	r := newRunner(&fakeProvisioner{stack: stack}, false)

	results := r.Run(context.Background(), []domain.Scenario{{
		Name: "provisioning",
		Checks: []domain.Check{
			failing("jobs", "want 6 got 5"),
			passing("topics"),
			failing("subjects", "missing [order-book-snapshot]"),
		},
	}})

	require.Len(t, results, 1)
	assert.False(t, results[0].OK())
	assert.Equal(t, []domain.Failure{
		{Check: "jobs", Detail: "want 6 got 5"},
		{Check: "subjects", Detail: "missing [order-book-snapshot]"},
	}, results[0].Failures)
}

func TestRunCollectsDiagnosticsOnFailureBeforeTeardown(t *testing.T) {
	stack := &fakeStack{diagnostics: "taskmanager: NoClassDefFoundError"}
	r := newRunner(&fakeProvisioner{stack: stack}, false)

	results := r.Run(context.Background(), []domain.Scenario{{
		Name:   "provisioning",
		Checks: []domain.Check{failing("jobs", "want 6 got 0")},
	}})

	assert.Equal(t, "taskmanager: NoClassDefFoundError", results[0].Diagnostics)
	assert.Equal(t, 1, stack.diagnosed)
	assert.Equal(t, 1, stack.closed)
}

// The reason harness.Start returns a stack alongside its error: a stack that
// half came up still holds the only explanation of why.
func TestRunDiagnosesAndTearsDownAStackThatFailedToProvision(t *testing.T) {
	stack := &fakeStack{diagnostics: "jobs: none submitted"}
	boom := errors.New("provision: submit job-aggregator: not RUNNING within 2m")
	r := newRunner(&fakeProvisioner{stack: stack, err: boom}, false)

	results := r.Run(context.Background(), []domain.Scenario{{Name: "provisioning"}})

	require.Len(t, results, 1)
	assert.ErrorIs(t, results[0].Err, boom)
	assert.Equal(t, "jobs: none submitted", results[0].Diagnostics)
	assert.Equal(t, 1, stack.closed, "a failed provision must still be torn down")
}

func TestRunSurvivesAProvisionerThatReturnedNoStack(t *testing.T) {
	boom := errors.New("build job jars: exit 100")
	r := newRunner(&fakeProvisioner{err: boom}, false)

	results := r.Run(context.Background(), []domain.Scenario{{Name: "provisioning"}})

	require.Len(t, results, 1)
	assert.ErrorIs(t, results[0].Err, boom)
	assert.Empty(t, results[0].Diagnostics)
}

func TestRunContinuesAfterAFailingScenario(t *testing.T) {
	p := &fakeProvisioner{stack: &fakeStack{}}
	r := newRunner(p, false)

	results := r.Run(context.Background(), []domain.Scenario{
		{Name: "first", Scope: domain.Scope{ExchangeID: 8, PairID: 1}, Checks: []domain.Check{failing("jobs", "nope")}},
		{Name: "second", Scope: domain.Scope{ExchangeID: 1, PairID: 1}, Checks: []domain.Check{passing("jobs")}},
	})

	require.Len(t, results, 2)
	assert.False(t, results[0].OK())
	assert.True(t, results[1].OK())
	assert.Equal(t, []domain.Scope{{ExchangeID: 8, PairID: 1}, {ExchangeID: 1, PairID: 1}}, p.scopes)
}

func TestRunStopsProvisioningOnceTheContextIsCancelled(t *testing.T) {
	p := &fakeProvisioner{stack: &fakeStack{}}
	r := newRunner(p, false)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	results := r.Run(ctx, []domain.Scenario{{Name: "first"}, {Name: "second"}})

	require.Len(t, results, 2)
	assert.ErrorIs(t, results[0].Err, context.Canceled)
	assert.ErrorIs(t, results[1].Err, context.Canceled)
	assert.Empty(t, p.scopes, "nothing may be provisioned after cancellation")
}

func TestKeepSkipsTeardown(t *testing.T) {
	stack := &fakeStack{}
	r := newRunner(&fakeProvisioner{stack: stack}, true)

	r.Run(context.Background(), []domain.Scenario{{Name: "provisioning"}})

	assert.Equal(t, 0, stack.closed)
}

func TestFilter(t *testing.T) {
	all := []domain.Scenario{{Name: "provisioning"}, {Name: "01-ex8-snapshot"}}

	t.Run("empty selects everything", func(t *testing.T) {
		got, err := runner.Filter(all, "")
		require.NoError(t, err)
		assert.Equal(t, all, got)
	})

	t.Run("a name selects one", func(t *testing.T) {
		got, err := runner.Filter(all, "01-ex8-snapshot")
		require.NoError(t, err)
		assert.Equal(t, []domain.Scenario{{Name: "01-ex8-snapshot"}}, got)
	})

	// A typo'd filter that silently runs nothing and exits 0 is the worst
	// possible outcome for a suite guarding a pipeline.
	t.Run("an unknown name is an error, not an empty run", func(t *testing.T) {
		_, err := runner.Filter(all, "typo")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no scenario named \"typo\"")
	})
}
