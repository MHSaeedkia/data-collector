package provision_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"orderbook-e2e/internal/domain"
	"orderbook-e2e/internal/provision"
)

// recorder is shared by every fake so the steps land on one ordered log.
type recorder struct{ steps []string }

type fakeSubjects struct {
	rec  *recorder
	out  []domain.Subject
	fail error
}

func (f *fakeSubjects) Subjects() ([]domain.Subject, error) {
	f.rec.steps = append(f.rec.steps, "subjects")
	return f.out, f.fail
}

type fakeRegistrar struct {
	rec        *recorder
	registered []string
	fail       error
}

func (f *fakeRegistrar) Register(_ context.Context, s domain.Subject) error {
	f.rec.steps = append(f.rec.steps, "register:"+s.Name)
	f.registered = append(f.registered, s.Name)
	return f.fail
}

type fakeTopics struct {
	rec     *recorder
	created []domain.Topic
	fail    error
}

func (f *fakeTopics) Create(_ context.Context, topics []domain.Topic) error {
	f.rec.steps = append(f.rec.steps, "topics")
	f.created = topics
	return f.fail
}

type fakeJars struct {
	rec  *recorder
	out  []domain.Jar
	fail error
}

func (f *fakeJars) Jars(context.Context) ([]domain.Jar, error) {
	f.rec.steps = append(f.rec.steps, "jars")
	return f.out, f.fail
}

type fakeJobs struct {
	rec       *recorder
	submitted []string
	fail      error
}

func (f *fakeJobs) Submit(_ context.Context, jar domain.Jar) error {
	f.rec.steps = append(f.rec.steps, "submit:"+jar.Name)
	f.submitted = append(f.submitted, jar.Name)
	return f.fail
}

type harness struct {
	deps     provision.Deps
	rec      *recorder
	subjects *fakeSubjects
	schemas  *fakeRegistrar
	topics   *fakeTopics
	jars     *fakeJars
	jobs     *fakeJobs
}

func newHarness() *harness {
	rec := &recorder{}
	h := &harness{
		rec: rec,
		subjects: &fakeSubjects{rec: rec, out: []domain.Subject{
			{Name: "raw-order-book-event", Schema: "{}"},
			{Name: "order-book-snapshot", Schema: "{}"},
		}},
		schemas: &fakeRegistrar{rec: rec},
		topics:  &fakeTopics{rec: rec},
		jars: &fakeJars{rec: rec, out: []domain.Jar{
			{Name: "job-aggregator", Bytes: []byte("a")},
			{Name: "job-pair-extractor", Bytes: []byte("b")},
		}},
		jobs: &fakeJobs{rec: rec},
	}
	h.deps = provision.Deps{
		Subjects: h.subjects,
		Schemas:  h.schemas,
		Topics:   h.topics,
		Jars:     h.jars,
		Jobs:     h.jobs,
	}
	return h
}

func TestRunOrdersTopicsBeforeJobs(t *testing.T) {
	h := newHarness()

	require.NoError(t, provision.Run(context.Background(), h.deps, domain.Scope{ExchangeID: 8, PairID: 1}))

	assert.Equal(t, []string{
		"subjects",
		"register:raw-order-book-event",
		"register:order-book-snapshot",
		"topics",
		"jars",
		"submit:job-aggregator",
		"submit:job-pair-extractor",
	}, h.rec.steps)
}

func TestRunCreatesTheScopesTopics(t *testing.T) {
	h := newHarness()

	require.NoError(t, provision.Run(context.Background(), h.deps, domain.Scope{ExchangeID: 8, PairID: 1}))

	assert.Equal(t, domain.TopicsFor(domain.Scope{ExchangeID: 8, PairID: 1}), h.topics.created)
}

func TestRunSubmitsEveryJarInOrder(t *testing.T) {
	h := newHarness()

	require.NoError(t, provision.Run(context.Background(), h.deps, domain.Scope{ExchangeID: 8, PairID: 1}))

	assert.Equal(t, []string{"job-aggregator", "job-pair-extractor"}, h.jobs.submitted)
}

func TestRunStopsAtTheFirstFailure(t *testing.T) {
	boom := errors.New("boom")

	tests := []struct {
		name      string
		breakDeps func(*harness)
		wantMsg   string
		wantSteps []string
	}{
		{
			name:      "reading schemas",
			breakDeps: func(h *harness) { h.subjects.fail = boom },
			wantMsg:   "read schemas: boom",
			wantSteps: []string{"subjects"},
		},
		{
			name:      "registering a subject",
			breakDeps: func(h *harness) { h.schemas.fail = boom },
			wantMsg:   "register subject raw-order-book-event: boom",
			wantSteps: []string{"subjects", "register:raw-order-book-event"},
		},
		{
			name:      "creating topics",
			breakDeps: func(h *harness) { h.topics.fail = boom },
			wantMsg:   "create topics: boom",
			wantSteps: []string{"subjects", "register:raw-order-book-event", "register:order-book-snapshot", "topics"},
		},
		{
			name:      "building jars",
			breakDeps: func(h *harness) { h.jars.fail = boom },
			wantMsg:   "build job jars: boom",
			wantSteps: []string{"subjects", "register:raw-order-book-event", "register:order-book-snapshot", "topics", "jars"},
		},
		{
			name:      "submitting a job",
			breakDeps: func(h *harness) { h.jobs.fail = boom },
			wantMsg:   "submit job-aggregator: boom",
			wantSteps: []string{"subjects", "register:raw-order-book-event", "register:order-book-snapshot", "topics", "jars", "submit:job-aggregator"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness()
			tc.breakDeps(h)

			err := provision.Run(context.Background(), h.deps, domain.Scope{ExchangeID: 8, PairID: 1})

			require.Error(t, err)
			assert.EqualError(t, err, tc.wantMsg)
			assert.ErrorIs(t, err, boom)
			assert.Equal(t, tc.wantSteps, h.rec.steps)
		})
	}
}
