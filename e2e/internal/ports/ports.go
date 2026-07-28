// Package ports declares the narrow interfaces the harness owns. The
// provisioning use case depends on these and on nothing else; the concrete
// adapters (testcontainers, franz-go, the Flink and Schema Registry REST APIs)
// depend inward on them.
package ports

import (
	"context"

	"orderbook-e2e/internal/domain"
)

// SubjectSource yields the Avro schemas to register, read from wherever they
// happen to live.
type SubjectSource interface {
	Subjects() ([]domain.Subject, error)
}

// SchemaRegistrar registers one Avro schema under its subject.
type SchemaRegistrar interface {
	Register(ctx context.Context, s domain.Subject) error
}

// TopicCreator creates topics, idempotently — an already-existing topic is
// not an error.
type TopicCreator interface {
	Create(ctx context.Context, topics []domain.Topic) error
}

// JarSource produces the built job jars. Implementations are expected to be
// expensive on first call and cheap afterwards.
type JarSource interface {
	Jars(ctx context.Context) ([]domain.Jar, error)
}

// JobSubmitter uploads a jar to the cluster, starts it, and returns only once
// the job is RUNNING.
type JobSubmitter interface {
	Submit(ctx context.Context, jar domain.Jar) error
}

// Stack is a provisioned environment, owned by whoever started it.
type Stack interface {
	// Endpoints are the addresses the scenario's checks talk to.
	Endpoints() domain.Endpoints
	// Diagnostics is everything worth reading when a scenario goes wrong —
	// job states, Flink exceptions, container logs. It must be collected
	// BEFORE Close, because Close destroys the containers holding it.
	Diagnostics(ctx context.Context) string
	// Close tears the environment down. Safe to call more than once.
	Close(ctx context.Context) error
}

// Provisioner brings a stack up for one scope.
//
// It returns a non-nil Stack whenever containers actually started, EVEN ON
// ERROR, so the caller can collect diagnostics from a half-provisioned
// environment. The caller always owns teardown: close a non-nil Stack whether
// or not an error came back with it.
type Provisioner interface {
	Start(ctx context.Context, scope domain.Scope) (Stack, error)
}
