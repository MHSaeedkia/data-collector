// Package provision is the harness's use case: given a started stack, bring it
// to the point where a scenario can produce raw events into it. It talks only
// to ports, so the whole ordering contract is unit-testable with fakes and
// nothing running.
package provision

import (
	"context"
	"fmt"

	"orderbook-e2e/internal/domain"
	"orderbook-e2e/internal/ports"
)

// Deps are the adapters the use case drives. They are constructed by the
// composition root after the stack is up, because their addresses are not
// known before then.
type Deps struct {
	Subjects ports.SubjectSource
	Schemas  ports.SchemaRegistrar
	Topics   ports.TopicCreator
	Jars     ports.JarSource
	Jobs     ports.JobSubmitter
}

// Run warms the stack up and deploys the pipeline into it.
//
// The order is a hard requirement, not a preference:
//
//  1. Schemas — the jobs' Avro serializers resolve subjects on first use.
//  2. Topics — every source subscribes by pattern and starts at latest(), so a
//     topic created after its job started is missed (or discovered a
//     partition-discovery interval later, having lost everything produced in
//     between).
//  3. Jars — built before submission, obviously, but also before the cluster is
//     touched at all: it is a slow step and a failure here should not leave
//     half a pipeline running.
//  4. Jobs — submitted downstream-first, each one RUNNING before the next.
func Run(ctx context.Context, d Deps, scope domain.Scope) error {
	subjects, err := d.Subjects.Subjects()
	if err != nil {
		return fmt.Errorf("read schemas: %w", err)
	}
	for _, s := range subjects {
		if err := d.Schemas.Register(ctx, s); err != nil {
			return fmt.Errorf("register subject %s: %w", s.Name, err)
		}
	}

	if err := d.Topics.Create(ctx, domain.TopicsFor(scope)); err != nil {
		return fmt.Errorf("create topics: %w", err)
	}

	jars, err := d.Jars.Jars(ctx)
	if err != nil {
		return fmt.Errorf("build job jars: %w", err)
	}

	for _, jar := range jars {
		if err := d.Jobs.Submit(ctx, jar); err != nil {
			return fmt.Errorf("submit %s: %w", jar.Name, err)
		}
	}

	return nil
}
