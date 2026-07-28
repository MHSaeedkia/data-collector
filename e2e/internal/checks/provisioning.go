// Package checks holds the expectations a scenario asserts against a running
// stack. They are adapters: each one talks to a real endpoint and reports what
// it found as domain.Failure values, so the runner and the report stay free of
// Kafka, Flink and HTTP.
package checks

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"orderbook-e2e/internal/domain"
	"orderbook-e2e/internal/flink"
)

// Provisioning asserts the three things every scenario depends on before it
// produces a single byte.
//
// It asserts nothing about the pipeline's behaviour. That is the point: when a
// scenario fails, these say whether the environment or the pipeline is to
// blame.
func Provisioning(scope domain.Scope) []domain.Check {
	return []domain.Check{
		JobsRunning,
		TopicsExist(scope),
		SubjectsRegistered,
	}
}

// JobsRunning checks that all six normalizer jobs reached RUNNING.
func JobsRunning(ctx context.Context, e domain.Endpoints) []domain.Failure {
	const name = "all six jobs are running"

	running, err := flink.New(e.FlinkAPI).RunningJobs(ctx)
	if err != nil {
		return []domain.Failure{{Check: name, Detail: fmt.Sprintf("querying the job manager: %v", err)}}
	}

	if len(running) != len(domain.JobModules) {
		sort.Strings(running)
		return []domain.Failure{{
			Check:  name,
			Detail: fmt.Sprintf("want %d running, got %d: %v", len(domain.JobModules), len(running), running),
		}}
	}
	return nil
}

// TopicsExist checks that every topic the scope needs was created. Sources
// subscribe by pattern at latest(), so a missing topic does not fail loudly at
// runtime — it silently drops everything produced to it.
func TopicsExist(scope domain.Scope) domain.Check {
	const name = "the scope's topics exist"

	return func(ctx context.Context, e domain.Endpoints) []domain.Failure {
		client, err := kgo.NewClient(kgo.SeedBrokers(e.KafkaBroker))
		if err != nil {
			return []domain.Failure{{Check: name, Detail: fmt.Sprintf("kafka client: %v", err)}}
		}
		defer client.Close()

		details, err := kadm.NewClient(client).ListTopics(ctx)
		if err != nil {
			return []domain.Failure{{Check: name, Detail: fmt.Sprintf("listing topics: %v", err)}}
		}

		var missing []string
		for _, topic := range domain.TopicsFor(scope) {
			if !details.Has(topic.Name) {
				missing = append(missing, topic.Name)
			}
		}
		if len(missing) > 0 {
			return []domain.Failure{{Check: name, Detail: fmt.Sprintf("missing %v", missing)}}
		}
		return nil
	}
}

// SubjectsRegistered checks that all four Avro subjects are in the registry.
// The jobs resolve them on first use, so a missing one fails the job at
// runtime rather than at submission.
func SubjectsRegistered(ctx context.Context, e domain.Endpoints) []domain.Failure {
	const name = "every subject is registered"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.SchemaRegistryURL+"/subjects", nil)
	if err != nil {
		return []domain.Failure{{Check: name, Detail: err.Error()}}
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return []domain.Failure{{Check: name, Detail: fmt.Sprintf("querying the registry: %v", err)}}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return []domain.Failure{{Check: name, Detail: fmt.Sprintf("registry returned %d", resp.StatusCode)}}
	}

	var subjects []string
	if err := json.NewDecoder(resp.Body).Decode(&subjects); err != nil {
		return []domain.Failure{{Check: name, Detail: fmt.Sprintf("decoding /subjects: %v", err)}}
	}

	registered := make(map[string]bool, len(subjects))
	for _, s := range subjects {
		registered[s] = true
	}

	var missing []string
	for _, s := range domain.SchemaFiles {
		if !registered[s.Subject] {
			missing = append(missing, s.Subject)
		}
	}
	if len(missing) > 0 {
		return []domain.Failure{{Check: name, Detail: fmt.Sprintf("missing %v", missing)}}
	}
	return nil
}
