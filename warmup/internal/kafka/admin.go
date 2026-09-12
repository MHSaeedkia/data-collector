// Package kafka makes the broker match the topic plan, in batches.
//
// Batching is the whole point of this tool: the shell script it replaced ran
// one `docker exec kafka kafka-topics --create` — and with it a whole JVM — per
// topic, which is about an hour at ~3000 topics. The admin API takes them a
// batch at a time, so the same work is a handful of requests.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"orderbook-warmup/internal/config"
	"orderbook-warmup/internal/domain"
	"orderbook-warmup/internal/topics"
)

// Reconcile creates the topics the broker is missing and rewrites retention.ms
// on the ones whose retention changed in .env. Topics that are not in the plan
// are never touched.
func Reconcile(ctx context.Context, cfg config.Config, desired []domain.Topic) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(cfg.Bootstrap, ",")...))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	existing, err := currentRetentions(ctx, adm, desired, cfg.BatchSize)
	if err != nil {
		return err
	}

	create, alter := topics.Diff(desired, existing)
	slog.Info("topic plan",
		"planned", len(desired), "existing", len(existing),
		"to_create", len(create), "to_retune", len(alter))

	if err := createTopics(ctx, adm, cfg, create); err != nil {
		return err
	}
	return alterRetentions(ctx, adm, cfg, alter, existing)
}

// currentRetentions returns the retention.ms of every planned topic that
// already exists. Topics the broker does not have are absent from the map.
func currentRetentions(ctx context.Context, adm *kadm.Client, desired []domain.Topic, batchSize int) (map[string]string, error) {
	details, err := adm.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}

	var names []string
	for _, t := range desired {
		if details.Has(t.Name) {
			names = append(names, t.Name)
		}
	}

	retentions := make(map[string]string, len(names))
	for _, batch := range chunk(names, batchSize) {
		configs, err := adm.DescribeTopicConfigs(ctx, batch...)
		if err != nil {
			return nil, fmt.Errorf("describe topic configs: %w", err)
		}
		for _, rc := range configs {
			if rc.Err != nil {
				// The topic went away between the list and the describe, or the
				// broker refused it. Treating it as "not there" is right for
				// both: the create call reports the real error if there is one.
				slog.Debug("cannot describe topic", "topic", rc.Name, "err", rc.Err)
				continue
			}
			retentions[rc.Name] = ""
			for _, c := range rc.Configs {
				if c.Key == "retention.ms" {
					retentions[rc.Name] = c.MaybeValue()
					break
				}
			}
		}
	}
	return retentions, nil
}

func createTopics(ctx context.Context, adm *kadm.Client, cfg config.Config, create []domain.Topic) error {
	for retention, names := range groupByRetention(create) {
		configs := map[string]*string{"retention.ms": kadm.StringPtr(retention)}
		for _, batch := range chunk(names, cfg.BatchSize) {
			responses, err := adm.CreateTopics(ctx, cfg.Partitions, cfg.Replication, configs, batch...)
			if err != nil {
				return fmt.Errorf("create topics: %w", err)
			}
			for _, r := range responses {
				switch {
				case r.Err == nil:
					slog.Debug("created topic", "topic", r.Topic, "retention_ms", retention)
				case errors.Is(r.Err, kerr.TopicAlreadyExists):
					// Someone else created it in between; its retention is then
					// theirs, and the next run reconciles it.
					slog.Debug("topic already exists", "topic", r.Topic)
				default:
					return fmt.Errorf("create topic %s: %w", r.Topic, r.Err)
				}
			}
		}
		slog.Info("created topics", "count", len(names), "retention_ms", retention)
	}
	return nil
}

// alterRetentions rewrites retention.ms on topics that already exist. This is
// what makes a retention change in .env take effect: creation alone would leave
// an existing topic on whatever retention it was created with.
//
// ⚠ Lowering a retention deletes whatever is already past the new limit.
func alterRetentions(ctx context.Context, adm *kadm.Client, cfg config.Config, alter []domain.Topic, existing map[string]string) error {
	for retention, names := range groupByRetention(alter) {
		configs := []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "retention.ms", Value: kadm.StringPtr(retention)}}
		for _, batch := range chunk(names, cfg.BatchSize) {
			responses, err := adm.AlterTopicConfigs(ctx, configs, batch...)
			if err != nil {
				return fmt.Errorf("alter topic configs: %w", err)
			}
			for _, r := range responses {
				if r.Err != nil {
					return fmt.Errorf("alter retention on %s: %w", r.Name, r.Err)
				}
				slog.Debug("retention changed", "topic", r.Name, "from", existing[r.Name], "to", retention)
			}
		}
		slog.Info("changed retention", "count", len(names), "retention_ms", retention, "was", existing[names[0]])
	}
	return nil
}

// groupByRetention buckets topic names by retention so each bucket can go to
// the broker in one request: a create or alter call carries one config set for
// every topic in it.
func groupByRetention(plan []domain.Topic) map[string][]string {
	groups := map[string][]string{}
	for _, t := range plan {
		groups[t.RetentionMS] = append(groups[t.RetentionMS], t.Name)
	}
	return groups
}

// chunk splits names into batches of at most size, so one request does not ask
// the controller to create several thousand topics at once.
func chunk(names []string, size int) [][]string {
	var batches [][]string
	for start := 0; start < len(names); start += size {
		batches = append(batches, names[start:min(start+size, len(names))])
	}
	return batches
}
