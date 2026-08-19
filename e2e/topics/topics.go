// Package topics creates the Kafka topics one exchange/pair pipeline needs.
package topics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Retentions, in milliseconds, as in scripts/warmup.sh.
const (
	inputRetentionMS    = "3600000"   // 1 hour
	rawRetentionMS      = "604800000" // 7 days
	outputRetentionMS   = "21600000"  // 6 hours
	rejectedRetentionMS = "604800000" // 7 days — dead-letter is an audit point, read by hand long after the fact
	controlRetentionMS  = "3600000"   // 1 hour — a stale command has no value once the gap it addressed is resolved
)

// ControlTopic is the shared control-plane topic job 2 writes snapshot requests
// to. Unlike every other topic here it is not per exchange or per pair: one
// topic carries the commands for every market, and the target is in the record.
// The harness still recreates it per run, so what a scenario reads back is its
// own commands and not the previous scenario's.
const ControlTopic = "control-plane"

// normalizerStages are the raw pipeline's intermediate stages, one per job output.
var normalizerStages = []string{
	"raw-flink",                // job 1 pair-extractor out
	"type-validated-raw-flink", // job 2 type-validator out
	"rebased-flink",            // job 3 rebaser        out
	"applied-precision-flink",  // job 4 precision      out
	"orderbook-snapshot-flink", // job 5 book-builder   out
}

type topic struct {
	name        string
	retentionMS string
}

// Create creates every topic the pipeline for exchangeID/pairID needs. Topics
// that already exist are left as they are.
func Create(ctx context.Context, broker string, exchangeID, pairID int64) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(broker, ",")...))
	if err != nil {
		return err
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)
	for _, t := range plan(exchangeID, pairID) {
		if err := create(ctx, adm, t); err != nil {
			return err
		}
	}
	return nil
}

// Delete removes every topic Create would make, so a run starts from an empty
// broker with no records left over from the previous one. Topics that do not
// exist are not an error.
func Delete(ctx context.Context, broker string, exchangeID, pairID int64) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(broker, ",")...))
	if err != nil {
		return err
	}
	defer cl.Close()

	p := plan(exchangeID, pairID)
	names := make([]string, 0, len(p))
	for _, t := range p {
		names = append(names, t.name)
	}

	adm := kadm.NewClient(cl)
	res, err := adm.DeleteTopics(ctx, names...)
	if err != nil {
		return err
	}
	for _, r := range res.Sorted() {
		switch {
		case r.Err == nil:
			slog.Debug("deleted topic", "topic", r.Topic)
		case errors.Is(r.Err, kerr.UnknownTopicOrPartition):
			// Nothing to delete, which is what we want anyway.
		default:
			return fmt.Errorf("delete topic %s: %w", r.Topic, r.Err)
		}
	}

	return waitGone(ctx, adm, names)
}

// waitGone blocks until the broker stops listing names. Deletion is
// asynchronous: creating a topic that is still marked for deletion fails, so
// Create must not run until the old ones are actually gone.
func waitGone(ctx context.Context, adm *kadm.Client, names []string) error {
	for {
		details, err := adm.ListTopics(ctx)
		if err != nil {
			return err
		}

		left := ""
		for _, name := range names {
			if details.Has(name) {
				left = name
				break
			}
		}
		if left == "" {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for %s to be deleted: %w", left, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// plan lists the topics in creation order. The normalizer stages come first
// because every normalizer source reads from `latest`: a topic that does not
// exist when its job starts is discovered late, and whatever was produced in
// between is lost.
func plan(exchangeID, pairID int64) []topic {
	prefix := fmt.Sprintf("ex%d-p%d", exchangeID, pairID)

	plan := make([]topic, 0, len(normalizerStages)+5)
	for _, stage := range normalizerStages {
		plan = append(plan, topic{prefix + "-" + stage, inputRetentionMS})
	}
	return append(plan,
		// Shared dead-letter for jobs 2 and 3.
		topic{prefix + "-rejected-flink", rejectedRetentionMS},
		// Control plane — job 2's snapshot requests to NiFi. Shared across every
		// market rather than per pair, and created here rather than left to the
		// broker's auto-create so it carries warmup.sh's retention and so the
		// previous run's commands are gone before this one starts.
		topic{ControlTopic, controlRetentionMS},
		// Raw topic for the exchange (NiFi publishes verbatim exchange payloads here).
		topic{fmt.Sprintf("ex%d-raw", exchangeID), rawRetentionMS},
		// Output topics, one per side (Flink aggregation writes the aggregated book here).
		topic{fmt.Sprintf("p%d-asks", pairID), outputRetentionMS},
		topic{fmt.Sprintf("p%d-bids", pairID), outputRetentionMS},
	)
}

func create(ctx context.Context, adm *kadm.Client, t topic) error {
	configs := map[string]*string{"retention.ms": kadm.StringPtr(t.retentionMS)}

	_, err := adm.CreateTopic(ctx, 1, 1, configs, t.name)
	if errors.Is(err, kerr.TopicAlreadyExists) {
		slog.Debug("topic already exists", "topic", t.name)
		return nil
	}
	if err != nil {
		return fmt.Errorf("create topic %s: %w", t.name, err)
	}

	slog.Debug("created topic", "topic", t.name, "retention_ms", t.retentionMS)
	return nil
}
