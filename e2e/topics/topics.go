// Package topics creates the Kafka topics one exchange/pair pipeline needs.
package topics

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

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
)

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

// plan lists the topics in creation order. The normalizer stages come first
// because every normalizer source reads from `latest`: a topic that does not
// exist when its job starts is discovered late, and whatever was produced in
// between is lost.
func plan(exchangeID, pairID int64) []topic {
	prefix := fmt.Sprintf("ex%d-p%d", exchangeID, pairID)

	plan := make([]topic, 0, len(normalizerStages)+4)
	for _, stage := range normalizerStages {
		plan = append(plan, topic{prefix + "-" + stage, inputRetentionMS})
	}
	return append(plan,
		// Shared dead-letter for jobs 2 and 3.
		topic{prefix + "-rejected-flink", rejectedRetentionMS},
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
		log.Printf("topic %s already exists", t.name)
		return nil
	}
	if err != nil {
		return fmt.Errorf("create topic %s: %w", t.name, err)
	}

	log.Printf("created %s (retention: %sms)", t.name, t.retentionMS)
	return nil
}
