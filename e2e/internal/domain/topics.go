package domain

import "fmt"

// Retention windows, ported verbatim from scripts/warmup.sh.
const (
	rawRetentionMS      int64 = 604800000 // 7 days
	stageRetentionMS    int64 = 3600000   // 1 hour
	outputRetentionMS   int64 = 21600000  // 6 hours
	rejectedRetentionMS int64 = 604800000 // 7 days — a dead-letter is an audit point, read by hand long after the fact
)

// stages are the per-job output topic families, in pipeline order.
var stages = []string{
	"raw-flink",                // job 1 pair-extractor out
	"type-validated-raw-flink", // job 2 type-validator out
	"rebased-flink",            // job 3 rebaser        out
	"applied-precision-flink",  // job 4 precision      out
	"orderbook-snapshot-flink", // job 5 book-builder   out
}

// TopicsFor lists every topic a scenario on this scope needs.
//
// Order matters and the stage topics come first: every normalizer source reads
// from OffsetsInitializer.latest() and subscribes by topic *pattern*, so a
// topic that does not exist when its job starts is discovered late — if at all
// — and whatever was produced in the meantime is lost.
func TopicsFor(s Scope) []Topic {
	prefix := fmt.Sprintf("ex%d-p%d", s.ExchangeID, s.PairID)

	topics := make([]Topic, 0, len(stages)+4)
	for _, stage := range stages {
		topics = append(topics, Topic{Name: prefix + "-" + stage, RetentionMS: stageRetentionMS})
	}

	// Shared dead-letter for jobs 2 and 3.
	topics = append(topics, Topic{Name: prefix + "-rejected-flink", RetentionMS: rejectedRetentionMS})

	// The raw topic the harness itself produces onto.
	topics = append(topics, Topic{
		Name:        fmt.Sprintf("ex%d-raw", s.ExchangeID),
		RetentionMS: rawRetentionMS,
	})

	// The aggregated book, one topic per side.
	for _, side := range []string{"asks", "bids"} {
		topics = append(topics, Topic{
			Name:        fmt.Sprintf("p%d-%s", s.PairID, side),
			RetentionMS: outputRetentionMS,
		})
	}

	return topics
}
