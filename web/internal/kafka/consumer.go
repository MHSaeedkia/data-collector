// Package kafka is a thin adapter over franz-go: it owns the client and
// the poll loop, and hands each record's topic/value to a callback. It
// has no branching logic of its own (that lives in internal/ingest,
// tested there against a fake), so it isn't unit-tested here.
package kafka

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// Aggregated output topics: p{pair_id}-{side} (e.g. p2-asks).
	// Per-exchange topics carry a leading ex... so they don't match.
	aggregatedPattern = `^p[0-9]+-(asks|bids)$`
	// Job 5's per-exchange books: one record holds both sides.
	snapshotPattern = `^ex[0-9]+-p[0-9]+-orderbook-snapshot-flink$`
)

type Consumer struct {
	client *kgo.Client
}

// NewAggregatedConsumer reads the aggregator's output from the earliest
// offset, so the current book renders on page load (dev only — it
// replays the retention window each restart).
func NewAggregatedConsumer(broker string) (*Consumer, error) {
	return newConsumer(broker, "agg", aggregatedPattern, kgo.NewOffset().AtStart())
}

// NewSnapshotConsumer reads job 5's per-exchange books from the LATEST
// offset, unlike the aggregated one. These topics carry a full book on
// every event, one per exchange × pair, so replaying their retention
// window at startup would cost far more than it is worth; the trade is
// that an idle exchange shows nothing until its next event.
func NewSnapshotConsumer(broker string) (*Consumer, error) {
	return newConsumer(broker, "ex", snapshotPattern, kgo.NewOffset().AtEnd())
}

// newConsumer connects with a fresh consumer group at the given offset.
// The two families need separate clients because the reset offset is a
// client-wide setting.
func newConsumer(broker, group, pattern string, offset kgo.Offset) (*Consumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeRegex(),
		kgo.ConsumeTopics(pattern),
		kgo.ConsumerGroup("orderbook-web-"+group+"-"+strconv.FormatInt(time.Now().UnixNano(), 10)),
		kgo.ConsumeResetOffset(offset),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{client: cl}, nil
}

// Run polls until ctx is cancelled, calling onRecord for each fetched
// record. It blocks, so callers run it in a goroutine.
func (c *Consumer) Run(ctx context.Context, onRecord func(topic string, value []byte)) {
	defer c.client.Close()
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			log.Printf("Kafka fetch error %s[%d]: %v", t, p, err)
		})
		fetches.EachRecord(func(rec *kgo.Record) {
			onRecord(rec.Topic, rec.Value)
		})
	}
}
