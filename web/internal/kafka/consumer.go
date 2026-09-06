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
	// Aggregated output topics: p{pair_id}-{side} (e.g. p2-asks), plus the
	// merger's p{pair_id}-{side}-merged. Per-exchange topics carry a
	// leading ex... so they don't match.
	//
	// The -merged suffix is optional here ON PURPOSE, and this regex is
	// the mirror image of the merger job's own: that one must exclude
	// -merged or it would consume its own output forever, while this one
	// wants both views. Same record rate and same shape of consumer, so
	// they share this client rather than needing a third.
	aggregatedPattern = `^p[0-9]+-(asks|bids)(-merged)?$`
	// Job 5's per-exchange books: one record holds both sides.
	snapshotPattern = `^ex[0-9]+-p[0-9]+-orderbook-snapshot-flink$`
)

type Consumer struct {
	client *kgo.Client
}

// NewAggregatedConsumer reads the aggregator's and the merger's output
// from the LATEST offset: live records only, no history.
//
// It used to start at the earliest offset so the book painted on page
// load. That was a dev convenience and it became the app's worst failure
// mode in production: warmup.sh keeps 6 hours on these topics, so every
// restart replayed six hours of full order books at fetch speed and
// pushed each one at the browser, which cannot render that fast. The
// socket backed up and the whole server froze (see internal/hub). The
// trade for reading live only is that a quiet pair shows nothing until
// its next record; the hub's snapshot answer covers everything that has
// arrived since the process started.
func NewAggregatedConsumer(broker string) (*Consumer, error) {
	return newConsumer(broker, "agg", aggregatedPattern, kgo.NewOffset().AtEnd())
}

// NewSnapshotConsumer reads job 5's per-exchange books, also from the
// latest offset. These topics carry a full book on every event, one per
// exchange × pair, so replaying their retention window at startup would
// cost far more than it is worth.
func NewSnapshotConsumer(broker string) (*Consumer, error) {
	return newConsumer(broker, "ex", snapshotPattern, kgo.NewOffset().AtEnd())
}

// newConsumer connects with a fresh consumer group at the given offset.
// Both families now read from the end, so separate clients are no longer
// forced by the client-wide reset offset — they stay apart only because
// they are two independent subscriptions. The group name carries a
// timestamp so every start is a new group: a stable name would resume
// from committed offsets and reintroduce the backlog replay that reading
// from the end exists to avoid.
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
