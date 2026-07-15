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

// Consolidated output topics: p{pair_id}-{side} (e.g. p2-asks). Input
// topics (ex{exchange_id}-p{pair_id}-{side}) carry a leading ex... so
// they don't match.
const topicPattern = `^p[0-9]+-(asks|bids)$`

type Consumer struct {
	client *kgo.Client
}

// NewConsumer connects with a fresh consumer group reset to the earliest
// offset, so the current book replays on load (dev only).
func NewConsumer(broker string) (*Consumer, error) {
	group := "orderbook-web-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeRegex(),
		kgo.ConsumeTopics(topicPattern),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
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
