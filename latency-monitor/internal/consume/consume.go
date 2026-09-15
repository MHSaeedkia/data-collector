// Package consume tails one Kafka topic from its end.
package consume

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Record is one Kafka record, reduced to what the monitor reads.
type Record struct {
	Partition int32
	Offset    int64
	// Timestamp is the record's own metadata timestamp — the broker's append
	// time, since the cluster runs LogAppendTime — NOT a field in the payload.
	Timestamp time.Time
	Value     []byte
}

// Tail reads topic from its END and calls handle for every record that arrives,
// in the order the broker returns them, until ctx is cancelled. No consumer
// group: this is an observer, and joining one would make two people watching
// the same topic split the records between them instead of both seeing all.
func Tail(ctx context.Context, brokers, topic string, handle func(Record) error) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	for {
		fetches := cl.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}

		var handleErr error
		fetches.EachRecord(func(r *kgo.Record) {
			if handleErr != nil {
				return
			}
			handleErr = handle(Record{
				Partition: r.Partition,
				Offset:    r.Offset,
				Timestamp: r.Timestamp,
				Value:     r.Value,
			})
		})
		if handleErr != nil {
			return handleErr
		}
	}
}
