// Package consumer reads back what the pipeline emits on its Kafka topics.
package consumer

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/twmb/franz-go/pkg/kgo"

	"orderbook-e2e/events"
	"orderbook-e2e/schemaregistry"
)

// settle is how long the topic must stay quiet after a record before the last
// one is taken as the latest: the book builder emits the whole book on every
// event, so an assertion made on the first record could be one book behind.
const settle = 2 * time.Second

// ReadLatestSnapshot returns the last order book snapshot on topic. The topics
// are recreated empty by the warmup, so reading from the start reads only this
// run's records. It waits up to wait for the first one — the pipeline has six
// jobs to traverse — and then until the topic goes quiet.
func ReadLatestSnapshot(ctx context.Context, broker, registryURL, topic string, wait time.Duration) (events.OrderbookSnapshot, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(broker, ",")...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return events.OrderbookSnapshot{}, err
	}
	defer cl.Close()

	last, err := pollLast(ctx, cl, topic, wait)
	if err != nil {
		return events.OrderbookSnapshot{}, err
	}
	return decode(registryURL, last.Value)
}

// pollLast polls until the topic has produced at least one record and then gone
// quiet for settle, and returns the last record it saw.
func pollLast(ctx context.Context, cl *kgo.Client, topic string, wait time.Duration) (*kgo.Record, error) {
	log.Printf("waiting up to %s for a snapshot on %s...", wait, topic)

	var last *kgo.Record
	deadline := time.Now().Add(wait)
	for {
		pollCtx, cancel := context.WithDeadline(ctx, deadline)
		fetches := cl.PollFetches(pollCtx)
		cancel()

		if err := fetches.Err(); err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				return nil, fmt.Errorf("consume %s: %w", topic, err)
			}
			if last == nil {
				return nil, fmt.Errorf("no record on %s within %s", topic, wait)
			}
			return last, nil
		}

		if fetches.NumRecords() > 0 {
			fetches.EachRecord(func(r *kgo.Record) { last = r })
			deadline = time.Now().Add(settle)
		}
	}
}

// decode reads a Confluent-wire-format value: a 0 byte, the big-endian id of the
// schema it was written with, then the Avro datum.
func decode(registryURL string, value []byte) (events.OrderbookSnapshot, error) {
	if len(value) < 5 || value[0] != 0 {
		return events.OrderbookSnapshot{}, fmt.Errorf("record is not Confluent wire format (%d bytes)", len(value))
	}

	schema, err := schemaregistry.SchemaByID(registryURL, int(binary.BigEndian.Uint32(value[1:5])))
	if err != nil {
		return events.OrderbookSnapshot{}, err
	}
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return events.OrderbookSnapshot{}, err
	}
	native, _, err := codec.NativeFromBinary(value[5:])
	if err != nil {
		return events.OrderbookSnapshot{}, fmt.Errorf("decode snapshot: %w", err)
	}
	text, err := codec.TextualFromNative(nil, native)
	if err != nil {
		return events.OrderbookSnapshot{}, err
	}

	// goavro's JSON is not the shape events.OrderbookSnapshot describes: it
	// writes event_time as epoch millis, and it names union branches by their
	// full Avro name. Only the asserted fields are read; pipeline_timings is
	// wall-clock, so it is left nil.
	var wire struct {
		ExchangeID int64               `json:"exchange_id"`
		PairID     int64               `json:"pair_id"`
		EventTime  int64               `json:"event_time"`
		Asks       []events.PriceLevel `json:"asks"`
		Bids       []events.PriceLevel `json:"bids"`
	}
	if err := json.Unmarshal(text, &wire); err != nil {
		return events.OrderbookSnapshot{}, fmt.Errorf("decode snapshot: %w", err)
	}

	return events.OrderbookSnapshot{
		ExchangeID: wire.ExchangeID,
		PairID:     wire.PairID,
		EventTime:  time.UnixMilli(wire.EventTime).UTC().Format(time.RFC3339),
		Asks:       wire.Asks,
		Bids:       wire.Bids,
	}, nil
}
