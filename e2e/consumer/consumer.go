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

// settle is how long the topic must stay quiet before what has been read is
// taken as everything the pipeline emitted for this run.
const settle = 2 * time.Second

// ReadSnapshots returns every order book snapshot on topic, in the order the
// book builder emitted them. The topics are recreated empty by the warmup, so
// reading from the start reads only this run's records. It waits up to wait for
// the first one — the pipeline has six jobs to traverse — and then keeps reading
// until the topic goes quiet.
func ReadSnapshots(ctx context.Context, broker, registryURL, topic string, wait time.Duration) ([]events.OrderbookSnapshot, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(broker, ",")...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, err
	}
	defer cl.Close()

	records, err := pollAll(ctx, cl, topic, wait)
	if err != nil {
		return nil, err
	}

	snapshots := make([]events.OrderbookSnapshot, 0, len(records))
	for i, r := range records {
		snapshot, err := decode(registryURL, r.Value)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}

// pollAll polls until the topic has produced at least one record and then gone
// quiet for settle, and returns every record it saw, in offset order.
func pollAll(ctx context.Context, cl *kgo.Client, topic string, wait time.Duration) ([]*kgo.Record, error) {
	log.Printf("waiting up to %s for snapshots on %s...", wait, topic)

	var records []*kgo.Record
	deadline := time.Now().Add(wait)
	for {
		pollCtx, cancel := context.WithDeadline(ctx, deadline)
		fetches := cl.PollFetches(pollCtx)
		cancel()

		if err := fetches.Err(); err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				return nil, fmt.Errorf("consume %s: %w", topic, err)
			}
			if len(records) == 0 {
				return nil, fmt.Errorf("no record on %s within %s", topic, wait)
			}
			return records, nil
		}

		if fetches.NumRecords() > 0 {
			fetches.EachRecord(func(r *kgo.Record) { records = append(records, r) })
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
