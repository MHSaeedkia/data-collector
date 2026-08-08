// Package consumer reads back what the pipeline emits on its Kafka topics.
package consumer

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
// book builder emitted them.
func ReadSnapshots(ctx context.Context, broker, registryURL, topic string, wait time.Duration) ([]events.OrderbookSnapshot, error) {
	records, err := readRecords(ctx, broker, topic, wait)
	if err != nil {
		return nil, err
	}

	snapshots := make([]events.OrderbookSnapshot, 0, len(records))
	for i, r := range records {
		snapshot, err := decodeSnapshot(registryURL, r.Value)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}

// ReadRejections returns the reason of every dead-letter on topic, in the order
// it was written. Only the reason is read: rejected_at is wall-clock and the
// rejected event itself is echoed back verbatim, so neither is worth asserting.
func ReadRejections(ctx context.Context, broker, registryURL, topic string, wait time.Duration) ([]string, error) {
	records, err := readRecords(ctx, broker, topic, wait)
	if err != nil {
		return nil, err
	}

	reasons := make([]string, 0, len(records))
	for i, r := range records {
		reason, err := decodeRejection(registryURL, r.Value)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		reasons = append(reasons, reason)
	}
	return reasons, nil
}

// ReadAggregated returns every aggregated book on topic, in the order the
// aggregator emitted them. It writes one record per side per input snapshot, so
// this stream is as long as the snapshot stream that produced it.
func ReadAggregated(ctx context.Context, broker, registryURL, topic string, wait time.Duration) ([]events.AggregatedSide, error) {
	records, err := readRecords(ctx, broker, topic, wait)
	if err != nil {
		return nil, err
	}

	sides := make([]events.AggregatedSide, 0, len(records))
	for i, r := range records {
		side, err := decodeAggregated(registryURL, r.Value)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		sides = append(sides, side)
	}
	return sides, nil
}

// readRecords returns every record on topic, in offset order. The topics are
// recreated empty by the warmup, so reading from the start reads only this run's
// records. It waits up to wait for the first one — the pipeline has six jobs to
// traverse — and then keeps reading until the topic goes quiet. A topic that
// stays empty for the whole wait is not an error: some runs expect nothing on a
// topic, and it is the caller that knows how many records it wanted.
func readRecords(ctx context.Context, broker, topic string, wait time.Duration) ([]*kgo.Record, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(broker, ",")...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, err
	}
	defer cl.Close()

	slog.Debug("reading topic", "topic", topic, "wait", wait)

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
			return records, nil
		}

		if fetches.NumRecords() > 0 {
			fetches.EachRecord(func(r *kgo.Record) { records = append(records, r) })
			deadline = time.Now().Add(settle)
		}
	}
}

func decodeSnapshot(registryURL string, value []byte) (events.OrderbookSnapshot, error) {
	text, err := textual(registryURL, value)
	if err != nil {
		return events.OrderbookSnapshot{}, err
	}

	// goavro's JSON is not the shape events.OrderbookSnapshot describes: it
	// writes event_time as epoch millis, and it names union branches by their
	// full Avro name. Only the asserted fields are read; pipeline_timings is
	// wall-clock, so it is left nil, and last_sequence_id is left nil because
	// the snapshot stream is compared with DeepEqual — reading it would put it
	// in every wanted snapshot in the suite.
	var wire struct {
		ExchangeID int64               `json:"exchange_id"`
		PairID     int64               `json:"pair_id"`
		Simulation int64               `json:"simulation"`
		ID         string              `json:"id"`
		TriggerID  string              `json:"trigger_id"`
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
		Simulation: wire.Simulation,
		ID:         wire.ID,
		TriggerID:  wire.TriggerID,
		EventTime:  time.UnixMilli(wire.EventTime).UTC().Format(time.RFC3339),
		Asks:       wire.Asks,
		Bids:       wire.Bids,
	}, nil
}

// decodeAggregated reads an aggregated record. Nothing asserts event_time — it is
// the max event time of the exchanges in the union, which for ex3 is job 1's
// processing time, so it is not comparable across exchanges — but it is read, so
// that what the harness hands back is the whole record rather than a subset of
// it. The levels are what a scenario checks.
func decodeAggregated(registryURL string, value []byte) (events.AggregatedSide, error) {
	text, err := textual(registryURL, value)
	if err != nil {
		return events.AggregatedSide{}, err
	}

	var wire struct {
		PairID    int64                    `json:"pair_id"`
		Side      string                   `json:"side"`
		ID        string                   `json:"id"`
		EventTime int64                    `json:"event_time"`
		Levels    []events.AggregatedLevel `json:"levels"`
	}
	if err := json.Unmarshal(text, &wire); err != nil {
		return events.AggregatedSide{}, fmt.Errorf("decode aggregated: %w", err)
	}

	return events.AggregatedSide{
		PairID:    wire.PairID,
		Side:      wire.Side,
		ID:        wire.ID,
		EventTime: time.UnixMilli(wire.EventTime).UTC().Format(time.RFC3339),
		Levels:    wire.Levels,
	}, nil
}

func decodeRejection(registryURL string, value []byte) (string, error) {
	text, err := textual(registryURL, value)
	if err != nil {
		return "", err
	}

	var wire struct {
		RejectReason string `json:"reject_reason"`
	}
	if err := json.Unmarshal(text, &wire); err != nil {
		return "", fmt.Errorf("decode rejection: %w", err)
	}
	return wire.RejectReason, nil
}

// textual turns a Confluent-wire-format value — a 0 byte, the big-endian id of
// the schema it was written with, then the Avro datum — into goavro's JSON.
func textual(registryURL string, value []byte) ([]byte, error) {
	if len(value) < 5 || value[0] != 0 {
		return nil, fmt.Errorf("record is not Confluent wire format (%d bytes)", len(value))
	}

	schema, err := schemaregistry.SchemaByID(registryURL, int(binary.BigEndian.Uint32(value[1:5])))
	if err != nil {
		return nil, err
	}
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, err
	}
	native, _, err := codec.NativeFromBinary(value[5:])
	if err != nil {
		return nil, fmt.Errorf("decode record: %w", err)
	}
	return codec.TextualFromNative(nil, native)
}
