// Package scenario runs one end-to-end case against the pipeline: it brings the
// stack back to a clean start, feeds an exchange's raw topic and checks what
// comes back out of the pipeline's two output topics.
package scenario

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"time"

	"orderbook-e2e/config"
	"orderbook-e2e/consumer"
	"orderbook-e2e/events"
	"orderbook-e2e/producer"
	"orderbook-e2e/schemaregistry"
	"orderbook-e2e/warmup"
)

const (
	// snapshotWait is how long the sources have to cross the six jobs and come
	// back out on the snapshot topic.
	snapshotWait = 60 * time.Second
	// rejectWait is short because the dead-letter topic is only read once the
	// snapshot topic has gone quiet.
	rejectWait = 10 * time.Second
)

// Scenario is one run: which pipeline to feed, the exchange payloads to put on
// its raw topic, and what the pipeline is expected to have emitted once they
// are through. An event can land in three places — a snapshot, a rejection, or
// nowhere at all when job 1 drops it — so the two wanted streams are declared
// per topic rather than per source, and their lengths are part of the assertion.
type Scenario struct {
	ExchangeID    int64
	PairID        int64
	Sources       []string
	WantSnapshots []events.OrderbookSnapshot
	WantRejects   []string // reject_reason of each dead-letter, e.g. "sequence_gap"

	// IgnoreEventTime blanks EventTime on both sides before comparing. Only ex3
	// wallex needs it: its wire carries no timestamp at all, so job 1 stamps
	// processing time and the field is wall-clock. The levels are still asserted.
	IgnoreEventTime bool
}

// Run brings the pipeline back to a clean start, feeds it s's sources and
// checks both output topics against what s wants.
func Run(ctx context.Context, cfg config.Config, s Scenario) error {
	// The registry comes up empty with the stack, so the schemas are registered
	// after provisioning, not before it.
	if err := schemaregistry.RegisterDir(cfg.SchemaRegistryURL, cfg.SchemasDir); err != nil {
		return err
	}

	if err := warmup.Run(ctx, cfg, s.ExchangeID, s.PairID); err != nil {
		return err
	}

	if err := s.produce(ctx, cfg); err != nil {
		return err
	}
	return s.verify(ctx, cfg)
}

// produce puts every source on the exchange's raw topic, in order. The raw
// topic has one partition and the jobs run at parallelism 1, so each output
// stream comes out in the order its sources went in.
func (s Scenario) produce(ctx context.Context, cfg config.Config) error {
	topic := fmt.Sprintf("ex%d-raw", s.ExchangeID)
	for i, source := range s.Sources {
		if err := producer.SendJSON(ctx, cfg.KafkaBroker, topic, source); err != nil {
			return fmt.Errorf("source %d: %w", i, err)
		}
	}
	return nil
}

// verify reads both output topics and compares each to its wanted stream.
//
// Snapshots are read first, for the full budget. Jobs 2 and 3 are upstream of
// the book builder, so anything they were going to reject is already on the
// dead-letter topic by the time the snapshots have settled — a run that expects
// no rejection does not have to wait the full budget a second time.
func (s Scenario) verify(ctx context.Context, cfg config.Config) error {
	prefix := fmt.Sprintf("ex%d-p%d", s.ExchangeID, s.PairID)

	snapshotTopic := prefix + "-orderbook-snapshot-flink"
	snapshots, err := consumer.ReadSnapshots(ctx, cfg.KafkaBroker, cfg.SchemaRegistryURL, snapshotTopic, snapshotWait)
	if err != nil {
		return err
	}
	if s.IgnoreEventTime {
		for i := range snapshots {
			snapshots[i].EventTime = ""
		}
	}
	if err := compare(snapshotTopic, snapshots, s.WantSnapshots); err != nil {
		return err
	}

	rejectedTopic := prefix + "-rejected-flink"
	rejects, err := consumer.ReadRejections(ctx, cfg.KafkaBroker, cfg.SchemaRegistryURL, rejectedTopic, rejectWait)
	if err != nil {
		return err
	}
	if err := compare(rejectedTopic, rejects, s.WantRejects); err != nil {
		return err
	}

	log.Printf("matched %d snapshots and %d rejections", len(snapshots), len(rejects))
	return nil
}

// compare checks that a topic carried exactly the wanted records, in order.
func compare[T any](topic string, got, want []T) error {
	if len(got) != len(want) {
		return fmt.Errorf("%s: got %d records, want %d\n got: %+v\nwant: %+v", topic, len(got), len(want), got, want)
	}
	for i := range want {
		if !reflect.DeepEqual(got[i], want[i]) {
			return fmt.Errorf("%s record %d:\n got: %+v\nwant: %+v", topic, i, got[i], want[i])
		}
	}
	return nil
}
