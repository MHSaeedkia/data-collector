// Package scenario runs one end-to-end case against the pipeline: it brings the
// stack back to a clean start, feeds an exchange's raw topic and checks what
// comes back out of the pipeline's output topics — the book builder's snapshots,
// its dead letters, and the aggregated book the pair ends on.
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
	// aggregateWait is short for the same reason: the aggregator sits directly
	// behind the book builder, so its output is already written by the time the
	// snapshot topic has settled.
	aggregateWait = 10 * time.Second
)

// Scenario is one run: which pipeline to feed, the exchange payloads to put on
// its raw topic, and what the pipeline is expected to have emitted once they
// are through. An event can land in three places — a snapshot, a rejection, or
// nowhere at all when job 1 drops it — so the two wanted streams are declared
// per topic rather than per source, and their lengths are part of the assertion.
// The json tags are the HTTP contract: a scenario can be posted to the server
// instead of being written as a package-level var.
type Scenario struct {
	ExchangeID    int64                      `json:"exchange_id" example:"3"`
	PairID        int64                      `json:"pair_id" example:"1"`
	Sources       []string                   `json:"sources"` // raw exchange documents, verbatim
	WantSnapshots []events.OrderbookSnapshot `json:"want_snapshots"`
	WantRejects   []string                   `json:"want_rejects"` // reject_reason of each dead-letter, e.g. "sequence_gap"

	// WantAggregated is the aggregated view of the pair once every source is
	// through — the last record on `p{PairID}-asks` and on `p{PairID}-bids`.
	// Every scenario in this package spells it out, and a posted one should too.
	// Nil does NOT mean "skip job 6": the expectation is then DERIVED from the
	// last wanted snapshot, which is what the aggregated book must be when a
	// single exchange feeds the pair.
	// Only the final state is read — the aggregator emits one record per side
	// per snapshot, so asserting the whole stream would restate WantSnapshots
	// twice over.
	WantAggregated *AggregatedBook `json:"want_aggregated"`

	// IgnoreEventTime blanks EventTime on both sides before comparing. Only ex3
	// wallex needs it: its wire carries no timestamp at all, so job 1 stamps
	// processing time and the field is wall-clock. The levels are still asserted.
	IgnoreEventTime bool `json:"ignore_event_time"`
}

// AggregatedBook is the two sides of the aggregated book as the web app would
// read them. Every level carries the exchange it came from, because the
// aggregator unions across exchanges instead of summing them.
type AggregatedBook struct {
	Asks []events.AggregatedLevel `json:"asks"`
	Bids []events.AggregatedLevel `json:"bids"`
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
//
// Each source gets a fresh id injected first, exactly as NiFi does. This is
// not optional decoration: job 1 DROPS a payload that carries no id, so a
// source sent unstamped produces nothing at all and the scenario fails with an
// empty snapshot stream rather than anything that points at the cause.
func (s Scenario) produce(ctx context.Context, cfg config.Config) error {
	topic := fmt.Sprintf("ex%d-raw", s.ExchangeID)
	for i, source := range s.Sources {
		stamped, _, err := stampID(source)
		if err != nil {
			return fmt.Errorf("source %d: %w", i, err)
		}
		if err := producer.SendJSON(ctx, cfg.KafkaBroker, topic, stamped); err != nil {
			return fmt.Errorf("source %d: %w", i, err)
		}
	}
	return nil
}

// verify reads each output topic and compares it to its wanted stream: the
// snapshot stream (job 5), the dead letters (jobs 2 and 3), then the pair's
// final aggregated book (job 6).
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
	// Lineage is checked on its own terms before the literal comparison, then
	// cleared — the ids are fresh every run, so no scenario can declare them.
	// The snapshots are kept (with their ids) for the aggregated check below,
	// which is why this reads them before stripping.
	if err := checkSnapshotLineage(snapshotTopic, snapshots); err != nil {
		return err
	}
	withLineage := append([]events.OrderbookSnapshot(nil), snapshots...)
	stripSnapshotLineage(snapshots)
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

	return s.verifyAggregated(ctx, cfg, withLineage)
}

// verifyAggregated checks the last record on each of the pair's two aggregated
// topics. Those topics are the pair's, not the exchange's — the aggregator keys
// on (pair_id, side) across every exchange — but a scenario feeds one exchange
// and the warmup emptied the topics, so what is there is this run's.
//
// A scenario that wants no snapshot at all leaves job 6 nothing to emit, and
// the empty snapshot stream has already been asserted, so there is nothing left
// to read.
//
// snapshots are job 5's records WITH their lineage intact, so the levels of the
// aggregated book can be matched against the snapshot they claim to come from —
// the only exact cross-job lineage assertion the harness can make.
func (s Scenario) verifyAggregated(ctx context.Context, cfg config.Config,
	snapshots []events.OrderbookSnapshot) error {
	if s.WantAggregated == nil && len(s.WantSnapshots) == 0 {
		return nil
	}
	want := s.wantAggregated()

	for _, side := range []struct {
		name string
		want []events.AggregatedLevel
	}{
		{"asks", want.Asks},
		{"bids", want.Bids},
	} {
		topic := fmt.Sprintf("p%d-%s", s.PairID, side.name)
		records, err := consumer.ReadAggregated(ctx, cfg.KafkaBroker, cfg.SchemaRegistryURL, topic, aggregateWait)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return fmt.Errorf("%s: no aggregated records, want a final book", topic)
		}

		final := records[len(records)-1]
		if final.PairID != s.PairID || final.Side != side.name {
			return fmt.Errorf("%s: record is pair %d %s, want pair %d %s",
				topic, final.PairID, final.Side, s.PairID, side.name)
		}
		if err := checkAggregatedLineage(topic, final, snapshots); err != nil {
			return err
		}
		stripAggregatedLineage(final.Levels)
		if err := compare(topic, final.Levels, side.want); err != nil {
			return err
		}
		log.Printf("matched the final aggregated book on %s (%d records seen)", topic, len(records))
	}
	return nil
}

// wantAggregated is the book the aggregated topics must end on: the scenario's
// own WantAggregated when it has one, otherwise the last wanted snapshot with
// the exchange stamped on every level.
//
// That derivation is exact because a scenario feeds ONE exchange: the union is
// a union of one, so job 6's output is job 5's last book plus the exchange tag.
// The order survives too — both jobs sort asks ascending and bids descending,
// and the aggregator's quantity tie-break never fires within a single exchange
// because job 4 has already merged levels that share a price.
func (s Scenario) wantAggregated() AggregatedBook {
	if s.WantAggregated != nil {
		return *s.WantAggregated
	}
	last := s.WantSnapshots[len(s.WantSnapshots)-1]
	return AggregatedBook{
		Asks: stampExchange(s.ExchangeID, last.Simulation, last.Asks),
		Bids: stampExchange(s.ExchangeID, last.Simulation, last.Bids),
	}
}

// stampExchange rewrites one side of a book-builder snapshot in the aggregated
// form: the same levels in the same order, each tagged with the exchange it
// came from and that book's simulation flag.
func stampExchange(exchangeID, simulation int64, levels []events.PriceLevel) []events.AggregatedLevel {
	stamped := make([]events.AggregatedLevel, 0, len(levels))
	for _, level := range levels {
		stamped = append(stamped, events.AggregatedLevel{
			ExchangeID: exchangeID,
			Simulation: simulation,
			Price:      level.Price,
			Quantity:   level.Quantity,
		})
	}
	return stamped
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
