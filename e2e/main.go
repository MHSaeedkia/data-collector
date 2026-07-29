package main

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
	// snapshotWait is how long the payload has to cross the six jobs and come back
	// out on the snapshot topic.
	snapshotWait = 60 * time.Second
	// rejectWait is short because the dead-letter topic is only read once the
	// snapshot topic has gone quiet.
	rejectWait = 10 * time.Second
)

func main() {
	cfg, err := config.Load(".env")
	if err != nil {
		log.Fatal(err)
	}

	// if err := stack.Provision(context.Background(), cfg.ComposeFile); err != nil {
	// 	log.Fatal(err)
	// }

	err = runTest(cfg, 1, 1, Scenario{
		Sources: []string{`{
	"action": "snapshot",
	"pair": "BTCUSDT",
	"status": "ok",
	"lastUpdate": 1800000000000,
	"lastTradePrice": "62650",
	"bids": [
		["62649", "0.50000000"],
		["62648", "0.02744953"],
		["62647", "0.20630833"],
		["62645", "0.90000000"],
		["62640", "1.31062803"]
	],
	"asks": [
		["62650", "2.21924167"],
		["62651", "0.17447383"],
		["62652", "0.19067482"],
		["62655", "1.05000000"],
		["62660", "0.33476925"]
	]
}
`},
		// What the source becomes by the time it reaches the book builder:
		// event_time is nobitex's `lastUpdate`, ex1/BTCUSDT rebases by 10^0 on both
		// sides (identity), pair 1 truncates price to 2 and quantity to 8 decimals,
		// and every value is canonicalized (trailing zeros stripped). Asks sort
		// ascending, bids descending — the source already arrives that way.
		WantSnapshots: []events.OrderbookSnapshot{
			{
				ExchangeID: 1,
				PairID:     1,
				EventTime:  "2027-01-15T08:00:00Z",
				Asks: []events.PriceLevel{
					{Price: "62650", Quantity: "2.21924167"},
					{Price: "62651", Quantity: "0.17447383"},
					{Price: "62652", Quantity: "0.19067482"},
					{Price: "62655", Quantity: "1.05"},
					{Price: "62660", Quantity: "0.33476925"},
				},
				Bids: []events.PriceLevel{
					{Price: "62649", Quantity: "0.5"},
					{Price: "62648", Quantity: "0.02744953"},
					{Price: "62647", Quantity: "0.20630833"},
					{Price: "62645", Quantity: "0.9"},
					{Price: "62640", Quantity: "1.31062803"},
				},
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
}

// Scenario is one run: the exchange payloads to feed the raw topic, and what
// the pipeline is expected to have emitted once they are through. An event can
// land in three places — a snapshot, a rejection, or nowhere at all when job 1
// drops it — so the two wanted streams are declared per topic rather than per
// source, and their lengths are part of the assertion.
type Scenario struct {
	Sources       []string
	WantSnapshots []events.OrderbookSnapshot
	WantRejects   []string // reject_reason of each dead-letter, e.g. "sequence_gap"
}

func runTest(cfg config.Config, pairID, exchangeID int64, scenario Scenario) error {
	ctx := context.Background()

	// The registry comes up empty with the stack, so the schemas are registered
	// after provisioning, not before it.
	if err := schemaregistry.RegisterDir(cfg.SchemaRegistryURL, cfg.SchemasDir); err != nil {
		return err
	}

	if err := warmup.Run(ctx, cfg, exchangeID, pairID); err != nil {
		return err
	}

	// The raw topic has one partition and the jobs run at parallelism 1, so each
	// stream comes out in the order its sources went in.
	rawTopic := fmt.Sprintf("ex%d-raw", exchangeID)
	for i, source := range scenario.Sources {
		if err := producer.SendJSON(ctx, cfg.KafkaBroker, rawTopic, source); err != nil {
			return fmt.Errorf("source %d: %w", i, err)
		}
	}

	snapshotTopic := fmt.Sprintf("ex%d-p%d-orderbook-snapshot-flink", exchangeID, pairID)
	snapshots, err := consumer.ReadSnapshots(ctx, cfg.KafkaBroker, cfg.SchemaRegistryURL, snapshotTopic, snapshotWait)
	if err != nil {
		return err
	}
	if err := compare(snapshotTopic, snapshots, scenario.WantSnapshots); err != nil {
		return err
	}

	// Jobs 2 and 3 are upstream of the book builder, so anything they were going
	// to reject is already on the topic by the time the snapshots have settled —
	// a run that expects no rejection does not have to wait the full budget again.
	rejectedTopic := fmt.Sprintf("ex%d-p%d-rejected-flink", exchangeID, pairID)
	rejects, err := consumer.ReadRejections(ctx, cfg.KafkaBroker, cfg.SchemaRegistryURL, rejectedTopic, rejectWait)
	if err != nil {
		return err
	}
	if err := compare(rejectedTopic, rejects, scenario.WantRejects); err != nil {
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
