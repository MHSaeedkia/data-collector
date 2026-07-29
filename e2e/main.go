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

// snapshotWait is how long the payload has to cross the six jobs and come back
// out on the snapshot topic.
const snapshotWait = 60 * time.Second

func main() {
	cfg, err := config.Load(".env")
	if err != nil {
		log.Fatal(err)
	}

	// if err := stack.Provision(context.Background(), cfg.ComposeFile); err != nil {
	// 	log.Fatal(err)
	// }

	err = runTest(cfg, 1, 1, TestPayload{
		SourceData: `{
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
`,
		// What SourceData becomes by the time it reaches the book builder:
		// event_time is nobitex's `lastUpdate`, ex1/BTCUSDT rebases by 10^0 on both
		// sides (identity), pair 1 truncates price to 2 and quantity to 8 decimals,
		// and every value is canonicalized (trailing zeros stripped). Asks sort
		// ascending, bids descending — the source already arrives that way.
		WantedSnapshotData: events.OrderbookSnapshot{
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
	})
	if err != nil {
		log.Fatal(err)
	}
}

type TestPayload struct {
	SourceData         string
	WantedSnapshotData events.OrderbookSnapshot
}

func runTest(cfg config.Config, pairID, exchangeID int64, payload TestPayload) error {
	ctx := context.Background()

	// The registry comes up empty with the stack, so the schemas are registered
	// after provisioning, not before it.
	if err := schemaregistry.RegisterDir(cfg.SchemaRegistryURL, cfg.SchemasDir); err != nil {
		return err
	}

	if err := warmup.Run(ctx, cfg, exchangeID, pairID); err != nil {
		return err
	}

	rawTopic := fmt.Sprintf("ex%d-raw", exchangeID)
	if err := producer.SendJSON(ctx, cfg.KafkaBroker, rawTopic, payload.SourceData); err != nil {
		return err
	}

	snapshotTopic := fmt.Sprintf("ex%d-p%d-orderbook-snapshot-flink", exchangeID, pairID)
	got, err := consumer.ReadLatestSnapshot(ctx, cfg.KafkaBroker, cfg.SchemaRegistryURL, snapshotTopic, snapshotWait)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(got, payload.WantedSnapshotData) {
		return fmt.Errorf("%s:\n got: %+v\nwant: %+v", snapshotTopic, got, payload.WantedSnapshotData)
	}

	log.Printf("%s matches the wanted snapshot", snapshotTopic)
	return nil
}
