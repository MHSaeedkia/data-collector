package main

import (
	"context"
	"fmt"
	"log"

	"orderbook-e2e/config"
	"orderbook-e2e/producer"
	"orderbook-e2e/schemaregistry"
	"orderbook-e2e/warmup"
)

func main() {
	cfg, err := config.Load(".env")
	if err != nil {
		log.Fatal(err)
	}

	if err := schemaregistry.RegisterDir(cfg.SchemaRegistryURL, cfg.SchemasDir); err != nil {
		log.Fatal(err)
	}

	if err := runTest(cfg, 1, 1, TestPayload{
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
	}); err != nil {
		log.Fatal(err)
	}
}

type TestPayload struct {
	SourceData string
	WantedData any
}

func runTest(cfg config.Config, pairID, exchangeID int64, payload TestPayload) error {
	ctx := context.Background()

	if err := warmup.Run(ctx, cfg, exchangeID, pairID); err != nil {
		return err
	}

	rawTopic := fmt.Sprintf("ex%d-raw", exchangeID)
	if err := producer.SendJSON(ctx, cfg.KafkaBroker, rawTopic, payload.SourceData); err != nil {
		return err
	}

	// verify each step has wanted value
	return nil
}
