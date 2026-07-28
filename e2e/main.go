package main

import (
	"context"
	"log"

	"orderbook-e2e/config"
	"orderbook-e2e/schemaregistry"
	"orderbook-e2e/topics"
)

func main() {
	cfg, err := config.Load(".env")
	if err != nil {
		log.Fatal(err)
	}

	if err := schemaregistry.RegisterDir(cfg.SchemaRegistryURL, cfg.SchemasDir); err != nil {
		log.Fatal(err)
	}

	if err := runTest(cfg); err != nil {
		log.Fatal(err)
	}
}

func runTest(cfg config.Config) error {
	pairID := 1
	exchangeID := 1

	if err := topics.Create(context.Background(), cfg.KafkaBroker, exchangeID, pairID); err != nil {
		return err
	}

	// send payloads to kafka topics
	// verify each step has wanted value
	return nil
}
