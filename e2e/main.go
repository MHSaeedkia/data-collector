package main

import (
	"context"
	"log"

	"orderbook-e2e/config"
	"orderbook-e2e/flink"
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
	ctx := context.Background()
	pairID := 1
	exchangeID := 1

	if err := topics.Create(ctx, cfg.KafkaBroker, exchangeID, pairID); err != nil {
		return err
	}

	if err := flink.RunJobs(ctx, cfg.FlinkAPI, cfg.NormalizerDir); err != nil {
		return err
	}

	// send payloads to kafka topics
	// verify each step has wanted value
	return nil
}
