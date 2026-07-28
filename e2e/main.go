package main

import (
	"log"

	"orderbook-e2e/config"
	"orderbook-e2e/schemaregistry"
)

func main() {
	cfg, err := config.Load(".env")
	if err != nil {
		log.Fatal(err)
	}

	if err := schemaregistry.RegisterDir(cfg.SchemaRegistryURL, cfg.SchemasDir); err != nil {
		log.Fatal(err)
	}

	// create required topics

	// send payloads to kafka topics
	// verify each step has wanted value
}
