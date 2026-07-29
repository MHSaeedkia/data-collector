package main

import (
	"context"
	"log"

	"orderbook-e2e/config"
	"orderbook-e2e/scenario"
)

func main() {
	cfg, err := config.Load(".env")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// if err := stack.Provision(ctx, cfg.ComposeFile); err != nil {
	// 	log.Fatal(err)
	// }

	if err := scenario.Run(ctx, cfg, scenario.NobitexSnapshot); err != nil {
		log.Fatal(err)
	}
}
