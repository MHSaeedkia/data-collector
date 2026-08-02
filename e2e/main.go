package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"orderbook-e2e/config"
	"orderbook-e2e/scenario"
	"orderbook-e2e/server"
	"orderbook-e2e/stack"
)

// @title			Orderbook E2E Harness API
// @version		1.0
// @description	Runs one end-to-end scenario against the normalizer pipeline: the stack is provisioned once when the server starts, then every request warms the pipeline up for its own exchange/pair, feeds the raw topic and checks what came back out.
// @description
// @description	The same `Scenario` the compiled-in cases use, posted as JSON. Regenerate this spec with `swag init -g main.go -o docs` after changing the handler or the scenario struct.
// @host			localhost:9595
// @BasePath		/
func main() {
	serve := flag.Bool("serve", false, "serve scenarios over HTTP instead of running the built-in list")
	addr := flag.String("addr", ":9595", "address the -serve listener binds to")
	provisionStack := flag.Bool("provision-stack", true, "provision stack using `docker compose up -d` command")

	flag.Parse()

	cfg, err := config.Load(".env")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	if provisionStack != nil && *provisionStack {
		if err := stack.Provision(ctx, cfg.ComposeFile); err != nil {
			log.Fatal(err)
		}
	}

	if *serve {
		runServer(cfg, *addr)
		return
	}

	// One failure does not stop the run: a suite this slow is only worth waiting
	// on if it reports every case it can, so failures are collected and listed
	// at the end.
	var failed []string
	for i, sc := range scenario.Scenarios {
		log.Printf("=== %d/%d %s", i+1, len(scenario.Scenarios), sc.Name)
		if err := scenario.Run(ctx, cfg, sc.S); err != nil {
			log.Printf("FAIL %s: %v", sc.Name, err)
			failed = append(failed, sc.Name)
		} else {
			log.Printf("PASS %s", sc.Name)
		}
		log.Println("=================================")
	}

	if len(failed) > 0 {
		log.Printf("%d/%d scenarios failed: %v", len(failed), len(scenario.Scenarios), failed)
		os.Exit(1)
	}
	log.Printf("all %d scenarios passed", len(scenario.Scenarios))
}

// runServer blocks serving scenarios over HTTP. There is no write timeout: a
// run warms the pipeline up and then waits a minute on the snapshot topic, so
// the response is minutes away and the server must not cut it off. Only the
// header read is bounded.
func runServer(cfg config.Config, addr string) {
	srv := &http.Server{
		Addr:              addr,
		Handler:           server.New(cfg),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("listening on %s — POST /scenarios/run", addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
