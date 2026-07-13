package main

import (
	"context"
	"embed"
	"io/fs"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"orderbook-web/internal/config"
	"orderbook-web/internal/hub"
	"orderbook-web/internal/ingest"
	"orderbook-web/internal/kafka"
	"orderbook-web/internal/postgres"
	"orderbook-web/internal/registry"
	"orderbook-web/internal/schema"
)

// The UI is baked into the binary so it ships as a single self-contained executable.
//
//go:embed public
var staticFiles embed.FS

func main() {
	ctx := context.Background()

	cfg := config.Load(".env")

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("postgres pool: %v", err)
	}
	defer pool.Close()

	reg := registry.New(postgres.NewRepository(pool))
	reg.Refresh(ctx) // initial load before anything uses the maps
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for range t.C {
			reg.Refresh(ctx)
		}
	}()

	h := hub.New()
	dec := schema.NewDecoder(cfg.SchemaRegistryURL)

	publicFS, err := fs.Sub(staticFiles, "public")
	if err != nil {
		log.Fatalf("embedded ui: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(publicFS)))
	mux.HandleFunc("/ws", h.ServeWS)

	go func() {
		consumer, err := kafka.NewConsumer(cfg.KafkaBroker)
		if err != nil {
			log.Printf("Kafka consumer error (UI stays up; ensure broker at %s is reachable): %v", cfg.KafkaBroker, err)
			return
		}
		consumer.Run(ctx, func(topic string, value []byte) {
			ingest.HandleRecord(dec, reg, h, topic, value)
		})
	}()

	// Serve the UI immediately so the page loads even before (or without) Kafka.
	log.Printf("Order book UI:    http://localhost:%s", cfg.Port)
	log.Printf("Kafka broker:     %s", cfg.KafkaBroker)
	log.Printf("Schema registry:  %s", cfg.SchemaRegistryURL)
	if err := http.ListenAndServe(":"+cfg.Port, mux); err != nil {
		log.Fatal(err)
	}
}
