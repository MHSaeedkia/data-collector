package main

import (
	"context"
	"embed"
	"io/fs"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"orderbook-web/internal/hub"
	"orderbook-web/internal/ingest"
	"orderbook-web/internal/kafka"
	"orderbook-web/internal/postgres"
	"orderbook-web/internal/registry"
)

// The UI is baked into the binary so it ships as a single self-contained executable.
//
//go:embed public
var staticFiles embed.FS

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	ctx := context.Background()

	port := env("PORT", "3000")
	broker := env("KAFKA_BROKER", "localhost:9092")
	dbURL := env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/markets")

	pool, err := pgxpool.New(ctx, dbURL)
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

	publicFS, err := fs.Sub(staticFiles, "public")
	if err != nil {
		log.Fatalf("embedded ui: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(publicFS)))
	mux.HandleFunc("/ws", h.ServeWS)

	go func() {
		consumer, err := kafka.NewConsumer(broker)
		if err != nil {
			log.Printf("Kafka consumer error (UI stays up; ensure broker at %s is reachable): %v", broker, err)
			return
		}
		consumer.Run(ctx, func(topic string, value []byte) {
			ingest.HandleRecord(reg, h, topic, value)
		})
	}()

	// Serve the UI immediately so the page loads even before (or without) Kafka.
	log.Printf("Order book UI:    http://localhost:%s", port)
	log.Printf("Kafka broker:     %s", broker)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}
