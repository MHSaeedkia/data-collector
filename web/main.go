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

const (
	// registryRefresh is how often the postgres id -> name maps reload.
	registryRefresh = 10 * time.Second
	// statsPeriod is how often the hub prints its heartbeat. It matches
	// the consumers' own reporting interval so one glance at the log lines
	// up "records in" with "clients served".
	statsPeriod = 30 * time.Second
)

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

	h := hub.New()
	h.SetCatalog(reg.Catalog()) // dropdowns are ready before the first client connects
	go func() {
		t := time.NewTicker(registryRefresh)
		defer t.Stop()
		for range t.C {
			reg.Refresh(ctx)
			h.SetCatalog(reg.Catalog())
		}
	}()

	// The heartbeat. One line every interval saying how many browsers are
	// attached, how many books are held and at what rate they are arriving
	// — enough to tell a quiet market from a stalled pipeline in the
	// container logs alone, which is what this app could not do when it
	// last went wrong.
	go func() {
		t := time.NewTicker(statsPeriod)
		defer t.Stop()
		for range t.C {
			h.LogStats(statsPeriod)
		}
	}()

	dec := schema.NewDecoder(cfg.SchemaRegistryURL)

	publicFS, err := fs.Sub(staticFiles, "public")
	if err != nil {
		log.Fatalf("embedded ui: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(publicFS)))
	mux.HandleFunc("/ws", h.ServeWS)

	// Two consumers, not one: the aggregated topics replay from the start
	// so the book paints on load, the per-exchange ones start at the end
	// (see internal/kafka), and the reset offset is a per-client setting.
	for _, newConsumer := range []func(string) (*kafka.Consumer, error){
		kafka.NewAggregatedConsumer,
		kafka.NewSnapshotConsumer,
	} {
		go func() {
			consumer, err := newConsumer(cfg.KafkaBroker)
			if err != nil {
				log.Printf("Kafka consumer error (UI stays up; ensure broker at %s is reachable): %v", cfg.KafkaBroker, err)
				return
			}
			consumer.Run(ctx, func(topic string, value []byte) {
				ingest.HandleRecord(dec, reg, h, topic, value)
			})
		}()
	}

	// Serve the UI immediately so the page loads even before (or without) Kafka.
	log.Printf("Order book UI:    http://localhost:%s", cfg.Port)
	log.Printf("Kafka broker:     %s", cfg.KafkaBroker)
	log.Printf("Schema registry:  %s", cfg.SchemaRegistryURL)
	log.Printf("Reading LIVE records only (latest offset) — no history is replayed on start")
	log.Printf("Heartbeat every %s; registry refresh every %s", statsPeriod, registryRefresh)
	if err := http.ListenAndServe(":"+cfg.Port, mux); err != nil {
		log.Fatal(err)
	}
}
