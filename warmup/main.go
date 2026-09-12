// Command warmup provisions the pipeline: it registers the Avro schemas and
// makes the Kafka topics for every subscribed market exist with the retention
// configured in .env.
//
// It replaces scripts/warmup.sh, which spawned one `docker exec kafka
// kafka-topics --create` — and with it a whole JVM — per topic. At 9 exchanges
// × 50 markets that is ~3000 topics and about an hour; the admin API takes them
// in batches instead.
//
// Safe to re-run: existing topics are kept, and only their retention is
// rewritten when .env no longer matches the broker.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"orderbook-warmup/internal/config"
	"orderbook-warmup/internal/kafka"
	"orderbook-warmup/internal/postgres"
	"orderbook-warmup/internal/registry"
	"orderbook-warmup/internal/topics"
)

func main() {
	envFile := flag.String("env", ".env", "path to the .env file with the warmup settings")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		slog.Error("invalid log level", "value", *logLevel)
		os.Exit(1)
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	if err := run(context.Background(), *envFile); err != nil {
		slog.Error("warmup failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, envFile string) error {
	started := time.Now()

	cfg, err := config.Load(envFile)
	if err != nil {
		return err
	}
	slog.Info("warmup starting",
		"bootstrap", cfg.Bootstrap, "registry", cfg.RegistryURL, "schema_dir", cfg.SchemaDir)

	if err := registry.RegisterDir(cfg.RegistryURL, cfg.SchemaDir); err != nil {
		return err
	}

	subscriptions, err := postgres.LoadSubscriptions(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	if len(subscriptions) == 0 {
		slog.Warn("no rows in exchange_markets, no topics to create")
		return nil
	}
	slog.Info("loaded subscriptions", "count", len(subscriptions))

	if err := kafka.Reconcile(ctx, cfg, topics.Plan(subscriptions, cfg.Retention)); err != nil {
		return err
	}

	slog.Info("warmup done", "took", time.Since(started).Round(time.Millisecond))
	return nil
}
