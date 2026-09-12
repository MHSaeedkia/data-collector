// Package config loads warmup's settings: a .env file (if present) into the
// process environment, then process environment variables, falling back to
// defaults for anything still unset.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// Retentions are the retention.ms values, one per topic family. Every value is
// a canonical millisecond string, which is also how the broker reports it, so
// the two can be compared directly.
type Retentions struct {
	Raw      string
	Input    string
	Output   string
	Rejected string
	Control  string
}

// Config holds every setting main() needs.
type Config struct {
	Bootstrap   string
	RegistryURL string
	DatabaseURL string
	SchemaDir   string
	Partitions  int32
	Replication int16
	Retention   Retentions
	BatchSize   int
}

const (
	defaultBootstrap   = "localhost:9092"
	defaultRegistryURL = "http://localhost:8082"
	defaultDatabaseURL = "postgres://postgres:postgres@localhost:5432/markets?sslmode=disable"
	// Relative to this project directory, which is where the tool runs from.
	defaultSchemaDir   = "../schemas"
	defaultPartitions  = "1"
	defaultReplication = "1"
	defaultBatchSize   = "500"
	// One hour for every topic family, as .env.example documents.
	defaultRetentionMS = "3600000"
)

// Load reads envFile into the process environment — a missing file is not an
// error, it just means defaults and real env vars are used — then builds a
// Config. Real environment variables set before Load runs always take priority
// over envFile, so a one-off override needs no edit:
// RETENTION_INPUT_MS=600000 make warmup.
func Load(envFile string) (Config, error) {
	_ = godotenv.Load(envFile)
	return FromEnv()
}

// FromEnv builds a Config purely from the current process environment, without
// touching any file. Kept separate from Load so the fallback/override logic can
// be unit-tested with t.Setenv alone.
func FromEnv() (Config, error) {
	cfg := Config{
		Bootstrap:   env("KAFKA_BOOTSTRAP", defaultBootstrap),
		RegistryURL: env("SCHEMA_REGISTRY_URL", defaultRegistryURL),
		DatabaseURL: env("DATABASE_URL", defaultDatabaseURL),
		SchemaDir:   env("SCHEMA_DIR", defaultSchemaDir),
	}

	partitions, err := number("TOPIC_PARTITIONS", defaultPartitions)
	if err != nil {
		return Config{}, err
	}
	replication, err := number("TOPIC_REPLICATION", defaultReplication)
	if err != nil {
		return Config{}, err
	}
	batch, err := number("TOPIC_BATCH_SIZE", defaultBatchSize)
	if err != nil {
		return Config{}, err
	}
	if batch < 1 {
		return Config{}, fmt.Errorf("TOPIC_BATCH_SIZE must be at least 1, got %d", batch)
	}
	cfg.Partitions, cfg.Replication, cfg.BatchSize = int32(partitions), int16(replication), int(batch)

	for _, r := range []struct {
		key   string
		field *string
	}{
		{"RETENTION_RAW_MS", &cfg.Retention.Raw},
		{"RETENTION_INPUT_MS", &cfg.Retention.Input},
		{"RETENTION_OUTPUT_MS", &cfg.Retention.Output},
		{"RETENTION_REJECTED_MS", &cfg.Retention.Rejected},
		{"RETENTION_CONTROL_MS", &cfg.Retention.Control},
	} {
		ms, err := number(r.key, defaultRetentionMS)
		if err != nil {
			return Config{}, err
		}
		*r.field = strconv.FormatInt(ms, 10)
	}

	return cfg, nil
}

func env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// number parses key as an integer. Parsing here rather than at use means a typo
// in .env fails before a single topic is created or altered.
func number(key, def string) (int64, error) {
	raw := env(key, def)
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer number of milliseconds, got %q", key, raw)
	}
	return n, nil
}
