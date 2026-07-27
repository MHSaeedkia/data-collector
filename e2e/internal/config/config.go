// Package config loads runtime configuration for the harness: a .env file
// (if present) into the process environment, then process environment
// variables, falling back to defaults for anything still unset.
//
// The defaults are the compose stack's HOST-published ports — the harness
// talks plain TCP/HTTP to them and never shells into a container.
package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Config holds every setting the harness needs to reach the running stack.
type Config struct {
	KafkaBroker       string
	SchemaRegistryURL string
	FlinkAPI          string
	Settle            time.Duration
}

const (
	defaultKafkaBroker       = "localhost:9092"
	defaultSchemaRegistryURL = "http://localhost:8082"
	defaultFlinkAPI          = "http://localhost:7070"
	defaultSettleSeconds     = 8
)

// Load reads envFile into the process environment — a missing file is not
// an error, it just means defaults/real env vars are used — then builds a
// Config. Real environment variables set before Load runs always take
// priority over envFile.
func Load(envFile string) Config {
	_ = godotenv.Load(envFile)
	return FromEnv()
}

// FromEnv builds a Config purely from the current process environment,
// without touching any file. Kept separate from Load so the fallback/
// override logic can be unit-tested with os.Setenv/t.Setenv alone.
func FromEnv() Config {
	return Config{
		KafkaBroker:       env("KAFKA_BROKER", defaultKafkaBroker),
		SchemaRegistryURL: env("SCHEMA_REGISTRY_URL", defaultSchemaRegistryURL),
		FlinkAPI:          env("FLINK_API", defaultFlinkAPI),
		Settle:            time.Duration(envInt("SETTLE", defaultSettleSeconds)) * time.Second,
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// envInt reads a whole number of seconds, falling back on anything
// unparseable rather than failing the whole run over a typo'd knob.
func envInt(key string, fallback int) int {
	v, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return v
}
