// Package config loads runtime configuration for the app: a .env file (if
// present) into the process environment, then process environment
// variables, falling back to defaults for anything still unset.
package config

import (
	"os"

	"github.com/joho/godotenv"
)

// Config holds every setting main() needs to wire the app up.
type Config struct {
	Port              string
	KafkaBroker       string
	DatabaseURL       string
	SchemaRegistryURL string
}

const (
	defaultPort              = "3000"
	defaultKafkaBroker       = "localhost:9092"
	defaultDatabaseURL       = "postgres://postgres:postgres@localhost:5432/markets"
	defaultSchemaRegistryURL = "http://localhost:8082"
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
		Port:              env("PORT", defaultPort),
		KafkaBroker:       env("KAFKA_BROKER", defaultKafkaBroker),
		DatabaseURL:       env("DATABASE_URL", defaultDatabaseURL),
		SchemaRegistryURL: env("SCHEMA_REGISTRY_URL", defaultSchemaRegistryURL),
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
