// Package config loads latency-monitor's endpoints: a .env file (if present)
// into the process environment, then process environment variables, falling
// back to defaults for anything still unset.
//
// Same shape and the same variable names as warmup/internal/config, so one .env
// can be copied between the two and a reader does not have to learn a second
// spelling of "where is Kafka".
package config

import (
	"os"

	"github.com/joho/godotenv"
)

// Config holds every setting main() needs.
type Config struct {
	Bootstrap   string
	RegistryURL string
}

const (
	// Reached from the HOST: docker-compose.yml and docker-compose.prod.yml
	// both publish 9092 and 8082. From inside the docker network it would be
	// kafka:29092 and http://schema-registry:8082.
	defaultBootstrap   = "localhost:9092"
	defaultRegistryURL = "http://localhost:8082"
)

// Load reads envFile into the process environment — a missing file is not an
// error, it just means defaults and real env vars are used — then builds a
// Config. Real environment variables set before Load runs always take priority
// over envFile, so a one-off override needs no edit:
//
//	KAFKA_BOOTSTRAP=kafka:29092 go run . p1-asks
func Load(envFile string) Config {
	_ = godotenv.Load(envFile)
	return FromEnv()
}

// FromEnv builds a Config purely from the current process environment, without
// touching any file. Kept separate from Load so the fallback logic can be
// unit-tested with t.Setenv alone.
func FromEnv() Config {
	return Config{
		Bootstrap:   env("KAFKA_BOOTSTRAP", defaultBootstrap),
		RegistryURL: env("SCHEMA_REGISTRY_URL", defaultRegistryURL),
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
