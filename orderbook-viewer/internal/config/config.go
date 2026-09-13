// Package config loads runtime configuration for the app: a .env file (if
// present) into the process environment, then process environment
// variables, falling back to defaults for anything still unset.
package config

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"

	"orderbook-viewer/internal/domain"
)

// Config holds every setting main() needs to wire the app up.
type Config struct {
	Port              string
	KafkaBroker       string
	DatabaseURL       string
	SchemaRegistryURL string
	// Defaults are the three dropdown values the page opens on. Pair and
	// Exchange are passed on as written: they name rows in postgres, which
	// this package cannot reach, so the registry is what resolves them (and
	// what reports a name that matches nothing). Only the depth can be
	// checked here, because the depths on offer are a fixed list.
	Defaults domain.Defaults
}

const (
	defaultPort              = "3000"
	defaultKafkaBroker       = "localhost:9092"
	defaultDatabaseURL       = "postgres://postgres:postgres@localhost:5432/markets"
	defaultSchemaRegistryURL = "http://localhost:8082"
	// defaultLevelLimit is the shallowest depth on offer — the cheapest
	// thing to render is the right thing to start on. It must stay one of
	// domain.LevelLimits, or the UI would open on a depth its own dropdown
	// cannot show.
	defaultLevelLimit = 25
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
		Defaults: domain.Defaults{
			Pair:       strings.TrimSpace(os.Getenv("DEFAULT_PAIR")),
			Exchange:   strings.TrimSpace(os.Getenv("DEFAULT_EXCHANGE")),
			LevelLimit: levelLimit(),
		},
	}
}

// levelLimit reads DEFAULT_LEVEL_LIMIT, which must name one of the depths
// the UI offers. A value outside that list is not clamped or honoured
// quietly: it is announced and replaced, because the alternative is a
// server whose starting depth cannot be selected back in the dropdown,
// with nothing in the log to say why.
func levelLimit() int {
	raw := os.Getenv("DEFAULT_LEVEL_LIMIT")
	if raw == "" {
		return defaultLevelLimit
	}
	n, err := strconv.Atoi(raw)
	if err != nil || !domain.ValidLevelLimit(n) {
		log.Printf("config: DEFAULT_LEVEL_LIMIT=%q is not one of %v — using %d", raw, domain.LevelLimits, defaultLevelLimit)
		return defaultLevelLimit
	}
	return n
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
