// Package config loads runtime configuration: a .env file (if present) into the process
// environment, then process environment variables, falling back to defaults for anything
// still unset. Nothing in this service is configured any other way.
package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Config holds every setting main() needs to wire the service up.
type Config struct {
	Port              string
	DatabaseURL       string
	NiFiBaseURL       string
	NiFiSubscribePath string
	NiFiUnsubPath     string
	NiFiTimeout       time.Duration
	NiFiMaxRetries    int
	NiFiRetryDelay    time.Duration
	UIRefreshSeconds  int
}

const (
	defaultPort              = "8090"
	defaultDatabaseURL       = "postgres://postgres:postgres@localhost:5432/markets"
	defaultNiFiBaseURL       = "http://localhost:8081/control-plane"
	defaultNiFiSubscribePath = "/subscribe"
	defaultNiFiUnsubPath     = "/unsubscribe"
	defaultNiFiTimeout       = 10 * time.Second
	defaultNiFiMaxRetries    = 3
	defaultNiFiRetryDelay    = 2 * time.Second
	defaultUIRefreshSeconds  = 10
)

// Load reads envFile into the process environment — a missing file is not an error, it
// just means real env vars and defaults are used — then builds a Config. Real
// environment variables set before Load runs always win over envFile.
func Load(envFile string) Config {
	_ = godotenv.Load(envFile)
	return FromEnv()
}

// FromEnv builds a Config purely from the current process environment, without touching
// any file. Kept separate from Load so the fallback logic is unit-testable with t.Setenv.
func FromEnv() Config {
	return Config{
		Port:              env("PORT", defaultPort),
		DatabaseURL:       env("DATABASE_URL", defaultDatabaseURL),
		NiFiBaseURL:       env("NIFI_BASE_URL", defaultNiFiBaseURL),
		NiFiSubscribePath: env("NIFI_SUBSCRIBE_PATH", defaultNiFiSubscribePath),
		NiFiUnsubPath:     env("NIFI_UNSUBSCRIBE_PATH", defaultNiFiUnsubPath),
		NiFiTimeout:       duration("NIFI_TIMEOUT", defaultNiFiTimeout),
		NiFiMaxRetries:    number("NIFI_MAX_RETRIES", defaultNiFiMaxRetries),
		NiFiRetryDelay:    duration("NIFI_RETRY_DELAY", defaultNiFiRetryDelay),
		UIRefreshSeconds:  number("UI_REFRESH_SECONDS", defaultUIRefreshSeconds),
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// An unparseable or negative value falls back to the default rather than failing
// startup: this is an operator console, and refusing to boot over a typo in .env would
// take away the tool you use to see what is going on.
func number(key string, fallback int) int {
	v, err := strconv.Atoi(env(key, ""))
	if err != nil || v < 0 {
		return fallback
	}
	return v
}

func duration(key string, fallback time.Duration) time.Duration {
	v, err := time.ParseDuration(env(key, ""))
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}
