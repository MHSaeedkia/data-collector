package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"orderbook-viewer/internal/domain"
)

// clearEnv unsets the three config vars for the duration of the test and
// restores whatever was there before, so tests don't leak state into each
// other or depend on the shell they happen to run in.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"PORT", "KAFKA_BROKER", "DATABASE_URL", "SCHEMA_REGISTRY_URL", "DEFAULT_LEVEL_LIMIT", "DEFAULT_PAIR_ID", "DEFAULT_EXCHANGE_ID"} {
		prev, had := os.LookupEnv(key)
		require.NoError(t, os.Unsetenv(key))
		t.Cleanup(func() {
			if had {
				os.Setenv(key, prev)
			} else {
				os.Unsetenv(key)
			}
		})
	}
}

func TestFromEnv_FallsBackToDefaultsWhenUnset(t *testing.T) {
	clearEnv(t)

	cfg := FromEnv()

	assert.Equal(t, defaultPort, cfg.Port)
	assert.Equal(t, defaultKafkaBroker, cfg.KafkaBroker)
	assert.Equal(t, defaultDatabaseURL, cfg.DatabaseURL)
	assert.Equal(t, defaultSchemaRegistryURL, cfg.SchemaRegistryURL)
	assert.Equal(t, defaultLevelLimit, cfg.Defaults.LevelLimit)
	assert.Zero(t, cfg.Defaults.PairID, "unset means: let the page take the first market")
	assert.Equal(t, domain.AggregatedExchangeID, cfg.Defaults.ExchangeID)
}

func TestFromEnv_UsesEnvironmentWhenSet(t *testing.T) {
	clearEnv(t)
	t.Setenv("PORT", "8080")
	t.Setenv("KAFKA_BROKER", "broker:9092")
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("SCHEMA_REGISTRY_URL", "http://registry:8082")
	t.Setenv("DEFAULT_LEVEL_LIMIT", "100")
	t.Setenv("DEFAULT_PAIR_ID", "  2  ") // padding is a .env typo, not a value
	t.Setenv("DEFAULT_EXCHANGE_ID", "8")

	cfg := FromEnv()

	assert.Equal(t, "8080", cfg.Port)
	assert.Equal(t, "broker:9092", cfg.KafkaBroker)
	assert.Equal(t, "postgres://x", cfg.DatabaseURL)
	assert.Equal(t, "http://registry:8082", cfg.SchemaRegistryURL)
	assert.Equal(t, 100, cfg.Defaults.LevelLimit)
	assert.Equal(t, 2, cfg.Defaults.PairID)
	assert.Equal(t, 8, cfg.Defaults.ExchangeID)
}

// Whether an id EXISTS is postgres's answer, not this package's: ids that
// no row carries are passed on and checked by the registry.
func TestFromEnv_PassesUnknownIDsThroughUnjudged(t *testing.T) {
	clearEnv(t)
	t.Setenv("DEFAULT_PAIR_ID", "999")
	t.Setenv("DEFAULT_EXCHANGE_ID", "-1") // the merged view

	cfg := FromEnv()

	assert.Equal(t, 999, cfg.Defaults.PairID)
	assert.Equal(t, -1, cfg.Defaults.ExchangeID)
}

// A value that is not a number is a typo, and silently opening on
// something else is what the log line exists to prevent.
func TestFromEnv_NonNumericIDsFallBack(t *testing.T) {
	clearEnv(t)
	t.Setenv("DEFAULT_PAIR_ID", "BTC/USDT")
	t.Setenv("DEFAULT_EXCHANGE_ID", "okx")

	cfg := FromEnv()

	assert.Zero(t, cfg.Defaults.PairID)
	assert.Equal(t, domain.AggregatedExchangeID, cfg.Defaults.ExchangeID)
}

// A depth the dropdown does not offer would leave the UI open on a value
// it cannot select back, so it is replaced rather than honoured.
func TestFromEnv_RejectsALevelLimitTheUIDoesNotOffer(t *testing.T) {
	for _, raw := range []string{"75", "0", "-25", "all", "25.0"} {
		t.Run(raw, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("DEFAULT_LEVEL_LIMIT", raw)

			assert.Equal(t, defaultLevelLimit, FromEnv().Defaults.LevelLimit)
		})
	}
}

// The default must itself be selectable in the dropdown it fills.
func TestDefaultLevelLimit_IsOneOfTheOfferedDepths(t *testing.T) {
	assert.True(t, domain.ValidLevelLimit(defaultLevelLimit))
}

func TestLoad_ReadsValuesFromEnvFile(t *testing.T) {
	clearEnv(t)
	path := filepath.Join(t.TempDir(), ".env")
	require.NoError(t, os.WriteFile(path, []byte("PORT=9000\nKAFKA_BROKER=kafka:29092\nDATABASE_URL=postgres://file\nSCHEMA_REGISTRY_URL=http://file:8082\nDEFAULT_LEVEL_LIMIT=200\nDEFAULT_PAIR_ID=3\n"), 0o600))

	cfg := Load(path)

	assert.Equal(t, "9000", cfg.Port)
	assert.Equal(t, "kafka:29092", cfg.KafkaBroker)
	assert.Equal(t, "postgres://file", cfg.DatabaseURL)
	assert.Equal(t, "http://file:8082", cfg.SchemaRegistryURL)
	assert.Equal(t, 200, cfg.Defaults.LevelLimit)
	assert.Equal(t, 3, cfg.Defaults.PairID)
}

func TestLoad_MissingFileFallsBackToDefaults(t *testing.T) {
	clearEnv(t)

	cfg := Load(filepath.Join(t.TempDir(), "does-not-exist.env"))

	assert.Equal(t, defaultPort, cfg.Port)
	assert.Equal(t, defaultKafkaBroker, cfg.KafkaBroker)
	assert.Equal(t, defaultDatabaseURL, cfg.DatabaseURL)
	assert.Equal(t, defaultSchemaRegistryURL, cfg.SchemaRegistryURL)
}

func TestLoad_RealEnvVarTakesPriorityOverFile(t *testing.T) {
	clearEnv(t)
	t.Setenv("PORT", "5555")
	path := filepath.Join(t.TempDir(), ".env")
	require.NoError(t, os.WriteFile(path, []byte("PORT=9000\n"), 0o600))

	cfg := Load(path)

	assert.Equal(t, "5555", cfg.Port, "a real env var set before Load must win over the .env file")
}
