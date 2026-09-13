package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"orderbook-web/internal/domain"
)

// clearEnv unsets the three config vars for the duration of the test and
// restores whatever was there before, so tests don't leak state into each
// other or depend on the shell they happen to run in.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"PORT", "KAFKA_BROKER", "DATABASE_URL", "SCHEMA_REGISTRY_URL", "LEVEL_LIMIT"} {
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
	assert.Equal(t, defaultLevelLimit, cfg.LevelLimit)
}

func TestFromEnv_UsesEnvironmentWhenSet(t *testing.T) {
	clearEnv(t)
	t.Setenv("PORT", "8080")
	t.Setenv("KAFKA_BROKER", "broker:9092")
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("SCHEMA_REGISTRY_URL", "http://registry:8082")
	t.Setenv("LEVEL_LIMIT", "100")

	cfg := FromEnv()

	assert.Equal(t, "8080", cfg.Port)
	assert.Equal(t, "broker:9092", cfg.KafkaBroker)
	assert.Equal(t, "postgres://x", cfg.DatabaseURL)
	assert.Equal(t, "http://registry:8082", cfg.SchemaRegistryURL)
	assert.Equal(t, 100, cfg.LevelLimit)
}

// A depth the dropdown does not offer would leave the UI open on a value
// it cannot select back, so it is replaced rather than honoured.
func TestFromEnv_RejectsALevelLimitTheUIDoesNotOffer(t *testing.T) {
	for _, raw := range []string{"75", "0", "-25", "all", "25.0"} {
		t.Run(raw, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("LEVEL_LIMIT", raw)

			assert.Equal(t, defaultLevelLimit, FromEnv().LevelLimit)
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
	require.NoError(t, os.WriteFile(path, []byte("PORT=9000\nKAFKA_BROKER=kafka:29092\nDATABASE_URL=postgres://file\nSCHEMA_REGISTRY_URL=http://file:8082\nLEVEL_LIMIT=200\n"), 0o600))

	cfg := Load(path)

	assert.Equal(t, "9000", cfg.Port)
	assert.Equal(t, "kafka:29092", cfg.KafkaBroker)
	assert.Equal(t, "postgres://file", cfg.DatabaseURL)
	assert.Equal(t, "http://file:8082", cfg.SchemaRegistryURL)
	assert.Equal(t, 200, cfg.LevelLimit)
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
