package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// clearEnv unsets the config vars for the duration of the test and restores
// whatever was there before, so tests don't leak state into each other or
// depend on the shell they happen to run in.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"KAFKA_BROKER", "SCHEMA_REGISTRY_URL", "FLINK_API", "SETTLE"} {
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

	assert.Equal(t, defaultKafkaBroker, cfg.KafkaBroker)
	assert.Equal(t, defaultSchemaRegistryURL, cfg.SchemaRegistryURL)
	assert.Equal(t, defaultFlinkAPI, cfg.FlinkAPI)
	assert.Equal(t, defaultSettleSeconds*time.Second, cfg.Settle)
}

func TestFromEnv_UsesEnvironmentWhenSet(t *testing.T) {
	clearEnv(t)
	t.Setenv("KAFKA_BROKER", "broker:9092")
	t.Setenv("SCHEMA_REGISTRY_URL", "http://registry:8082")
	t.Setenv("FLINK_API", "http://jobmanager:8081")
	t.Setenv("SETTLE", "20")

	cfg := FromEnv()

	assert.Equal(t, "broker:9092", cfg.KafkaBroker)
	assert.Equal(t, "http://registry:8082", cfg.SchemaRegistryURL)
	assert.Equal(t, "http://jobmanager:8081", cfg.FlinkAPI)
	assert.Equal(t, 20*time.Second, cfg.Settle)
}

func TestFromEnv_UnparseableSettleFallsBackToDefault(t *testing.T) {
	clearEnv(t)
	t.Setenv("SETTLE", "8s")

	assert.Equal(t, defaultSettleSeconds*time.Second, FromEnv().Settle)
}

func TestLoad_ReadsValuesFromEnvFile(t *testing.T) {
	clearEnv(t)
	path := filepath.Join(t.TempDir(), ".env")
	require.NoError(t, os.WriteFile(path, []byte("KAFKA_BROKER=kafka:29092\nFLINK_API=http://file:7070\nSETTLE=3\n"), 0o600))

	cfg := Load(path)

	assert.Equal(t, "kafka:29092", cfg.KafkaBroker)
	assert.Equal(t, "http://file:7070", cfg.FlinkAPI)
	assert.Equal(t, 3*time.Second, cfg.Settle)
}

func TestLoad_MissingFileFallsBackToDefaults(t *testing.T) {
	clearEnv(t)

	cfg := Load(filepath.Join(t.TempDir(), "does-not-exist.env"))

	assert.Equal(t, defaultKafkaBroker, cfg.KafkaBroker)
	assert.Equal(t, defaultFlinkAPI, cfg.FlinkAPI)
}

func TestLoad_RealEnvVarTakesPriorityOverFile(t *testing.T) {
	clearEnv(t)
	t.Setenv("FLINK_API", "http://real:7070")
	path := filepath.Join(t.TempDir(), ".env")
	require.NoError(t, os.WriteFile(path, []byte("FLINK_API=http://file:7070\n"), 0o600))

	cfg := Load(path)

	assert.Equal(t, "http://real:7070", cfg.FlinkAPI, "a real env var set before Load must win over the .env file")
}
