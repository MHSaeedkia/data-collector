package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var keys = []string{
	"KAFKA_BOOTSTRAP", "SCHEMA_REGISTRY_URL", "DATABASE_URL", "SCHEMA_DIR",
	"TOPIC_PARTITIONS", "TOPIC_REPLICATION", "TOPIC_BATCH_SIZE",
	"RETENTION_RAW_MS", "RETENTION_INPUT_MS", "RETENTION_OUTPUT_MS",
	"RETENTION_REJECTED_MS", "RETENTION_CONTROL_MS",
}

// clearEnv unsets every config var for the duration of the test and restores
// whatever was there before, so tests don't leak state into each other or
// depend on the shell they happen to run in. Load writes into the process
// environment, which makes this mandatory here and not merely tidy.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, key := range keys {
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

func writeEnvFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), ".env")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

func TestFromEnv_FallsBackToDefaultsWhenUnset(t *testing.T) {
	clearEnv(t)

	cfg, err := FromEnv()

	require.NoError(t, err)
	assert.Equal(t, defaultBootstrap, cfg.Bootstrap)
	assert.Equal(t, defaultRegistryURL, cfg.RegistryURL)
	assert.Equal(t, defaultSchemaDir, cfg.SchemaDir)
	assert.Equal(t, int32(1), cfg.Partitions)
	assert.Equal(t, int16(1), cfg.Replication)
	// One hour everywhere, the documented default.
	assert.Equal(t, Retentions{"3600000", "3600000", "3600000", "3600000", "3600000"}, cfg.Retention)
}

func TestFromEnv_ReadsEveryRetention(t *testing.T) {
	clearEnv(t)
	t.Setenv("RETENTION_RAW_MS", "1")
	t.Setenv("RETENTION_INPUT_MS", "2")
	t.Setenv("RETENTION_OUTPUT_MS", "3")
	t.Setenv("RETENTION_REJECTED_MS", "4")
	t.Setenv("RETENTION_CONTROL_MS", "5")

	cfg, err := FromEnv()

	require.NoError(t, err)
	assert.Equal(t, Retentions{Raw: "1", Input: "2", Output: "3", Rejected: "4", Control: "5"}, cfg.Retention)
}

// Whitespace would otherwise make every run see a retention change.
func TestFromEnv_CanonicalisesRetention(t *testing.T) {
	clearEnv(t)
	t.Setenv("RETENTION_CONTROL_MS", " 60000 ")

	cfg, err := FromEnv()

	require.NoError(t, err)
	assert.Equal(t, "60000", cfg.Retention.Control)
}

func TestFromEnv_RejectsNonNumericRetention(t *testing.T) {
	clearEnv(t)
	t.Setenv("RETENTION_INPUT_MS", "1h")

	_, err := FromEnv()

	require.Error(t, err, "a non-numeric retention must fail before anything is created")
	assert.Contains(t, err.Error(), "RETENTION_INPUT_MS")
}

func TestLoad_ReadsTheEnvFile(t *testing.T) {
	clearEnv(t)
	path := writeEnvFile(t, "# comment\nRETENTION_INPUT_MS=600000\nKAFKA_BOOTSTRAP=broker:9092\n")

	cfg, err := Load(path)

	require.NoError(t, err)
	assert.Equal(t, "600000", cfg.Retention.Input)
	assert.Equal(t, "broker:9092", cfg.Bootstrap)
}

// A real environment variable beats the file, which is what makes
// `RETENTION_INPUT_MS=1000 make warmup` a one-off override.
func TestLoad_PrefersTheProcessEnvironment(t *testing.T) {
	clearEnv(t)
	t.Setenv("RETENTION_INPUT_MS", "111")

	cfg, err := Load(writeEnvFile(t, "RETENTION_INPUT_MS=222"))

	require.NoError(t, err)
	assert.Equal(t, "111", cfg.Retention.Input)
}

func TestLoad_MissingFileIsNotAnError(t *testing.T) {
	clearEnv(t)

	cfg, err := Load(filepath.Join(t.TempDir(), "absent"))

	require.NoError(t, err)
	assert.Equal(t, defaultBootstrap, cfg.Bootstrap)
}
