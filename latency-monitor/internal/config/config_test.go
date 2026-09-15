package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unset removes a variable for the duration of the test. t.Setenv first, so the
// original value is restored on cleanup; then actually unset it, because
// godotenv skips any key already PRESENT in the environment — an empty-string
// value still counts as present, and is what makes .env look ignored.
func unset(t *testing.T, keys ...string) {
	t.Helper()
	for _, k := range keys {
		t.Setenv(k, "")
		require.NoError(t, os.Unsetenv(k))
	}
}

func TestFromEnvFallsBackToTheHostEndpoints(t *testing.T) {
	unset(t, "KAFKA_BOOTSTRAP", "SCHEMA_REGISTRY_URL")

	cfg := FromEnv()

	assert.Equal(t, "localhost:9092", cfg.Bootstrap)
	assert.Equal(t, "http://localhost:8082", cfg.RegistryURL)
}

func TestFromEnvReadsTheEnvironment(t *testing.T) {
	t.Setenv("KAFKA_BOOTSTRAP", "kafka:29092")
	t.Setenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8082")

	cfg := FromEnv()

	assert.Equal(t, "kafka:29092", cfg.Bootstrap)
	assert.Equal(t, "http://schema-registry:8082", cfg.RegistryURL)
}

func writeEnv(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), ".env")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

func TestLoadReadsTheEnvFile(t *testing.T) {
	unset(t, "KAFKA_BOOTSTRAP", "SCHEMA_REGISTRY_URL")
	path := writeEnv(t, "KAFKA_BOOTSTRAP=broker-from-file:9092\nSCHEMA_REGISTRY_URL=http://file:8082\n")

	cfg := Load(path)

	assert.Equal(t, "broker-from-file:9092", cfg.Bootstrap)
	assert.Equal(t, "http://file:8082", cfg.RegistryURL)
}

// A real environment variable must beat the file, which is what makes a one-off
// override possible without editing anything.
func TestARealEnvVarBeatsTheFile(t *testing.T) {
	t.Setenv("KAFKA_BOOTSTRAP", "broker-from-env:9092")
	path := writeEnv(t, "KAFKA_BOOTSTRAP=broker-from-file:9092\n")

	cfg := Load(path)

	assert.Equal(t, "broker-from-env:9092", cfg.Bootstrap)
}

// The file is optional: with none present the tool still runs against the host
// defaults, which is the common case on a dev box.
func TestLoadWithNoFileIsNotAnError(t *testing.T) {
	unset(t, "KAFKA_BOOTSTRAP", "SCHEMA_REGISTRY_URL")

	cfg := Load(filepath.Join(t.TempDir(), "does-not-exist"))

	assert.Equal(t, "localhost:9092", cfg.Bootstrap)
}

// A key the file does not mention falls through to the default rather than
// becoming empty — a half-filled .env must not break the other endpoint.
func TestAPartialFileLeavesTheRestOnDefaults(t *testing.T) {
	unset(t, "KAFKA_BOOTSTRAP", "SCHEMA_REGISTRY_URL")
	path := writeEnv(t, "KAFKA_BOOTSTRAP=only-this:9092\n")

	cfg := Load(path)

	assert.Equal(t, "only-this:9092", cfg.Bootstrap)
	assert.Equal(t, "http://localhost:8082", cfg.RegistryURL)
}
