package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var keys = []string{"PORT", "DATABASE_URL", "NIFI_BASE_URL", "NIFI_SUBSCRIBE_PATH",
	"NIFI_UNSUBSCRIBE_PATH", "NIFI_TIMEOUT", "NIFI_MAX_RETRIES", "NIFI_RETRY_DELAY",
	"UI_REFRESH_SECONDS"}

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range keys {
		prev, had := os.LookupEnv(k)
		require.NoError(t, os.Unsetenv(k))
		t.Cleanup(func() {
			if had {
				os.Setenv(k, prev)
			} else {
				os.Unsetenv(k)
			}
		})
	}
}

func TestFromEnv_FallsBackToDefaults(t *testing.T) {
	clearEnv(t)
	cfg := FromEnv()
	assert.Equal(t, defaultPort, cfg.Port)
	assert.Equal(t, defaultNiFiBaseURL, cfg.NiFiBaseURL)
	assert.Equal(t, defaultNiFiTimeout, cfg.NiFiTimeout)
	assert.Equal(t, defaultNiFiMaxRetries, cfg.NiFiMaxRetries)
	assert.Equal(t, defaultUIRefreshSeconds, cfg.UIRefreshSeconds)
}

func TestFromEnv_UsesEnvironment(t *testing.T) {
	clearEnv(t)
	t.Setenv("PORT", "9999")
	t.Setenv("NIFI_TIMEOUT", "45s")
	t.Setenv("NIFI_MAX_RETRIES", "7")
	t.Setenv("UI_REFRESH_SECONDS", "0")

	cfg := FromEnv()
	assert.Equal(t, "9999", cfg.Port)
	assert.Equal(t, 45*time.Second, cfg.NiFiTimeout)
	assert.Equal(t, 7, cfg.NiFiMaxRetries)
	assert.Equal(t, 0, cfg.UIRefreshSeconds, "0 must be honoured, not treated as unset")
}

func TestFromEnv_GarbageFallsBackInsteadOfCrashing(t *testing.T) {
	clearEnv(t)
	t.Setenv("NIFI_TIMEOUT", "ten seconds")
	t.Setenv("NIFI_MAX_RETRIES", "-4")

	cfg := FromEnv()
	assert.Equal(t, defaultNiFiTimeout, cfg.NiFiTimeout)
	assert.Equal(t, defaultNiFiMaxRetries, cfg.NiFiMaxRetries)
}

func TestLoad_ReadsEnvFile(t *testing.T) {
	clearEnv(t)
	path := filepath.Join(t.TempDir(), ".env")
	require.NoError(t, os.WriteFile(path, []byte("PORT=7001\nNIFI_BASE_URL=http://file/cp\n"), 0o600))

	cfg := Load(path)
	assert.Equal(t, "7001", cfg.Port)
	assert.Equal(t, "http://file/cp", cfg.NiFiBaseURL)
}
