// Package config loads the harness settings from a .env file and the environment.
package config

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Config holds everything the harness needs from the environment.
type Config struct {
	SchemaRegistryURL string
	SchemasDir        string
	KafkaBroker       string
	FlinkAPI          string
	NormalizerDir     string
	ComposeFile       string
}

// Load reads envFile (if present) into the process environment, then builds the
// config. Real environment variables always win over the file.
func Load(envFile string) (Config, error) {
	if err := loadEnvFile(envFile); err != nil {
		return Config{}, err
	}

	cfg := Config{
		SchemaRegistryURL: env("SCHEMA_REGISTRY_URL", "http://localhost:8082"),
		SchemasDir:        env("SCHEMAS_DIR", "../schemas"),
		KafkaBroker:       env("KAFKA_BROKER", "localhost:9092"),
		FlinkAPI:          env("FLINK_API", "http://localhost:7070"),
		NormalizerDir:     env("NORMALIZER_DIR", "../flink/normalizer"),
		ComposeFile:       env("COMPOSE_FILE", "../docker-compose.yml"),
	}

	if cfg.SchemaRegistryURL == "" {
		return Config{}, fmt.Errorf("SCHEMA_REGISTRY_URL is empty")
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// loadEnvFile parses a KEY=VALUE file. A missing file is not an error.
func loadEnvFile(path string) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if _, set := os.LookupEnv(key); !set {
			os.Setenv(key, value)
		}
	}
	return scanner.Err()
}
