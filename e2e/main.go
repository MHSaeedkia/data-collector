package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	cfg, err := loadConfig(".env")
	if err != nil {
		log.Fatal(err)
	}

	if err := registerSchemas(cfg); err != nil {
		log.Fatal(err)
	}

	// create required topics

	// send payloads to kafka topics
	// verify each step has wanted value
}

// registerSchemas registers every *.avsc file in cfg.SchemasDir with the schema
// registry. The subject is the file name with underscores turned into dashes:
// aggregated_order_book_event.avsc -> aggregated-order-book-event.
func registerSchemas(cfg Config) error {
	files, err := filepath.Glob(filepath.Join(cfg.SchemasDir, "*.avsc"))
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no .avsc files found in %s", cfg.SchemasDir)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	for _, file := range files {
		base := strings.TrimSuffix(filepath.Base(file), ".avsc")
		subject := strings.ReplaceAll(base, "_", "-")

		id, err := registerSchema(client, cfg.SchemaRegistryURL, subject, file)
		if err != nil {
			return err
		}
		log.Printf("registered %s (id: %d)", subject, id)
	}
	return nil
}

func registerSchema(client *http.Client, registryURL, subject, file string) (int, error) {
	schema, err := os.ReadFile(file)
	if err != nil {
		return 0, err
	}

	payload, err := json.Marshal(map[string]string{
		"schemaType": "AVRO",
		"schema":     string(schema),
	})
	if err != nil {
		return 0, err
	}

	url := fmt.Sprintf("%s/subjects/%s/versions", strings.TrimSuffix(registryURL, "/"), subject)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("register %s: %w", subject, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("register %s: http %d: %s", subject, resp.StatusCode, body)
	}

	var result struct {
		ID int `json:"id"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, fmt.Errorf("register %s: decode response: %w", subject, err)
	}
	return result.ID, nil
}
