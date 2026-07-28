// Package schemaregistry registers Avro schema files with a Confluent schema registry.
package schemaregistry

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

// RegisterDir registers every *.avsc file in dir with the schema registry at
// registryURL. The subject is the file name with underscores turned into dashes:
// aggregated_order_book_event.avsc -> aggregated-order-book-event.
func RegisterDir(registryURL, dir string) error {
	files, err := filepath.Glob(filepath.Join(dir, "*.avsc"))
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no .avsc files found in %s", dir)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	for _, file := range files {
		base := strings.TrimSuffix(filepath.Base(file), ".avsc")
		subject := strings.ReplaceAll(base, "_", "-")

		id, err := register(client, registryURL, subject, file)
		if err != nil {
			return err
		}
		log.Printf("registered %s (id: %d)", subject, id)
	}
	return nil
}

func register(client *http.Client, registryURL, subject, file string) (int, error) {
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
