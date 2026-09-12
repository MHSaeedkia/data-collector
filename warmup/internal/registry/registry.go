// Package registry registers the Avro schema files with a Confluent schema
// registry.
package registry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// RegisterDir registers every *.avsc in dir. The subject is the file name with
// underscores turned into dashes: aggregated_order_book_event.avsc ->
// aggregated-order-book-event.
//
// Registering the whole directory rather than a hand-written list is
// deliberate: the list in the old shell script left control-command
// unregistered for two days, and a missing subject there is invisible to a
// green e2e run (the harness registers the directory) — it only shows up as a
// Flink job dying at its first emit.
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
		subject := strings.ReplaceAll(strings.TrimSuffix(filepath.Base(file), ".avsc"), "_", "-")
		id, err := register(client, registryURL, subject, file)
		if err != nil {
			return err
		}
		slog.Info("registered schema", "subject", subject, "id", id)
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
