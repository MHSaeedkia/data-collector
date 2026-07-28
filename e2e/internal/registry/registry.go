// Package registry registers Avro schemas with the Confluent Schema Registry.
// Adapter for ports.SchemaRegistrar.
package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"orderbook-e2e/internal/domain"
)

const contentType = "application/vnd.schemaregistry.v1+json"

// Client talks to one Schema Registry.
type Client struct {
	baseURL string
	http    *http.Client
}

// New returns a client for a registry base URL, e.g. http://localhost:32891.
func New(baseURL string) *Client {
	return &Client{baseURL: baseURL, http: &http.Client{Timeout: 30 * time.Second}}
}

// Register posts a schema under its subject. The registry is idempotent:
// re-registering an identical schema returns the existing id.
func (c *Client) Register(ctx context.Context, s domain.Subject) error {
	body, err := json.Marshal(struct {
		SchemaType string `json:"schemaType"`
		Schema     string `json:"schema"`
	}{"AVRO", s.Schema})
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}

	endpoint := fmt.Sprintf("%s/subjects/%s/versions", c.baseURL, url.PathEscape(s.Name))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentType)

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("registry returned %d: %s", resp.StatusCode, bytes.TrimSpace(respBody))
	}
	return nil
}
