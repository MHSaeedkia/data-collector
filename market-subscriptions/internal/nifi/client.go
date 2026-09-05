// Package nifi talks to the two control-plane endpoints NiFi exposes: one to subscribe a
// market, one to unsubscribe it. Both take {"exchange": ..., "market": ...} and are
// judged purely on a 2xx, exactly as csv-bulk-sync/market-sync.sh has always done.
package nifi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"market-subscriptions/internal/domain"
)

// Client posts subscribe/unsubscribe requests, retrying transient failures.
type Client struct {
	baseURL       string
	subscribePath string
	unsubPath     string
	maxRetries    int
	retryDelay    time.Duration
	http          *http.Client
}

// Options mirrors the NIFI_* settings in .env — nothing here has a hardcoded fallback,
// config owns the defaults.
type Options struct {
	BaseURL       string
	SubscribePath string
	UnsubPath     string
	Timeout       time.Duration
	MaxRetries    int
	RetryDelay    time.Duration
}

func New(o Options) *Client {
	if o.MaxRetries < 1 {
		o.MaxRetries = 1
	}
	return &Client{
		baseURL:       strings.TrimRight(o.BaseURL, "/"),
		subscribePath: "/" + strings.Trim(o.SubscribePath, "/"),
		unsubPath:     "/" + strings.Trim(o.UnsubPath, "/"),
		maxRetries:    o.MaxRetries,
		retryDelay:    o.RetryDelay,
		http:          &http.Client{Timeout: o.Timeout},
	}
}

// URL is the endpoint an action maps to. Exported so startup can log exactly where
// requests will go — a wrong base URL is otherwise only visible as a failed action.
func (c *Client) URL(action domain.Action) string {
	if action == domain.ActionSubscribe {
		return c.baseURL + c.subscribePath
	}
	return c.baseURL + c.unsubPath
}

type payload struct {
	Exchange string `json:"exchange"`
	Market   string `json:"market"`
}

// Send posts one action and returns nil only on a 2xx. Retries are for the whole request
// including connect errors; a non-2xx is retried too, since market-sync.sh treated a 503
// as retryable and this service inherits that behaviour.
func (c *Client) Send(ctx context.Context, action domain.Action, exchange, market string) error {
	body, err := json.Marshal(payload{Exchange: exchange, Market: market})
	if err != nil {
		return fmt.Errorf("encode payload: %w", err)
	}
	url := c.URL(action)

	var lastErr error
	for attempt := 1; attempt <= c.maxRetries; attempt++ {
		if attempt > 1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.retryDelay):
			}
		}
		lastErr = c.post(ctx, url, body)
		if lastErr == nil {
			return nil
		}
	}
	return fmt.Errorf("%s %s/%s failed after %d attempt(s): %w",
		action, exchange, market, c.maxRetries, lastErr)
}

func (c *Client) post(ctx context.Context, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// Drained so the connection can be reused, and capped so a NiFi error page cannot
	// end up as a multi-megabyte error string in the UI.
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	return nil
}
