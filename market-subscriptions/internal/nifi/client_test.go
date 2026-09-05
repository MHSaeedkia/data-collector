package nifi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"market-subscriptions/internal/domain"
)

func opts(base string) Options {
	return Options{BaseURL: base, SubscribePath: "/subscribe", UnsubPath: "/unsubscribe",
		Timeout: 2 * time.Second, MaxRetries: 3, RetryDelay: time.Millisecond}
}

func TestSend_PostsTheDocumentedPayload(t *testing.T) {
	var gotPath, gotType string
	var gotBody map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotType = r.URL.Path, r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	require.NoError(t, New(opts(srv.URL)).Send(context.Background(),
		domain.ActionSubscribe, "bybit", "BTCUSDT"))

	assert.Equal(t, "/subscribe", gotPath)
	assert.Equal(t, "application/json", gotType)
	// The exact shape market-sync.sh has always sent.
	assert.Equal(t, map[string]string{"exchange": "bybit", "market": "BTCUSDT"}, gotBody)
}

func TestSend_UnsubscribeUsesTheOtherPath(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
	}))
	defer srv.Close()

	require.NoError(t, New(opts(srv.URL)).Send(context.Background(),
		domain.ActionUnsubscribe, "okx", "ETH-USDT"))
	assert.Equal(t, "/unsubscribe", gotPath)
}

func TestSend_RetriesThenSucceeds(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&n, 1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	require.NoError(t, New(opts(srv.URL)).Send(context.Background(),
		domain.ActionSubscribe, "bybit", "BTCUSDT"))
	assert.EqualValues(t, 3, atomic.LoadInt32(&n))
}

func TestSend_GivesUpAfterMaxRetries(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&n, 1)
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer srv.Close()

	err := New(opts(srv.URL)).Send(context.Background(), domain.ActionSubscribe, "bybit", "BTCUSDT")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP 500")
	assert.Contains(t, err.Error(), "3 attempt(s)")
	assert.EqualValues(t, 3, atomic.LoadInt32(&n))
}

func TestURL_TrimsSlashesConsistently(t *testing.T) {
	c := New(Options{BaseURL: "http://nifi:8081/control-plane/", SubscribePath: "subscribe",
		UnsubPath: "/unsubscribe/", MaxRetries: 1})
	assert.Equal(t, "http://nifi:8081/control-plane/subscribe", c.URL(domain.ActionSubscribe))
	assert.Equal(t, "http://nifi:8081/control-plane/unsubscribe", c.URL(domain.ActionUnsubscribe))
}
