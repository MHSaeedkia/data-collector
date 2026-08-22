package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"market-subscriptions/internal/domain"
)

type fakeStore struct {
	subs     map[int64]domain.Subscription
	writes   []domain.Status
	failWith error
}

func (f *fakeStore) List(context.Context) ([]domain.Subscription, error) {
	out := []domain.Subscription{}
	for _, s := range f.subs {
		out = append(out, s)
	}
	return out, nil
}
func (f *fakeStore) Exchanges(context.Context) ([]domain.Exchange, error) { return nil, nil }
func (f *fakeStore) Ping(context.Context) error                           { return nil }
func (f *fakeStore) Get(_ context.Context, id int64) (domain.Subscription, error) {
	s, ok := f.subs[id]
	if !ok {
		return domain.Subscription{}, errors.New("not found")
	}
	return s, nil
}
func (f *fakeStore) SetStatus(_ context.Context, id int64, st domain.Status) error {
	if f.failWith != nil {
		return f.failWith
	}
	f.writes = append(f.writes, st)
	s := f.subs[id]
	s.Status = st
	f.subs[id] = s
	return nil
}

type fakeNiFi struct {
	calls []string
	err   error
}

func (f *fakeNiFi) Send(_ context.Context, a domain.Action, exchange, market string) error {
	f.calls = append(f.calls, string(a)+" "+exchange+"/"+market)
	return f.err
}

func newHandler(store Store, n Notifier) *Handler {
	ui, _ := fs.Sub(fstest.MapFS{"index.html": {Data: []byte("ui")}}, ".")
	return New(store, n, ui, 10, slog.New(slog.DiscardHandler))
}

func post(t *testing.T, h *Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/actions", strings.NewReader(body)))
	return rec
}

func results(t *testing.T, rec *httptest.ResponseRecorder) []domain.Result {
	t.Helper()
	var body struct {
		Results []domain.Result `json:"results"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	return body.Results
}

func store() *fakeStore {
	return &fakeStore{subs: map[int64]domain.Subscription{
		1: {ID: 1, ExchangeName: "bybit", Market: "BTCUSDT", Status: domain.Unsubscribed},
		2: {ID: 2, ExchangeName: "okx", Market: "ETH-USDT", Status: domain.Subscribed},
	}}
}

func TestSubscribe_WritesPendingAndCallsNiFi(t *testing.T) {
	s, n := store(), &fakeNiFi{}
	rec := post(t, newHandler(s, n), `{"action":"subscribe","ids":[1]}`)

	require.Equal(t, http.StatusOK, rec.Code)
	res := results(t, rec)
	require.Len(t, res, 1)
	assert.True(t, res[0].OK)
	// The row is left PENDING — this service never writes a settled status.
	assert.Equal(t, domain.PendingSubscribe, res[0].Status)
	assert.Equal(t, []domain.Status{domain.PendingSubscribe}, s.writes)
	assert.Equal(t, []string{"subscribe bybit/BTCUSDT"}, n.calls)
}

func TestUnsubscribe_WritesPendingUnsubscribe(t *testing.T) {
	s, n := store(), &fakeNiFi{}
	rec := post(t, newHandler(s, n), `{"action":"unsubscribe","ids":[2]}`)

	assert.Equal(t, domain.PendingUnsubscribe, results(t, rec)[0].Status)
	assert.Equal(t, []string{"unsubscribe okx/ETH-USDT"}, n.calls)
}

func TestNiFiFailure_RollsTheRowBack(t *testing.T) {
	s, n := store(), &fakeNiFi{err: errors.New("HTTP 503")}
	rec := post(t, newHandler(s, n), `{"action":"subscribe","ids":[1]}`)

	res := results(t, rec)
	assert.False(t, res[0].OK)
	assert.Contains(t, res[0].Error, "503")
	// pending was written, then reverted to the status the row had before.
	assert.Equal(t, []domain.Status{domain.PendingSubscribe, domain.Unsubscribed}, s.writes)
	assert.Equal(t, domain.Unsubscribed, res[0].Status)
	assert.Equal(t, domain.Unsubscribed, s.subs[1].Status)
}

func TestBulk_OneFailureDoesNotStopTheRest(t *testing.T) {
	s := store()
	// Fails only for market 1, so the second market must still be attempted.
	n := &failOnce{fail: "subscribe bybit/BTCUSDT"}
	rec := post(t, newHandler(s, n), `{"action":"subscribe","ids":[1,2]}`)

	res := results(t, rec)
	require.Len(t, res, 2)
	assert.False(t, res[0].OK)
	assert.True(t, res[1].OK)
	assert.Equal(t, domain.PendingSubscribe, s.subs[2].Status)
}

type failOnce struct{ fail string }

func (f *failOnce) Send(_ context.Context, a domain.Action, exchange, market string) error {
	if string(a)+" "+exchange+"/"+market == f.fail {
		return errors.New("boom")
	}
	return nil
}

func TestUnknownAction_IsRejectedBeforeAnyWrite(t *testing.T) {
	s, n := store(), &fakeNiFi{}
	rec := post(t, newHandler(s, n), `{"action":"delete","ids":[1]}`)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Empty(t, s.writes)
	assert.Empty(t, n.calls)
}

func TestEmptySelection_IsRejected(t *testing.T) {
	s, n := store(), &fakeNiFi{}
	rec := post(t, newHandler(s, n), `{"action":"subscribe","ids":[]}`)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Empty(t, n.calls)
}

func TestRollbackFailure_LeavesRowPendingAndSaysSo(t *testing.T) {
	s := &fakeStore{subs: store().subs}
	h := newHandler(&rollbackBreaker{fakeStore: s}, &fakeNiFi{err: errors.New("down")})
	rec := post(t, h, `{"action":"subscribe","ids":[1]}`)

	res := results(t, rec)
	assert.False(t, res[0].OK)
	assert.Equal(t, domain.PendingSubscribe, res[0].Status)
	assert.Contains(t, res[0].Error, "rollback also failed")
}

// rollbackBreaker lets the first SetStatus (the pending write) through and fails the
// second (the rollback).
type rollbackBreaker struct {
	*fakeStore
	n int
}

func (r *rollbackBreaker) SetStatus(ctx context.Context, id int64, st domain.Status) error {
	r.n++
	if r.n > 1 {
		return errors.New("db gone")
	}
	return r.fakeStore.SetStatus(ctx, id, st)
}
