// Package httpapi exposes the JSON API and serves the embedded UI.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"

	"market-subscriptions/internal/domain"
)

// Store is the persistence this handler needs. An interface so the handler can be tested
// without a database.
type Store interface {
	List(ctx context.Context) ([]domain.Subscription, error)
	Exchanges(ctx context.Context) ([]domain.Exchange, error)
	Get(ctx context.Context, id int64) (domain.Subscription, error)
	SetStatus(ctx context.Context, id int64, status domain.Status) error
	Ping(ctx context.Context) error
}

// Notifier is the NiFi side, likewise an interface for testing.
type Notifier interface {
	Send(ctx context.Context, action domain.Action, exchange, market string) error
}

// Handler wires the store, NiFi and the UI together.
type Handler struct {
	store    Store
	nifi     Notifier
	ui       fs.FS
	refreshS int
	log      *slog.Logger
}

func New(store Store, nifi Notifier, ui fs.FS, refreshSeconds int, log *slog.Logger) *Handler {
	return &Handler{store: store, nifi: nifi, ui: ui, refreshS: refreshSeconds, log: log}
}

// Routes returns the mux. Kept in one place so the surface is readable at a glance.
func (h *Handler) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/subscriptions", h.listSubscriptions)
	mux.HandleFunc("GET /api/exchanges", h.listExchanges)
	mux.HandleFunc("POST /api/actions", h.applyAction)
	mux.HandleFunc("GET /api/config", h.uiConfig)
	mux.HandleFunc("GET /healthz", h.health)
	mux.Handle("GET /", http.FileServer(http.FS(h.ui)))
	return mux
}

func (h *Handler) listSubscriptions(w http.ResponseWriter, r *http.Request) {
	subs, err := h.store.List(r.Context())
	if err != nil {
		h.fail(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, subs)
}

func (h *Handler) listExchanges(w http.ResponseWriter, r *http.Request) {
	exchanges, err := h.store.Exchanges(r.Context())
	if err != nil {
		h.fail(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, exchanges)
}

// uiConfig hands the browser the settings it needs, so the page has no baked-in values
// of its own — every knob still lives in .env.
func (h *Handler) uiConfig(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"refresh_seconds": h.refreshS})
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	if err := h.store.Ping(r.Context()); err != nil {
		h.fail(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type actionRequest struct {
	Action string  `json:"action"`
	IDs    []int64 `json:"ids"`
}

// applyAction is the whole write path: one action applied to one or many markets.
//
// Per market, in this order:
//  1. write the PENDING status,
//  2. POST to NiFi,
//  3. leave it pending — NiFi settles the row to subscribe/unsubscribe itself.
//
// If step 2 fails the pending status is rolled back to what the row held before, because
// a pending row that NiFi was never told about would sit there forever looking like work
// in progress. The rollback is best-effort and honest about its limit: a request that
// timed out may still have reached NiFi, so a rolled-back row can in principle be
// settled by NiFi a moment later. That is visible in the UI on the next refresh, which is
// better than silently stranding the row.
//
// One market failing never stops the rest — each gets its own Result.
func (h *Handler) applyAction(w http.ResponseWriter, r *http.Request) {
	var req actionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.fail(w, http.StatusBadRequest, err)
		return
	}
	action, err := domain.ParseAction(req.Action)
	if err != nil {
		h.fail(w, http.StatusBadRequest, err)
		return
	}
	if len(req.IDs) == 0 {
		h.fail(w, http.StatusBadRequest, errors.New("no markets selected"))
		return
	}

	ctx := r.Context()
	results := make([]domain.Result, 0, len(req.IDs))
	for _, id := range req.IDs {
		results = append(results, h.apply(ctx, action, id))
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

func (h *Handler) apply(ctx context.Context, action domain.Action, id int64) domain.Result {
	sub, err := h.store.Get(ctx, id)
	if err != nil {
		return domain.Result{ID: id, OK: false, Error: err.Error()}
	}
	res := domain.Result{ID: id, Market: sub.ExchangeName + "/" + sub.Market}

	pending := action.Pending()
	if err := h.store.SetStatus(ctx, id, pending); err != nil {
		res.Status, res.Error = sub.Status, err.Error()
		return res
	}

	if err := h.nifi.Send(ctx, action, sub.ExchangeName, sub.Market); err != nil {
		h.log.Error("nifi rejected the request, rolling the row back",
			"market", res.Market, "action", action, "from", pending, "to", sub.Status, "err", err)
		if rbErr := h.store.SetStatus(ctx, id, sub.Status); rbErr != nil {
			h.log.Error("rollback failed — row is left pending",
				"market", res.Market, "status", pending, "err", rbErr)
			res.Status, res.Error = pending, err.Error()+" (rollback also failed: "+rbErr.Error()+")"
			return res
		}
		res.Status, res.Error = sub.Status, err.Error()
		return res
	}

	h.log.Info("asked nifi to change a subscription",
		"market", res.Market, "action", action, "status", pending)
	res.Status, res.OK = pending, true
	return res
}

func (h *Handler) fail(w http.ResponseWriter, code int, err error) {
	h.log.Error("request failed", "code", code, "err", err)
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

// ParseID is a small helper shared with tests.
func ParseID(s string) (int64, error) { return strconv.ParseInt(s, 10, 64) }
