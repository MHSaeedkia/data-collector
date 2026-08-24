// Package registry resolves pair_id/exchange_id identities carried by the
// Flink output into display metadata (base/quote/exchange name+label).
package registry

import (
	"context"
	"log"
	"sort"
	"strconv"
	"sync"

	"orderbook-web/internal/domain"
	"orderbook-web/internal/ports"
)

// Registry holds the id -> display maps, refreshed periodically from the
// repository.
type Registry struct {
	repo ports.MarketRepository

	mu        sync.RWMutex
	markets   map[int]domain.Market
	exchanges map[int]domain.Exchange
}

func New(repo ports.MarketRepository) *Registry {
	return &Registry{
		repo:      repo,
		markets:   map[int]domain.Market{},
		exchanges: map[int]domain.Exchange{},
	}
}

// Refresh reloads the markets and exchanges maps. Only replaces a map if
// its load returned something, so a transient repository error doesn't
// blank out display data already held.
func (r *Registry) Refresh(ctx context.Context) {
	markets, err := r.repo.LoadMarkets(ctx)
	if err != nil {
		log.Printf("markets query error: %v", err)
	}

	exchanges, err := r.repo.LoadExchanges(ctx)
	if err != nil {
		log.Printf("exchanges query error: %v", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if len(markets) > 0 {
		r.markets = markets
	}
	if len(exchanges) > 0 {
		r.exchanges = exchanges
	}
}

// Catalog is the full id -> display listing the browser needs to build
// its dropdowns, sorted by id so the option order is stable across
// refreshes. Everything postgres knows about is listed, whether or not it
// has produced data — with server-side filtering the client only receives
// the books it selected, so it cannot derive the lists from the stream.
func (r *Registry) Catalog() domain.Catalog {
	r.mu.RLock()
	defer r.mu.RUnlock()

	c := domain.Catalog{
		Markets:   make([]domain.Market, 0, len(r.markets)),
		Exchanges: make([]domain.Exchange, 0, len(r.exchanges)),
	}
	for _, m := range r.markets {
		c.Markets = append(c.Markets, m)
	}
	for _, e := range r.exchanges {
		c.Exchanges = append(c.Exchanges, e)
	}
	sort.Slice(c.Markets, func(i, j int) bool { return c.Markets[i].ID < c.Markets[j].ID })
	sort.Slice(c.Exchanges, func(i, j int) bool { return c.Exchanges[i].ID < c.Exchanges[j].ID })
	return c
}

// Enrich resolves a raw book into the display shape pushed to the
// browser. Unknown ids fall back to placeholders. A per-exchange book
// (one whose ExchangeID is a real exchange) also gets its source exchange
// resolved at the book level — that is what the browser routes on, while
// the per-level exchange keeps the table rendering identical for both
// kinds of book. A merged book resolves a list per level instead of one.
func (r *Registry) Enrich(rb domain.RawBook) domain.Book {
	r.mu.RLock()
	defer r.mu.RUnlock()

	m, ok := r.markets[rb.PairID]
	if !ok {
		m = domain.Market{ID: rb.PairID, Base: "p" + strconv.Itoa(rb.PairID), Quote: "?"}
	}

	levels := make([]domain.Level, 0, len(rb.Levels))
	for _, rl := range rb.Levels {
		l := domain.Level{
			Price:      rl.Price,
			Quantity:   rl.Quantity,
			Simulation: rl.Simulation,
			SourceID:   rl.SourceID,
		}
		// A merged level sums several exchanges, so it resolves a list and
		// leaves the scalar Exchange empty — resolving id 0 there would
		// stamp every merged row with the "unknown" placeholder.
		if rb.Merged {
			l.Exchanges = make([]domain.Exchange, 0, len(rl.ExchangeIDs))
			for _, id := range rl.ExchangeIDs {
				l.Exchanges = append(l.Exchanges, r.exchange(id))
			}
		} else {
			l.Exchange = r.exchange(rl.ExchangeID)
		}
		levels = append(levels, l)
	}

	b := domain.Book{
		PairID:    rb.PairID,
		Merged:    rb.Merged,
		Base:      m.Base,
		Quote:     m.Quote,
		Side:      rb.Side,
		ID:        rb.ID,
		Levels:    levels,
		EventTime: rb.EventTime,
	}
	if rb.ExchangeID != domain.AggregatedExchangeID {
		ex := r.exchange(rb.ExchangeID)
		b.Exchange = &ex
	}
	return b
}

// exchange resolves one id, with the placeholder fallback. Callers hold
// the read lock.
func (r *Registry) exchange(id int) domain.Exchange {
	if ex, ok := r.exchanges[id]; ok {
		return ex
	}
	return domain.Exchange{ID: id, Name: "unknown", Label: "نامشخص"}
}
