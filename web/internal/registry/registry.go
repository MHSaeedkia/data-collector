// Package registry resolves pair_id/exchange_id identities carried by the
// Flink output into display metadata (base/quote/exchange name+label).
package registry

import (
	"context"
	"log"
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

// Enrich resolves a raw consolidated book into the display shape pushed
// to the browser. Unknown ids fall back to placeholders.
func (r *Registry) Enrich(rb domain.RawBook) domain.Book {
	r.mu.RLock()
	defer r.mu.RUnlock()

	m, ok := r.markets[rb.PairID]
	if !ok {
		m = domain.Market{ID: rb.PairID, Base: "p" + strconv.Itoa(rb.PairID), Quote: "?"}
	}

	levels := make([]domain.Level, 0, len(rb.Levels))
	for _, rl := range rb.Levels {
		ex, ok := r.exchanges[rl.ExchangeID]
		if !ok {
			ex = domain.Exchange{ID: rl.ExchangeID, Name: "unknown", Label: "unknown"}
		}
		levels = append(levels, domain.Level{Price: rl.Price, Quantity: rl.Quantity, Exchange: ex})
	}

	return domain.Book{
		PairID:    rb.PairID,
		Base:      m.Base,
		Quote:     m.Quote,
		Side:      rb.Side,
		Levels:    levels,
		EventTime: rb.EventTime,
	}
}
