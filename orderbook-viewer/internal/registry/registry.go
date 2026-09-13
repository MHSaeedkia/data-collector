// Package registry resolves pair_id/exchange_id identities carried by the
// Flink output into display metadata (base/quote/exchange name+label).
package registry

import (
	"context"
	"log"
	"sort"
	"strconv"
	"sync"

	"orderbook-viewer/internal/domain"
	"orderbook-viewer/internal/ports"
)

// Registry holds the id -> display maps, refreshed periodically from the
// repository.
type Registry struct {
	repo ports.MarketRepository
	// defaults are the dropdown values the page opens on, as configured.
	// The two ids are checked against the maps below on every refresh —
	// they are uncheckable until postgres has answered at least once,
	// which on a cold start it may not have.
	defaults domain.Defaults

	mu        sync.RWMutex
	markets   map[int]domain.Market
	exchanges map[int]domain.Exchange
	// stale records that a load last came back empty, so the "serving
	// old data" warning is printed on the TRANSITION rather than on every
	// tick. Refresh runs every 10s; a line per tick per map would bury the
	// websocket and consumer lines this log exists to make readable.
	stale map[string]bool
	// lastErr is the previous failure per load, so a repeating error is
	// reported once rather than on every tick.
	lastErr map[string]string
	// defaultPairID/defaultExchangeID are the checked defaults, recomputed
	// on each refresh, and resolvedID is what they were last reported as —
	// the same "say it on the transition" discipline as lastErr.
	defaultPairID     int
	defaultExchangeID int
	resolvedID        map[string]string
}

func New(repo ports.MarketRepository, defaults domain.Defaults) *Registry {
	return &Registry{
		repo:              repo,
		defaults:          defaults,
		markets:           map[int]domain.Market{},
		exchanges:         map[int]domain.Exchange{},
		stale:             map[string]bool{},
		lastErr:           map[string]string{},
		defaultExchangeID: domain.AggregatedExchangeID,
		resolvedID:        map[string]string{},
	}
}

// Refresh reloads the markets and exchanges maps. Only replaces a map if
// its load returned something, so a transient repository error doesn't
// blank out display data already held.
func (r *Registry) Refresh(ctx context.Context) {
	markets, marketsErr := r.repo.LoadMarkets(ctx)
	exchanges, exchangesErr := r.repo.LoadExchanges(ctx)

	r.mu.Lock()
	defer r.mu.Unlock()
	r.logErr("markets", marketsErr)
	r.logErr("exchanges", exchangesErr)
	r.markets = adopt(r, "markets", markets, r.markets)
	r.exchanges = adopt(r, "exchanges", exchanges, r.exchanges)
	r.resolveDefaults()
}

// resolveDefaults checks the configured ids against the maps just loaded
// and keeps the ones postgres actually has. It runs on every refresh
// rather than once at startup because postgres may not have answered yet
// when the process comes up, and because a market or exchange can be
// deleted under us. Callers hold the write lock.
func (r *Registry) resolveDefaults() {
	r.defaultPairID = 0
	if want := r.defaults.PairID; want != 0 {
		m, ok := r.markets[want]
		if ok {
			r.defaultPairID = want
		}
		r.logResolved("DEFAULT_PAIR_ID", want, m.Base+"/"+m.Quote, ok)
	}

	r.defaultExchangeID = domain.AggregatedExchangeID
	switch want := r.defaults.ExchangeID; want {
	case domain.AggregatedExchangeID: // the separated view; nothing to look up
	case domain.MergedExchangeID:
		r.defaultExchangeID = domain.MergedExchangeID
	default:
		e, ok := r.exchanges[want]
		if ok {
			r.defaultExchangeID = want
		}
		r.logResolved("DEFAULT_EXCHANGE_ID", want, e.Name, ok)
	}
}

// logResolved names what a configured id turned out to BE, once — when
// the answer changes, not on every 10s refresh. Saying the name is the
// point: an id in a config file is unreadable on its own, and the page
// silently opening on a different pair than the one configured is
// otherwise indistinguishable from a typo nobody made. Callers hold the
// write lock.
func (r *Registry) logResolved(what string, want int, name string, ok bool) {
	state := name
	if !ok {
		state = ""
	}
	if prev, seen := r.resolvedID[what]; seen && prev == state {
		return
	}
	r.resolvedID[what] = state
	if !ok {
		log.Printf("registry: %s=%d is not in postgres (yet) — the page falls back", what, want)
		return
	}
	log.Printf("registry: %s=%d is %s", what, want, name)
}

// logErr prints a query failure once per run of identical failures, not
// once per 10s tick. A postgres outage otherwise repeats the same
// multi-line pgx error every tick for as long as it lasts, which drowns
// the websocket and consumer lines that this log exists to surface.
// Callers hold the write lock.
func (r *Registry) logErr(what string, err error) {
	if err == nil {
		if prev := r.lastErr[what]; prev != "" {
			r.lastErr[what] = ""
			log.Printf("registry: %s query is working again", what)
		}
		return
	}
	if msg := err.Error(); msg != r.lastErr[what] {
		r.lastErr[what] = msg
		log.Printf("registry: %s query error: %v", what, err)
	}
}

// adopt takes the freshly loaded map if the load returned anything, and
// otherwise keeps what is already held. Keeping the old map is the right
// behaviour but an invisible one: without a word in the log, a registry
// that has been serving hours-old names looks exactly like a healthy one.
// The word is said once when it goes stale and once when it recovers.
func adopt[T any](r *Registry, what string, loaded, held map[int]T) map[int]T {
	if len(loaded) == 0 {
		if !r.stale[what] {
			r.stale[what] = true
			log.Printf("registry: %s load returned nothing — SERVING THE PREVIOUS %d until it recovers", what, len(held))
		}
		return held
	}
	if r.stale[what] {
		r.stale[what] = false
		log.Printf("registry: %s load recovered — %d now", what, len(loaded))
	} else if len(loaded) != len(held) {
		log.Printf("registry: %d %s (was %d)", len(loaded), what, len(held))
	}
	return loaded
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

	// Everything the dropdowns need comes from here, including the depths,
	// so there is one place to look for what a catalog message contains.
	c.LevelLimits = domain.LevelLimits
	c.DefaultLevelLimit = r.defaults.LevelLimit
	c.DefaultPairID = r.defaultPairID
	c.DefaultExchangeID = r.defaultExchangeID
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
