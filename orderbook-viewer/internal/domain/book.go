package domain

// The vocabulary of exchange_id, in both directions on the wire: it is
// either a real exchange or one of the two cross-exchange views, and
// nothing else. Real ids come from postgres and start at 1, which is what
// leaves 0 and the negatives free to name the views. Never write these
// numbers as literals — a bare 0 or -1 is unreadable at the point of use,
// and these constants are what keeps the browser, the hub and the
// registry talking about the same three things.
const (
	// AggregatedExchangeID is job 6's union: every exchange's levels side
	// by side, each level keeping its own exchange.
	AggregatedExchangeID = 0
	// MergedExchangeID is the price merger's summed view: one level per
	// price, naming the list of exchanges behind the sum.
	MergedExchangeID = -1
)

// LevelLimits are the depths a browser may ask for — how many levels of
// one side it wants — and the whole list the UI dropdown offers. The
// limit is applied on the server, before the fan-out: these books are far
// deeper than a page can usefully render, and a browser that drops the
// extra levels itself has already paid to receive and parse them.
//
// The list is sent to the page in the Catalog rather than written out
// again in JavaScript, so unlike the exchange_id constants above there is
// no second copy to keep in step.
var LevelLimits = []int{25, 50, 100, 200}

// ValidLevelLimit reports whether n is one of LevelLimits. Anything else
// — a client asking for a depth nobody offers, a mistyped env var — falls
// back to the configured default instead of being honoured.
func ValidLevelLimit(n int) bool {
	for _, l := range LevelLimits {
		if l == n {
			return true
		}
	}
	return false
}

// RawLevel/RawBook are a book as produced by the Flink jobs
// (identity only: pair_id / exchange_id, no display fields).
//
// Three producers land in this shape. Job 6 (the aggregator) writes
// p{pair_id}-{side}: one record per side, unioned across exchanges, so
// exchange_id and simulation are per LEVEL. Job 5 (the book builder)
// writes ex{id}-p{id}-orderbook-snapshot-flink: one record holding BOTH
// sides for a single exchange, so exchange_id and simulation are per
// RECORD. The decoder splits a job-5 record into two RawBooks and copies
// the record-level exchange/simulation onto every level, so everything
// downstream of it sees one shape. The merger writes p{pair_id}-{side}-merged:
// one record per side like job 6, but with the quantities at each price
// summed, so a level names a LIST of exchanges instead of one.

type RawLevel struct {
	ExchangeID int `json:"exchange_id"`
	// Simulation is per level, not per book: the aggregator unions across
	// exchanges, so one book can mix live and simulated sources.
	// 0 = live data, 1 = simulation data.
	Simulation int `json:"simulation"`
	// SourceID is per level for the same reason Simulation is: it is the
	// id of the job-5 snapshot this level came from, and one book's
	// levels come from several snapshots. On a per-exchange book it is
	// instead the job-4 event that last set the level (job 5's own
	// per-level lineage) — one hop further up, same meaning.
	SourceID string `json:"source_id"`
	// ExchangeIDs/SourceIDs replace the two scalars above on a MERGED
	// level and are empty otherwise: merging sums every exchange quoting a
	// price into one level, so there is no single exchange or parent to
	// name. Positionally aligned with each other, as on the wire.
	ExchangeIDs []int    `json:"exchange_ids,omitempty"`
	SourceIDs   []string `json:"source_ids,omitempty"`
	Price       string   `json:"price"`
	Quantity    string   `json:"quantity"`
}

type RawBook struct {
	PairID int `json:"pair_id"`
	// ExchangeID is AggregatedExchangeID for the aggregated book and the
	// source exchange for a per-exchange book — the same vocabulary the
	// browser sends back in a Select.
	ExchangeID int `json:"exchange_id"`
	// Merged marks a book from the price merger: quantities summed, one
	// level per price. A producer never stamps MergedExchangeID here; this
	// flag is what Book.ExchangeID turns into it.
	Merged bool   `json:"merged"`
	Side   string `json:"side"`
	// ID is the producing job's id for this record. There is no SourceIDs
	// counterpart — the parents are per level, on RawLevel.SourceID.
	ID        string     `json:"id"`
	Levels    []RawLevel `json:"levels"`
	EventTime int64      `json:"event_time"`
}

// Level/Book are the enriched book pushed to the browser, with display
// fields (base, quote, exchange name/label) resolved.

type Level struct {
	Price      string `json:"price"`
	Quantity   string `json:"quantity"`
	Simulation int    `json:"simulation"`
	SourceID   string `json:"source_id"`
	// Exchange is the one exchange behind this level. On a merged level it
	// is the zero value and Exchanges carries the contributors instead —
	// that is the whole point of the merged view.
	Exchange  Exchange   `json:"exchange"`
	Exchanges []Exchange `json:"exchanges,omitempty"`
}

type Book struct {
	PairID int `json:"pair_id"`
	// Exchange is nil for the aggregated book and the source exchange for
	// a per-exchange book. It is what the browser routes on; the per-level
	// Exchange is still filled in either case so the table renders the
	// same way.
	Exchange *Exchange `json:"exchange,omitempty"`
	// Merged is true for the price-merged view. Exchange is nil for both
	// that and the aggregated union, so this is what tells them apart.
	Merged    bool    `json:"merged"`
	Base      string  `json:"base"`
	Quote     string  `json:"quote"`
	Side      string  `json:"side"`
	ID        string  `json:"id"`
	Levels    []Level `json:"levels"`
	EventTime int64   `json:"event_time"`
}

// ExchangeID is the id the browser selects on — the one place that maps a
// book onto the exchange_id vocabulary declared at the top of this file.
// Merged is checked first because Exchange is nil for both cross-exchange
// views, so nil alone cannot tell them apart.
func (b Book) ExchangeID() int {
	if b.Merged {
		return MergedExchangeID
	}
	if b.Exchange == nil {
		return AggregatedExchangeID
	}
	return b.Exchange.ID
}

// Limit returns this book with at most n levels, keeping the FIRST n:
// every producer emits asks ascending and bids descending, so the levels
// at the front are the ones nearest the spread and the only ones a
// shallow view wants. n <= 0 means no limit.
//
// The receiver is a copy and the level array is never written to, because
// the hub holds ONE book per key and each client watching it chooses its
// own depth.
func (b Book) Limit(n int) Book {
	if n <= 0 || len(b.Levels) <= n {
		return b
	}
	b.Levels = b.Levels[:n]
	return b
}

// Key identifies the one book a producer keeps overwriting. Derived from
// the content rather than the Kafka topic because a job-5 record carries
// two sides on one topic — and because topic strings stay opaque to
// everything past the consumer.
func (b Book) Key() Selection {
	return Selection{PairID: b.PairID, ExchangeID: b.ExchangeID(), Side: b.Side}
}

// Selection is a client's current view: one pair, one exchange — a real
// id, AggregatedExchangeID or MergedExchangeID. Side is set only when it
// doubles as a book key.
type Selection struct {
	PairID     int    `json:"pair_id"`
	ExchangeID int    `json:"exchange_id"`
	Side       string `json:"side,omitempty"`
}

// Matches reports whether a book belongs to this selection.
func (s Selection) Matches(b Book) bool {
	return b.PairID == s.PairID && b.ExchangeID() == s.ExchangeID
}

// Catalog is the dropdown content: every market and every exchange known
// to postgres. It is deliberately independent of what has arrived on
// Kafka — with server-side filtering a client only ever receives the
// books it asked for, so it cannot infer the lists from the data.
type Catalog struct {
	Markets   []Market   `json:"markets"`
	Exchanges []Exchange `json:"exchanges"`
	// LevelLimits is the depth dropdown's contents, and the three Default
	// fields are the entry each of the three dropdowns opens on. They ride
	// on the catalog because it is already the one message that bootstraps
	// the dropdowns, and they come from the server so the page holds no
	// copy of the vocabulary and no opinion about what to show first.
	//
	// DefaultPairID is 0 when nothing is configured or the configured pair
	// is not a market postgres knows; the page then falls back to the
	// first market in the list. DefaultExchangeID always names one of the
	// three things an exchange_id can be, so it needs no such escape.
	LevelLimits       []int `json:"level_limits"`
	DefaultLevelLimit int   `json:"default_level_limit"`
	DefaultPairID     int   `json:"default_pair_id"`
	DefaultExchangeID int   `json:"default_exchange_id"`
}

// Defaults are the three dropdown values the page opens on, exactly as
// configured (see internal/config). Pair and Exchange are written the way
// a person writes them in .env — "BTC/USDT", "okx", "aggregated",
// "merged" — never as ids: an id in a hand-edited file says nothing
// without the database open next to it. Resolving those names is the
// registry's job, because postgres is where the names live.
type Defaults struct {
	Pair       string
	Exchange   string
	LevelLimit int
}

// The two views the exchange dropdown offers besides a real exchange,
// spelled as DEFAULT_EXCHANGE takes them and as the dropdown shows them.
//
// SeparatedName is the user's word for the view AggregatedExchangeID
// identifies — job 6's union, where every exchange's levels sit side by
// side, each keeping its own exchange. The pipeline calls that job the
// aggregator and the wire vocabulary above follows it; the page says
// "separated", because to a reader of the book the levels are exactly
// that. Only the words a person sees changed (2026-09-13); nothing on the
// wire did.
const (
	SeparatedName = "separated"
	MergedName    = "merged"
)

// The websocket message shapes. Server -> client: catalog, snapshot,
// update. Client -> server: select.

type WSCatalog struct {
	Type string `json:"type"`
	Catalog
}

type WSSnapshot struct {
	Type  string `json:"type"`
	Books []Book `json:"books"`
}

type WSUpdate struct {
	Type string `json:"type"`
	Book Book   `json:"book"`
}

type WSSelect struct {
	Type       string `json:"type"`
	PairID     int    `json:"pair_id"`
	ExchangeID int    `json:"exchange_id"`
	// Limit is the depth this client wants, one of LevelLimits. It is
	// part of the selection rather than of Selection itself: Selection
	// doubles as the key of a stored book, and how deep one browser
	// renders is not part of what a book IS.
	Limit int `json:"limit"`
}
