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
}

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
}
