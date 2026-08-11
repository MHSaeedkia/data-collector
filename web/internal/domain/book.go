package domain

// RawLevel/RawBook are a book as produced by the Flink jobs
// (identity only: pair_id / exchange_id, no display fields).
//
// Two producers land in this shape. Job 6 (the aggregator) writes
// p{pair_id}-{side}: one record per side, unioned across exchanges, so
// exchange_id and simulation are per LEVEL. Job 5 (the book builder)
// writes ex{id}-p{id}-orderbook-snapshot-flink: one record holding BOTH
// sides for a single exchange, so exchange_id and simulation are per
// RECORD. The decoder splits a job-5 record into two RawBooks and copies
// the record-level exchange/simulation onto every level, so everything
// downstream of it sees one shape.

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
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
}

type RawBook struct {
	PairID int `json:"pair_id"`
	// ExchangeID is 0 for the aggregated book and the source exchange for
	// a per-exchange book. Exchange ids start at 1, so 0 is free to mean
	// "all exchanges" — the same convention the browser sends back in a
	// Select.
	ExchangeID int    `json:"exchange_id"`
	Side       string `json:"side"`
	// ID is the producing job's id for this record. There is no SourceIDs
	// counterpart — the parents are per level, on RawLevel.SourceID.
	ID        string     `json:"id"`
	Levels    []RawLevel `json:"levels"`
	EventTime int64      `json:"event_time"`
}

// Level/Book are the enriched book pushed to the browser, with display
// fields (base, quote, exchange name/label) resolved.

type Level struct {
	Price      string   `json:"price"`
	Quantity   string   `json:"quantity"`
	Simulation int      `json:"simulation"`
	SourceID   string   `json:"source_id"`
	Exchange   Exchange `json:"exchange"`
}

type Book struct {
	PairID int `json:"pair_id"`
	// Exchange is nil for the aggregated book and the source exchange for
	// a per-exchange book. It is what the browser routes on; the per-level
	// Exchange is still filled in either case so the table renders the
	// same way.
	Exchange  *Exchange `json:"exchange,omitempty"`
	Base      string    `json:"base"`
	Quote     string    `json:"quote"`
	Side      string    `json:"side"`
	ID        string    `json:"id"`
	Levels    []Level   `json:"levels"`
	EventTime int64     `json:"event_time"`
}

// ExchangeID is the id the browser selects on: 0 for the aggregated book.
func (b Book) ExchangeID() int {
	if b.Exchange == nil {
		return 0
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

// Selection is a client's current view: one pair, one exchange (0 = the
// aggregated union). Side is set only when it doubles as a book key.
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
