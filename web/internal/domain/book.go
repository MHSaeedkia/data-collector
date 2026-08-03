package domain

// RawLevel/RawBook are the aggregated book as produced by the Flink job
// (identity only: pair_id / exchange_id, no display fields).

type RawLevel struct {
	ExchangeID int `json:"exchange_id"`
	// Simulation is per level, not per book: the aggregator unions across
	// exchanges, so one book can mix live and simulated sources.
	// 0 = live data, 1 = simulation data.
	Simulation int    `json:"simulation"`
	Price      string `json:"price"`
	Quantity   string `json:"quantity"`
}

type RawBook struct {
	PairID    int        `json:"pair_id"`
	Side      string     `json:"side"`
	Levels    []RawLevel `json:"levels"`
	EventTime int64      `json:"event_time"`
}

// Level/Book are the enriched book pushed to the browser, with display
// fields (base, quote, exchange name/label) resolved.

type Level struct {
	Price      string   `json:"price"`
	Quantity   string   `json:"quantity"`
	Simulation int      `json:"simulation"`
	Exchange   Exchange `json:"exchange"`
}

type Book struct {
	PairID    int     `json:"pair_id"`
	Base      string  `json:"base"`
	Quote     string  `json:"quote"`
	Side      string  `json:"side"`
	Levels    []Level `json:"levels"`
	EventTime int64   `json:"event_time"`
}

// WSSnapshot/WSUpdate are the two message shapes sent over the websocket.

type WSSnapshot struct {
	Type  string `json:"type"`
	Books []Book `json:"books"`
}

type WSUpdate struct {
	Type string `json:"type"`
	Book Book   `json:"book"`
}
