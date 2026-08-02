// Scenarios for ex3/wallex — one side per message, no ordering field, no timestamp.
//
// What makes ex3 different from every other exchange in the suite:
//
//   - The envelope is a 2-element array `["{market}@{side}", [levels…]]`; buyDepth is bids,
//     sellDepth is asks, and the side that is not in the message stays NULL — "no report for
//     this side", which job 5 must leave alone even though the event is a snapshot.
//   - Levels are objects with JSON-NUMBER price/quantity (every other exchange sends strings),
//     so the values come off the wire as BigDecimal-from-literal.
//   - There is no sequence field and no timestamp anywhere on the wire. Job 1 stamps processing
//     time, so `EventTime` is wall-clock and every scenario here sets IgnoreEventTime.
//
// That last point is why none of these scenarios expect a rejection. Job 2 keys its null-sequence
// guard on event time, and processing time only ever moves forward, so the guard can never fire
// for ex3; job 3 only dead-letters a missing rebase row, and pair resolution and the rebase
// factors come from the same `exchange_markets` row, so a frame that got a pair_id has one. ex3
// cannot produce a dead-letter at all — its whole failure surface is drops (job 1) and book state.
//
// Pair 1 (BTC/USDT) is price_precision 2 / quantity_precision 8, and ex3's rebase is 0/0.

package scenario

import "orderbook-e2e/events"

// Ex3WallexHalfBook — wallex sends one side per message; a null side must never wipe the other.
var Ex3WallexHalfBook = Scenario{
	ExchangeID:      3,
	PairID:          1,
	IgnoreEventTime: true,
	Sources: []string{
		// 01 buy depth
		`[
	"BTCUSDT@buyDepth",
	[
		{ "price": 62942.5, "quantity": 0.8, "sum": 50354.0 },
		{ "price": 62937.5, "quantity": 1.6, "sum": 100700.0 },
		{ "price": 62932.5, "quantity": 2.9, "sum": 182504.25 }
	]
]`,
		// 02 sell depth
		`[
	"BTCUSDT@sellDepth",
	[
		{ "price": 62952.5, "quantity": 0.7, "sum": 44066.75 },
		{ "price": 62957.5, "quantity": 1.4, "sum": 88140.5 },
		{ "price": 62962.5, "quantity": 2.2, "sum": 138517.5 }
	]
]`,
		// 03 buy depth refresh
		`[
	"BTCUSDT@buyDepth",
	[
		{ "price": 62942.5, "quantity": 0.5, "sum": 31471.25 },
		{ "price": 62927.5, "quantity": 3.5, "sum": 220246.25 }
	]
]`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 buy depth
			ExchangeID: 3,
			PairID:     1,
			Asks:       []events.PriceLevel{},
			Bids: []events.PriceLevel{
				{Price: "62942.5", Quantity: "0.8"},
				{Price: "62937.5", Quantity: "1.6"},
				{Price: "62932.5", Quantity: "2.9"},
			},
		},
		{ // after 02 sell depth
			ExchangeID: 3,
			PairID:     1,
			Asks: []events.PriceLevel{
				{Price: "62952.5", Quantity: "0.7"},
				{Price: "62957.5", Quantity: "1.4"},
				{Price: "62962.5", Quantity: "2.2"},
			},
			Bids: []events.PriceLevel{
				{Price: "62942.5", Quantity: "0.8"},
				{Price: "62937.5", Quantity: "1.6"},
				{Price: "62932.5", Quantity: "2.9"},
			},
		},
		{ // after 03 buy depth refresh
			ExchangeID: 3,
			PairID:     1,
			Asks: []events.PriceLevel{
				{Price: "62952.5", Quantity: "0.7"},
				{Price: "62957.5", Quantity: "1.4"},
				{Price: "62962.5", Quantity: "2.2"},
			},
			Bids: []events.PriceLevel{
				{Price: "62942.5", Quantity: "0.5"},
				{Price: "62927.5", Quantity: "3.5"},
			},
		},
	},
	// The two sides were reported by separate messages; the aggregated view is one book.
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 3, Price: "62952.5", Quantity: "0.7"},
			{ExchangeID: 3, Price: "62957.5", Quantity: "1.4"},
			{ExchangeID: 3, Price: "62962.5", Quantity: "2.2"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 3, Price: "62942.5", Quantity: "0.5"},
			{ExchangeID: 3, Price: "62927.5", Quantity: "3.5"},
		},
	},
}

// Ex3EmptySideWipe — an empty level array is a report that the side is empty, unlike an absent
// side; and a zero quantity inside a per-side snapshot leaves no level behind.
var Ex3EmptySideWipe = Scenario{
	ExchangeID:      3,
	PairID:          1,
	IgnoreEventTime: true,
	Sources: []string{
		// 01 buy depth
		`[
	"BTCUSDT@buyDepth",
	[
		{ "price": 62500.25, "quantity": 0.5, "sum": 31250.125 },
		{ "price": 62499.5, "quantity": 1.25, "sum": 78124.375 }
	]
]`,
		// 02 sell depth
		`[
	"BTCUSDT@sellDepth",
	[
		{ "price": 62501.75, "quantity": 0.4, "sum": 25000.7 },
		{ "price": 62502, "quantity": 2, "sum": 125004 }
	]
]`,
		// 03 buy depth, empty — wallex reporting no bids at all
		`[
	"BTCUSDT@buyDepth",
	[]
]`,
		// 04 sell depth carrying a zero-quantity level
		`[
	"BTCUSDT@sellDepth",
	[
		{ "price": 62503.5, "quantity": 0, "sum": 0 },
		{ "price": 62504, "quantity": 1.5, "sum": 93756 }
	]
]`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 buy depth
			ExchangeID: 3,
			PairID:     1,
			Asks:       []events.PriceLevel{},
			Bids: []events.PriceLevel{
				{Price: "62500.25", Quantity: "0.5"},
				{Price: "62499.5", Quantity: "1.25"},
			},
		},
		{ // after 02 sell depth
			ExchangeID: 3,
			PairID:     1,
			Asks: []events.PriceLevel{
				{Price: "62501.75", Quantity: "0.4"},
				{Price: "62502", Quantity: "2"},
			},
			Bids: []events.PriceLevel{
				{Price: "62500.25", Quantity: "0.5"},
				{Price: "62499.5", Quantity: "1.25"},
			},
		},
		{ // after 03 empty buy depth — bids wiped, asks untouched
			ExchangeID: 3,
			PairID:     1,
			Asks: []events.PriceLevel{
				{Price: "62501.75", Quantity: "0.4"},
				{Price: "62502", Quantity: "2"},
			},
			Bids: []events.PriceLevel{},
		},
		{ // after 04 sell depth with a zero level — the side is replaced, the zero rests nowhere
			ExchangeID: 3,
			PairID:     1,
			Asks: []events.PriceLevel{
				{Price: "62504", Quantity: "1.5"},
			},
			Bids: []events.PriceLevel{},
		},
	},
	// An emptied side reaches the aggregated view as a book with no levels at all — the same
	// shape the aggregator produces when a reset drops an exchange out of the union.
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 3, Price: "62504", Quantity: "1.5"},
		},
		Bids: []events.AggregatedLevel{},
	},
}

// Ex3PrecisionDust — JSON-number levels still go through job 4: prices truncate DOWN to the
// market's 2 places (colliding ones merging into one level with their quantities summed) and a
// quantity below the market's 8 places truncates to zero, which job 5 reads as a delete.
var Ex3PrecisionDust = Scenario{
	ExchangeID:      3,
	PairID:          1,
	IgnoreEventTime: true,
	Sources: []string{
		// 01 buy depth — the first two prices collide at 2 decimal places
		`[
	"BTCUSDT@buyDepth",
	[
		{ "price": 62500.123, "quantity": 0.4, "sum": 25000.0492 },
		{ "price": 62500.129, "quantity": 0.25, "sum": 15625.03225 },
		{ "price": 62499.999, "quantity": 1.5, "sum": 93749.9985 }
	]
]`,
		// 02 sell depth — the first quantity is dust below the market's lot precision
		`[
	"BTCUSDT@sellDepth",
	[
		{ "price": 62501.01, "quantity": 0.000000005, "sum": 0.00031250505 },
		{ "price": 62502.5, "quantity": 1.000000009, "sum": 62502.5005625225 }
	]
]`,
		// 03 buy depth — an integer price literal, and truncation that must not round up
		`[
	"BTCUSDT@buyDepth",
	[
		{ "price": 62200, "quantity": 0.068493, "sum": 4260.2646 },
		{ "price": 62199.999, "quantity": 0.123456789, "sum": 7679.012152343211 }
	]
]`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 buy depth — .123 and .129 merged at .12, quantities summed to 0.65
			ExchangeID: 3,
			PairID:     1,
			Asks:       []events.PriceLevel{},
			Bids: []events.PriceLevel{
				{Price: "62500.12", Quantity: "0.65"},
				{Price: "62499.99", Quantity: "1.5"},
			},
		},
		{ // after 02 sell depth — the dust level truncated to 0 and was deleted, not rested
			ExchangeID: 3,
			PairID:     1,
			Asks: []events.PriceLevel{
				{Price: "62502.5", Quantity: "1"},
			},
			Bids: []events.PriceLevel{
				{Price: "62500.12", Quantity: "0.65"},
				{Price: "62499.99", Quantity: "1.5"},
			},
		},
		{ // after 03 buy depth — 62199.999 truncates DOWN to 62199.99, never up to 62200
			ExchangeID: 3,
			PairID:     1,
			Asks: []events.PriceLevel{
				{Price: "62502.5", Quantity: "1"},
			},
			Bids: []events.PriceLevel{
				{Price: "62200", Quantity: "0.068493"},
				{Price: "62199.99", Quantity: "0.12345678"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{{ExchangeID: 3, Price: "62502.5", Quantity: "1"}},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 3, Price: "62200", Quantity: "0.068493"},
			{ExchangeID: 3, Price: "62199.99", Quantity: "0.12345678"},
		},
	},
}

// Ex3NoiseFrames — everything that is not a well-formed depth message for a known market is
// dropped by job 1 without a dead-letter and without touching the book.
var Ex3NoiseFrames = Scenario{
	ExchangeID:      3,
	PairID:          1,
	IgnoreEventTime: true,
	Sources: []string{
		// 01 buy depth
		`[
	"BTCUSDT@buyDepth",
	[
		{ "price": 62450.5, "quantity": 0.3, "sum": 18735.15 },
		{ "price": 62449, "quantity": 1.1, "sum": 68693.9 }
	]
]`,
		// 02 not the array envelope at all
		`{ "ping": 1 }`,
		// 03 a known market, but not a depth channel
		`[
	"BTCUSDT@trades",
	[
		{ "price": 62450.5, "quantity": 0.3, "sum": 18735.15 }
	]
]`,
		// 04 no @ in the key, so there is no side to read
		`[
	"BTCUSDT",
	[
		{ "price": 62450.5, "quantity": 0.3, "sum": 18735.15 }
	]
]`,
		// 05 a third element the envelope does not have
		`[
	"BTCUSDT@buyDepth",
	[
		{ "price": 62450.5, "quantity": 0.3, "sum": 18735.15 }
	],
	"extra"
]`,
		// 06 a market ex3 has no exchange_markets row for
		`[
	"FOOBARUSDT@buyDepth",
	[
		{ "price": 1.5, "quantity": 10, "sum": 15 }
	]
]`,
		// 07 string levels — ex3's wire is JSON numbers, so the whole frame is unparseable
		`[
	"BTCUSDT@sellDepth",
	[
		{ "price": "62451", "quantity": "0.9" }
	]
]`,
		// 08 sell depth
		`[
	"BTCUSDT@sellDepth",
	[
		{ "price": 62451, "quantity": 0.9, "sum": 56205.9 },
		{ "price": 62452.25, "quantity": 0.6, "sum": 37471.35 }
	]
]`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 buy depth
			ExchangeID: 3,
			PairID:     1,
			Asks:       []events.PriceLevel{},
			Bids: []events.PriceLevel{
				{Price: "62450.5", Quantity: "0.3"},
				{Price: "62449", Quantity: "1.1"},
			},
		},
		{ // after 08 sell depth — 02 through 07 emitted nothing, and the bids are still 01's
			ExchangeID: 3,
			PairID:     1,
			Asks: []events.PriceLevel{
				{Price: "62451", Quantity: "0.9"},
				{Price: "62452.25", Quantity: "0.6"},
			},
			Bids: []events.PriceLevel{
				{Price: "62450.5", Quantity: "0.3"},
				{Price: "62449", Quantity: "1.1"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 3, Price: "62451", Quantity: "0.9"},
			{ExchangeID: 3, Price: "62452.25", Quantity: "0.6"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 3, Price: "62450.5", Quantity: "0.3"},
			{ExchangeID: 3, Price: "62449", Quantity: "1.1"},
		},
	},
}

// Ex3StaleReplay — pins ex3's known blind spot: a replayed older frame is accepted and the book
// silently goes backwards. On ex1/ex2 the same replay would be rejected out_of_order, but that
// guard compares event times and ex3's event time is job 1's processing time, which only ever
// moves forward. Nothing on the wire can catch this — a failure here means the guard changed.
var Ex3StaleReplay = Scenario{
	ExchangeID:      3,
	PairID:          1,
	IgnoreEventTime: true,
	Sources: []string{
		// 01 buy depth
		`[
	"BTCUSDT@buyDepth",
	[
		{ "price": 62600.5, "quantity": 0.7, "sum": 43820.35 },
		{ "price": 62599, "quantity": 1.4, "sum": 87638.6 }
	]
]`,
		// 02 buy depth, newer
		`[
	"BTCUSDT@buyDepth",
	[
		{ "price": 62610.5, "quantity": 0.9, "sum": 56349.45 }
	]
]`,
		// 03 the frame from 01 again, replayed after the newer one
		`[
	"BTCUSDT@buyDepth",
	[
		{ "price": 62600.5, "quantity": 0.7, "sum": 43820.35 },
		{ "price": 62599, "quantity": 1.4, "sum": 87638.6 }
	]
]`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 buy depth
			ExchangeID: 3,
			PairID:     1,
			Asks:       []events.PriceLevel{},
			Bids: []events.PriceLevel{
				{Price: "62600.5", Quantity: "0.7"},
				{Price: "62599", Quantity: "1.4"},
			},
		},
		{ // after 02 buy depth
			ExchangeID: 3,
			PairID:     1,
			Asks:       []events.PriceLevel{},
			Bids: []events.PriceLevel{
				{Price: "62610.5", Quantity: "0.9"},
			},
		},
		{ // after 03 replay — back to 01's book, no rejection anywhere
			ExchangeID: 3,
			PairID:     1,
			Asks:       []events.PriceLevel{},
			Bids: []events.PriceLevel{
				{Price: "62600.5", Quantity: "0.7"},
				{Price: "62599", Quantity: "1.4"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 3, Price: "62600.5", Quantity: "0.7"},
			{ExchangeID: 3, Price: "62599", Quantity: "1.4"},
		},
	},
}
