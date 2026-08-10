// Scenarios for ex3/wallex — one side per message, no ordering field, no timestamp.
//
// What makes ex3 different from every other exchange in the suite:
//
//   - The envelope is an array `["{market}@{side}", [levels…], {"simulation": N, "id": "…"}]`;
//     buyDepth is bids, sellDepth is asks, and the side that is not in the message stays NULL —
//     "no report for this side", which job 5 must leave alone even though the event is a snapshot.
//   - Levels are objects with JSON-NUMBER price/quantity (every other exchange sends strings),
//     so the values come off the wire as BigDecimal-from-literal.
//   - It is the one exchange whose NiFi-injected fields are NOT root fields: an array has no root
//     fields, so NiFi appends that trailing third element instead, and BOTH `simulation` and `id`
//     ride in it. The parser reads element INDEX 2, so it is never a fourth element — a 4-element
//     frame is malformed and dropped. A 2-element frame parses as simulation 0, but it carries no
//     id either, so job 1 drops it anyway.
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
	],
	{ "simulation": 1, "id": "b21c0549-daa7-49c5-a4eb-049fe2fee0d1" }
]`,
		// 02 sell depth
		`[
	"BTCUSDT@sellDepth",
	[
		{ "price": 62952.5, "quantity": 0.7, "sum": 44066.75 },
		{ "price": 62957.5, "quantity": 1.4, "sum": 88140.5 },
		{ "price": 62962.5, "quantity": 2.2, "sum": 138517.5 }
	],
	{ "simulation": 1, "id": "70875639-8705-4ee4-8076-3bbf0cc656bc" }
]`,
		// 03 buy depth refresh
		`[
	"BTCUSDT@buyDepth",
	[
		{ "price": 62942.5, "quantity": 0.5, "sum": 31471.25 },
		{ "price": 62927.5, "quantity": 3.5, "sum": 220246.25 }
	],
	{ "simulation": 1, "id": "1be97642-314f-4a2f-ae5b-ff4a1846cd5f" }
]`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 buy depth
			ExchangeID: 3,
			PairID:     1,
			Simulation: 1,
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
			Simulation: 1,
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
			Simulation: 1,
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
			{ExchangeID: 3, Simulation: 1, Price: "62952.5", Quantity: "0.7"},
			{ExchangeID: 3, Simulation: 1, Price: "62957.5", Quantity: "1.4"},
			{ExchangeID: 3, Simulation: 1, Price: "62962.5", Quantity: "2.2"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 3, Simulation: 1, Price: "62942.5", Quantity: "0.5"},
			{ExchangeID: 3, Simulation: 1, Price: "62927.5", Quantity: "3.5"},
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
	],
	{ "simulation": 1, "id": "501973ae-fb4e-45b6-ae49-661416b3eaed" }
]`,
		// 02 sell depth
		`[
	"BTCUSDT@sellDepth",
	[
		{ "price": 62501.75, "quantity": 0.4, "sum": 25000.7 },
		{ "price": 62502, "quantity": 2, "sum": 125004 }
	],
	{ "simulation": 1, "id": "b9f27498-db82-41c1-8284-d51d8fed045f" }
]`,
		// 03 buy depth, empty — wallex reporting no bids at all
		`[
	"BTCUSDT@buyDepth",
	[],
	{ "simulation": 1, "id": "fb8a5c6d-8c74-4f85-8caf-005a3206a37c" }
]`,
		// 04 sell depth carrying a zero-quantity level
		`[
	"BTCUSDT@sellDepth",
	[
		{ "price": 62503.5, "quantity": 0, "sum": 0 },
		{ "price": 62504, "quantity": 1.5, "sum": 93756 }
	],
	{ "simulation": 1, "id": "46a37566-1d97-432a-9aa0-39da159bc4da" }
]`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 buy depth
			ExchangeID: 3,
			PairID:     1,
			Simulation: 1,
			Asks:       []events.PriceLevel{},
			Bids: []events.PriceLevel{
				{Price: "62500.25", Quantity: "0.5"},
				{Price: "62499.5", Quantity: "1.25"},
			},
		},
		{ // after 02 sell depth
			ExchangeID: 3,
			PairID:     1,
			Simulation: 1,
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
			Simulation: 1,
			Asks: []events.PriceLevel{
				{Price: "62501.75", Quantity: "0.4"},
				{Price: "62502", Quantity: "2"},
			},
			Bids: []events.PriceLevel{},
		},
		{ // after 04 sell depth with a zero level — the side is replaced, the zero rests nowhere
			ExchangeID: 3,
			PairID:     1,
			Simulation: 1,
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
			{ExchangeID: 3, Simulation: 1, Price: "62504", Quantity: "1.5"},
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
	],
	{ "simulation": 1, "id": "12c14929-4c00-4a33-a442-0bb898be205d" }
]`,
		// 02 sell depth — the first quantity is dust below the market's lot precision
		`[
	"BTCUSDT@sellDepth",
	[
		{ "price": 62501.01, "quantity": 0.000000005, "sum": 0.00031250505 },
		{ "price": 62502.5, "quantity": 1.000000009, "sum": 62502.5005625225 }
	],
	{ "simulation": 1, "id": "36a29e66-f772-4c79-87a3-b3efe7e53f3b" }
]`,
		// 03 buy depth — an integer price literal, and truncation that must not round up
		`[
	"BTCUSDT@buyDepth",
	[
		{ "price": 62200, "quantity": 0.068493, "sum": 4260.2646 },
		{ "price": 62199.999, "quantity": 0.123456789, "sum": 7679.012152343211 }
	],
	{ "simulation": 1, "id": "3e1d3194-d78f-45b3-b60a-1b8f761be5ab" }
]`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 buy depth — .123 and .129 merged at .12, quantities summed to 0.65
			ExchangeID: 3,
			PairID:     1,
			Simulation: 1,
			Asks:       []events.PriceLevel{},
			Bids: []events.PriceLevel{
				{Price: "62500.12", Quantity: "0.65"},
				{Price: "62499.99", Quantity: "1.5"},
			},
		},
		{ // after 02 sell depth — the dust level truncated to 0 and was deleted, not rested
			ExchangeID: 3,
			PairID:     1,
			Simulation: 1,
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
			Simulation: 1,
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
		Asks: []events.AggregatedLevel{{ExchangeID: 3, Simulation: 1, Price: "62502.5", Quantity: "1"}},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 3, Simulation: 1, Price: "62200", Quantity: "0.068493"},
			{ExchangeID: 3, Simulation: 1, Price: "62199.99", Quantity: "0.12345678"},
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
	],
	{ "simulation": 1, "id": "a86e505a-5c9f-4037-97a6-982dc8c23458" }
]`,
		// 02 not the array envelope at all
		`{ "id": "2963bdcc-d075-4905-9a86-9a414093b928", "simulation": 1, "ping": 1 }`,
		// 03 a known market, but not a depth channel
		`[
	"BTCUSDT@trades",
	[
		{ "price": 62450.5, "quantity": 0.3, "sum": 18735.15 }
	],
	{ "simulation": 1, "id": "6a00d427-e768-42f8-88eb-951d04b33eef" }
]`,
		// 04 no @ in the key, so there is no side to read
		`[
	"BTCUSDT",
	[
		{ "price": 62450.5, "quantity": 0.3, "sum": 18735.15 }
	],
	{ "simulation": 1, "id": "36f6a3c2-8981-4f15-9c73-8d86e7d592c9" }
]`,
		// 05 a FOURTH element the envelope does not have — the third is now the
		// simulation metadata object, so only a longer array is still malformed
		`[
	"BTCUSDT@buyDepth",
	[
		{ "price": 62450.5, "quantity": 0.3, "sum": 18735.15 }
	],
	{ "simulation": 1, "id": "60706f52-8c44-45ae-bc5e-b7bdb2b582df" },
	"extra"
]`,
		// 06 a market ex3 has no exchange_markets row for
		`[
	"FOOBARUSDT@buyDepth",
	[
		{ "price": 1.5, "quantity": 10, "sum": 15 }
	],
	{ "simulation": 1, "id": "ddac82cd-c118-4cba-b491-748498fe6ad3" }
]`,
		// 07 string levels — ex3's wire is JSON numbers, so the whole frame is unparseable
		`[
	"BTCUSDT@sellDepth",
	[
		{ "price": "62451", "quantity": "0.9" }
	],
	{ "simulation": 1, "id": "513600e5-cab3-4dcb-bc05-a47f30faf9ef" }
]`,
		// 08 sell depth
		`[
	"BTCUSDT@sellDepth",
	[
		{ "price": 62451, "quantity": 0.9, "sum": 56205.9 },
		{ "price": 62452.25, "quantity": 0.6, "sum": 37471.35 }
	],
	{ "simulation": 1, "id": "5915f45a-7e36-43c2-af31-5829039050b5" }
]`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 buy depth
			ExchangeID: 3,
			PairID:     1,
			Simulation: 1,
			Asks:       []events.PriceLevel{},
			Bids: []events.PriceLevel{
				{Price: "62450.5", Quantity: "0.3"},
				{Price: "62449", Quantity: "1.1"},
			},
		},
		{ // after 08 sell depth — 02 through 07 emitted nothing, and the bids are still 01's
			ExchangeID: 3,
			PairID:     1,
			Simulation: 1,
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
			{ExchangeID: 3, Simulation: 1, Price: "62451", Quantity: "0.9"},
			{ExchangeID: 3, Simulation: 1, Price: "62452.25", Quantity: "0.6"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 3, Simulation: 1, Price: "62450.5", Quantity: "0.3"},
			{ExchangeID: 3, Simulation: 1, Price: "62449", Quantity: "1.1"},
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
	],
	{ "simulation": 1, "id": "1867a03a-c5bb-42a1-9388-9e7e91768a10" }
]`,
		// 02 buy depth, newer
		`[
	"BTCUSDT@buyDepth",
	[
		{ "price": 62610.5, "quantity": 0.9, "sum": 56349.45 }
	],
	{ "simulation": 1, "id": "62a0b462-c23a-453f-97ef-24f375f6c661" }
]`,
		// 03 the frame from 01 again, replayed after the newer one
		`[
	"BTCUSDT@buyDepth",
	[
		{ "price": 62600.5, "quantity": 0.7, "sum": 43820.35 },
		{ "price": 62599, "quantity": 1.4, "sum": 87638.6 }
	],
	{ "simulation": 1, "id": "3d371f44-084f-4213-97ce-20207ce5615e" }
]`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 buy depth
			ExchangeID: 3,
			PairID:     1,
			Simulation: 1,
			Asks:       []events.PriceLevel{},
			Bids: []events.PriceLevel{
				{Price: "62600.5", Quantity: "0.7"},
				{Price: "62599", Quantity: "1.4"},
			},
		},
		{ // after 02 buy depth
			ExchangeID: 3,
			PairID:     1,
			Simulation: 1,
			Asks:       []events.PriceLevel{},
			Bids: []events.PriceLevel{
				{Price: "62610.5", Quantity: "0.9"},
			},
		},
		{ // after 03 replay — back to 01's book, no rejection anywhere
			ExchangeID: 3,
			PairID:     1,
			Simulation: 1,
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
			{ExchangeID: 3, Simulation: 1, Price: "62600.5", Quantity: "0.7"},
			{ExchangeID: 3, Simulation: 1, Price: "62599", Quantity: "1.4"},
		},
	},
}
