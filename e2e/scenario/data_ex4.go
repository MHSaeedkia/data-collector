// Scenarios for ex4/ramzinex — Centrifugo, full snapshot on every message, numeric market ids.
//
// What makes ex4 different from the other Centrifugo exchanges (ex1, ex2):
//
//   - The channel is `orderbook:{numeric market id}` — the id IS the exchange_markets.market
//     string, so the lookup key is `4|12`, not a symbol. There is no symbol anywhere in the frame.
//   - Sides are named `buys`/`sells`, and BOTH arrive price-DESCENDING, so the best ask is the
//     LAST element. Job 1 keeps wire order and job 5 sorts, which is what these scenarios pin.
//   - Levels are 7-element JSON-NUMBER arrays `[price, qty, notional, false, null, n, millis]`.
//     Only the first two are read; the parser accepts any array of length >= 2.
//   - There is no message-level timestamp on the wire, so job 1 stamps processing time and every
//     scenario here sets IgnoreEventTime — the same as ex3.
//
// Unlike ex3, ex4 DOES have an ordering field: `pub.offset`, carried as sequence_id with jump 0.
// Every frame is a snapshot, so job 2 takes its snapshot branch — `seq <= lastSeq` is rejected
// stale_or_duplicate and any forward offset is accepted, however large. There is no gap rule for
// a snapshot feed, which Ex4StaleOffset pins from both sides.
//
// Pair 1 (market "12", BTC/USDT) is price_precision 2 / quantity_precision 8 with rebase 0/0.
// Pair 2 (market "2", BTC/IRT) rebases -1/0, and pair 17 (market "552", PEPE/USDT) rebases
// -2/+2 at precision 10/10 — ramzinex is the only exchange besides nobitex whose seed carries
// non-identity rebase rows, so it is the only other place job 3 can be asserted.

package scenario

import "orderbook-e2e/events"

// Ex4RamzinexSnapshots — every frame replaces both sides wholesale, and the descending `sells`
// come back out ascending. The last frame also proves elements 3–7 are optional metadata: a bare
// 2-element level array parses exactly like the full 7-element one.
var Ex4RamzinexSnapshots = Scenario{
	ExchangeID:      4,
	PairID:          1,
	IgnoreEventTime: true,
	Sources: []string{
		// 01 snapshot
		`{
	"id": "f7564cd1-109b-490b-b17f-06a02302d2b6",
	"simulation": 1,
	"push": {
		"channel": "orderbook:12",
		"pub": {
			"data": {
				"buys": [
					[62649.5, 0.5, 31324.75, false, null, 65, 1800000000000],
					[62648.25, 1.25, 78310.3125, false, null, 10, 1800000000000],
					[62640, 2, 125280, false, null, 52, 1800000000000]
				],
				"sells": [
					[62660, 0.4, 25064, false, null, 41, 1800000000000],
					[62655.5, 1.5, 93983.25, false, null, 33, 1800000000000],
					[62650.75, 0.25, 15662.6875, false, null, 12, 1800000000000]
				]
			},
			"offset": 5000
		}
	}
}`,
		// 02 snapshot — every 01 level is gone, and a zero-quantity level rests nowhere
		`{
	"id": "39ed78d8-644d-4958-9a72-3ed4e12e44de",
	"simulation": 1,
	"push": {
		"channel": "orderbook:12",
		"pub": {
			"data": {
				"buys": [
					[62651, 0.75, 46988.25, false, null, 22, 1800000000100],
					[62650, 3, 187950, false, null, 18, 1800000000100]
				],
				"sells": [
					[62670, 1, 62670, false, null, 27, 1800000000100],
					[62652.5, 0, 0, false, null, 5, 1800000000100],
					[62651.5, 0.6, 37590.9, false, null, 9, 1800000000100]
				]
			},
			"offset": 5001
		}
	}
}`,
		// 03 snapshot — 2-element levels alongside full 7-element ones
		`{
	"id": "4bc5ce24-5940-4b8d-a190-d6929edf5032",
	"simulation": 1,
	"push": {
		"channel": "orderbook:12",
		"pub": {
			"data": {
				"buys": [
					[62655, 0.8],
					[62654.5, 1.1, 68920.05, false, null, 12, 1800000000200]
				],
				"sells": [
					[62680, 0.9]
				]
			},
			"offset": 5002
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 — sells came in descending, the book is ascending
			ExchangeID: 4,
			PairID:     1,
			Simulation: 1,
			Asks: []events.PriceLevel{
				{Price: "62650.75", Quantity: "0.25"},
				{Price: "62655.5", Quantity: "1.5"},
				{Price: "62660", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62649.5", Quantity: "0.5"},
				{Price: "62648.25", Quantity: "1.25"},
				{Price: "62640", Quantity: "2"},
			},
		},
		{ // after 02 — wholesale replace, and 62652.5 never rested
			ExchangeID: 4,
			PairID:     1,
			Simulation: 1,
			Asks: []events.PriceLevel{
				{Price: "62651.5", Quantity: "0.6"},
				{Price: "62670", Quantity: "1"},
			},
			Bids: []events.PriceLevel{
				{Price: "62651", Quantity: "0.75"},
				{Price: "62650", Quantity: "3"},
			},
		},
		{ // after 03 — the 2-element level parsed like any other
			ExchangeID: 4,
			PairID:     1,
			Simulation: 1,
			Asks: []events.PriceLevel{
				{Price: "62680", Quantity: "0.9"},
			},
			Bids: []events.PriceLevel{
				{Price: "62655", Quantity: "0.8"},
				{Price: "62654.5", Quantity: "1.1"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 4, Simulation: 1, Price: "62680", Quantity: "0.9"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 4, Simulation: 1, Price: "62655", Quantity: "0.8"},
			{ExchangeID: 4, Simulation: 1, Price: "62654.5", Quantity: "1.1"},
		},
	},
}

// Ex4StaleOffset — job 2's snapshot branch from both sides: an offset that does not move forward
// is stale_or_duplicate, and a forward offset is accepted however far it jumps. A snapshot feed
// has no contiguity rule, so the huge jump in 04 must NOT be read as a gap.
var Ex4StaleOffset = Scenario{
	ExchangeID:      4,
	PairID:          1,
	IgnoreEventTime: true,
	Sources: []string{
		// 01 snapshot
		`{
	"id": "ab221072-343d-41f4-a121-af715641c716",
	"simulation": 1,
	"push": {
		"channel": "orderbook:12",
		"pub": {
			"data": {
				"buys": [[62500, 1, 62500, false, null, 20, 1800000000000]],
				"sells": [[62510, 2, 125020, false, null, 20, 1800000000000]]
			},
			"offset": 6000
		}
	}
}`,
		// 02 the same offset again — a duplicate publication
		`{
	"id": "e9deed0d-79fe-4d62-921c-b11acf708900",
	"simulation": 1,
	"push": {
		"channel": "orderbook:12",
		"pub": {
			"data": {
				"buys": [[62498, 1, 62498, false, null, 20, 1800000000100]],
				"sells": [[62512, 2, 125024, false, null, 20, 1800000000100]]
			},
			"offset": 6000
		}
	}
}`,
		// 03 an older offset — a replay that must not overwrite the newer book
		`{
	"id": "675bd23d-8b82-48f5-8096-8838c14f1ff2",
	"simulation": 1,
	"push": {
		"channel": "orderbook:12",
		"pub": {
			"data": {
				"buys": [[62497, 1, 62497, false, null, 20, 1800000000200]],
				"sells": [[62513, 2, 125026, false, null, 20, 1800000000200]]
			},
			"offset": 5999
		}
	}
}`,
		// 04 a far-forward offset — accepted, because a snapshot feed has no gap rule
		`{
	"id": "56d01f82-c05b-4e68-9a03-bd29cf16c965",
	"simulation": 1,
	"push": {
		"channel": "orderbook:12",
		"pub": {
			"data": {
				"buys": [[62520, 0.5, 31260, false, null, 20, 1800000000300]],
				"sells": [[62530, 1.5, 93795, false, null, 20, 1800000000300]]
			},
			"offset": 9999
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 4,
			PairID:     1,
			Simulation: 1,
			Asks:       []events.PriceLevel{{Price: "62510", Quantity: "2"}},
			Bids:       []events.PriceLevel{{Price: "62500", Quantity: "1"}},
		},
		{ // after 04 — 02 and 03 were dead-lettered, so the book skipped straight here
			ExchangeID: 4,
			PairID:     1,
			Simulation: 1,
			Asks:       []events.PriceLevel{{Price: "62530", Quantity: "1.5"}},
			Bids:       []events.PriceLevel{{Price: "62520", Quantity: "0.5"}},
		},
	},
	WantRejects: []string{"stale_or_duplicate", "stale_or_duplicate"},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{{ExchangeID: 4, Simulation: 1, Price: "62530", Quantity: "1.5"}},
		Bids: []events.AggregatedLevel{{ExchangeID: 4, Simulation: 1, Price: "62520", Quantity: "0.5"}},
	},
}

// Ex4NoiseFrames — everything that is not a Centrifugo orderbook publication for a known numeric
// market is dropped by job 1 without a dead-letter and without touching the book.
var Ex4NoiseFrames = Scenario{
	ExchangeID:      4,
	PairID:          1,
	IgnoreEventTime: true,
	Sources: []string{
		// 01 snapshot
		`{
	"id": "eb7da6cf-eb8f-4123-b167-a844d3fcc81a",
	"simulation": 1,
	"push": {
		"channel": "orderbook:12",
		"pub": {
			"data": {
				"buys": [[62400, 0.3, 18720, false, null, 20, 1800000000000]],
				"sells": [[62410, 1.1, 68651, false, null, 20, 1800000000000]]
			},
			"offset": 7000
		}
	}
}`,
		// 02 not a publication at all
		`{
	"id": "26e61fa7-58be-41cc-aeb4-95b3d83194d5", "simulation": 1, "ping": 1 }`,
		// 03 a publication, but not the orderbook channel
		`{
	"id": "908c49dd-9c9d-422c-be08-309ee0d41cdd",
	"simulation": 1,
	"push": {
		"channel": "trades:12",
		"pub": {
			"data": {
				"buys": [[62400, 0.3, 18720, false, null, 20, 1800000000100]],
				"sells": [[62410, 1.1, 68651, false, null, 20, 1800000000100]]
			},
			"offset": 7001
		}
	}
}`,
		// 04 no sells key — a half book is not a shape ex4 ever sends
		`{
	"id": "dc52179b-8de3-4d47-bd55-ce1516082873",
	"simulation": 1,
	"push": {
		"channel": "orderbook:12",
		"pub": {
			"data": {
				"buys": [[62400, 0.3, 18720, false, null, 20, 1800000000200]]
			},
			"offset": 7002
		}
	}
}`,
		// 05 a market id ex4 has no exchange_markets row for
		`{
	"id": "629289e1-0a5e-4256-ae4b-17866dedec99",
	"simulation": 1,
	"push": {
		"channel": "orderbook:99999",
		"pub": {
			"data": {
				"buys": [[1.5, 10, 15, false, null, 20, 1800000000300]],
				"sells": [[1.6, 10, 16, false, null, 20, 1800000000300]]
			},
			"offset": 7003
		}
	}
}`,
		// 06 string levels — ex4's wire is JSON numbers, so the whole frame is unparseable
		`{
	"id": "1e4c6c97-fa6f-409c-97e4-d66415185ef9",
	"simulation": 1,
	"push": {
		"channel": "orderbook:12",
		"pub": {
			"data": {
				"buys": [["62400", "0.3"]],
				"sells": [["62410", "1.1"]]
			},
			"offset": 7004
		}
	}
}`,
		// 07 a 1-element level — below the [price, qty] minimum, so the frame is dropped whole
		`{
	"id": "c703f400-dad2-407b-8228-5b24e983a3d0",
	"simulation": 1,
	"push": {
		"channel": "orderbook:12",
		"pub": {
			"data": {
				"buys": [[62400]],
				"sells": [[62410, 1.1]]
			},
			"offset": 7005
		}
	}
}`,
		// 08 snapshot
		`{
	"id": "250a5561-69be-4894-93c5-4a72d9b686d4",
	"simulation": 1,
	"push": {
		"channel": "orderbook:12",
		"pub": {
			"data": {
				"buys": [
					[62400, 0.3, 18720, false, null, 20, 1800000000600],
					[62399, 1.2, 74878.8, false, null, 20, 1800000000600]
				],
				"sells": [
					[62411, 0.9, 56169.9, false, null, 20, 1800000000600],
					[62410.5, 0.6, 37446.3, false, null, 20, 1800000000600]
				]
			},
			"offset": 7006
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 4,
			PairID:     1,
			Simulation: 1,
			Asks:       []events.PriceLevel{{Price: "62410", Quantity: "1.1"}},
			Bids:       []events.PriceLevel{{Price: "62400", Quantity: "0.3"}},
		},
		{ // after 08 — 02 through 07 emitted nothing at all
			ExchangeID: 4,
			PairID:     1,
			Simulation: 1,
			Asks: []events.PriceLevel{
				{Price: "62410.5", Quantity: "0.6"},
				{Price: "62411", Quantity: "0.9"},
			},
			Bids: []events.PriceLevel{
				{Price: "62400", Quantity: "0.3"},
				{Price: "62399", Quantity: "1.2"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 4, Simulation: 1, Price: "62410.5", Quantity: "0.6"},
			{ExchangeID: 4, Simulation: 1, Price: "62411", Quantity: "0.9"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 4, Simulation: 1, Price: "62400", Quantity: "0.3"},
			{ExchangeID: 4, Simulation: 1, Price: "62399", Quantity: "1.2"},
		},
	},
}

// Ex4RebaseToman — ramzinex quotes BTC/IRT in rials and we store tomans, so market "2" carries
// price_amount_rebase -1. The wire prices are chosen so the shift lands a third decimal on a
// 2-place market: that only truncates away if job 3 runs BEFORE job 4, which is what pins the
// order of the two jobs. Frame 02 adds the other consequence — two rial prices that were distinct
// on the wire collide after the shift and merge into one level with their quantities summed.
var Ex4RebaseToman = Scenario{
	ExchangeID:      4,
	PairID:          2,
	IgnoreEventTime: true,
	Sources: []string{
		// 01 snapshot, prices in rials
		`{
	"id": "31f71387-a263-4169-98c7-a8af1dd9babf",
	"simulation": 1,
	"push": {
		"channel": "orderbook:2",
		"pub": {
			"data": {
				"buys": [
					[38523456.55, 0.5, 19261728.275, false, null, 31, 1800000000000],
					[38520000, 1.25, 48150000, false, null, 24, 1800000000000],
					[38510900, 2, 77021800, false, null, 17, 1800000000000]
				],
				"sells": [
					[38541000, 0.8, 30832800, false, null, 44, 1800000000000],
					[38530559, 1.5, 57795838.5, false, null, 39, 1800000000000]
				]
			},
			"offset": 8000
		}
	}
}`,
		// 02 snapshot — the first two buys are distinct rials but the same toman
		`{
	"id": "75d81243-d741-45fb-a05c-0749267f3b7a",
	"simulation": 1,
	"push": {
		"channel": "orderbook:2",
		"pub": {
			"data": {
				"buys": [
					[38523456.59, 0.25, 9630864.1475, false, null, 8, 1800000000100],
					[38523456.55, 0.4, 15409382.62, false, null, 12, 1800000000100],
					[38519999, 1.5, 57779998.5, false, null, 21, 1800000000100]
				],
				"sells": [
					[38560000, 1, 38560000, false, null, 30, 1800000000100],
					[38550005, 0.000000009, 0.346950045, false, null, 3, 1800000000100]
				]
			},
			"offset": 8001
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 — 38523456.55 rials is 3852345.655 tomans, truncated DOWN to 2 places
			ExchangeID: 4,
			PairID:     2,
			Simulation: 1,
			Asks: []events.PriceLevel{
				{Price: "3853055.9", Quantity: "1.5"},
				{Price: "3854100", Quantity: "0.8"},
			},
			Bids: []events.PriceLevel{
				{Price: "3852345.65", Quantity: "0.5"},
				{Price: "3852000", Quantity: "1.25"},
				{Price: "3851090", Quantity: "2"},
			},
		},
		{ // after 02 — the two colliding buys merged at 3852345.65 with 0.4 + 0.25 = 0.65,
			// and the 9e-9 ask quantity truncated to zero so that level never rested
			ExchangeID: 4,
			PairID:     2,
			Simulation: 1,
			Asks: []events.PriceLevel{
				{Price: "3856000", Quantity: "1"},
			},
			Bids: []events.PriceLevel{
				{Price: "3852345.65", Quantity: "0.65"},
				{Price: "3851999.9", Quantity: "1.5"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{{ExchangeID: 4, Simulation: 1, Price: "3856000", Quantity: "1"}},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 4, Simulation: 1, Price: "3852345.65", Quantity: "0.65"},
			{ExchangeID: 4, Simulation: 1, Price: "3851999.9", Quantity: "1.5"},
		},
	},
}

// Ex4RebaseScaledUnit — market "552" is ramzinex's PEPE/USDT, quoted per 100 PEPE: price shifts
// by -2 and quantity by +2. It is the mirror image of the dust rule everywhere else in this
// suite — here the rebase SAVES a quantity that would have truncated away un-rebased (5e-12 of a
// 100-PEPE unit is 5e-10 whole PEPE, which survives at 10 places), while a hundredth of that
// still dies. Both only work if job 3 runs before job 4.
var Ex4RebaseScaledUnit = Scenario{
	ExchangeID:      4,
	PairID:          17,
	IgnoreEventTime: true,
	Sources: []string{
		// 01 snapshot, priced per 100 PEPE
		`{
	"id": "f6534273-7ba5-4322-900a-1e8f5d30a071",
	"simulation": 1,
	"push": {
		"channel": "orderbook:552",
		"pub": {
			"data": {
				"buys": [
					[0.00123456, 5000, 6.1728, false, null, 61, 1800000000000],
					[0.00123, 12000, 14.76, false, null, 48, 1800000000000]
				],
				"sells": [
					[0.001245, 8000, 9.96, false, null, 52, 1800000000000],
					[0.00124, 3000, 3.72, false, null, 37, 1800000000000]
				]
			},
			"offset": 8500
		}
	}
}`,
		// 02 snapshot — a price with more places than the market keeps, and two dust quantities
		// either side of the line the +2 shift moves
		`{
	"id": "579c1c82-c89e-4cb8-aafd-ac5c57524b5b",
	"simulation": 1,
	"push": {
		"channel": "orderbook:552",
		"pub": {
			"data": {
				"buys": [
					[0.001234567890123, 4000, 4.938271560492, false, null, 14, 1800000000100],
					[0.00123, 6000, 7.38, false, null, 11, 1800000000100]
				],
				"sells": [
					[0.00127, 0.00000000000005, 0.000000000000000635, false, null, 2, 1800000000100],
					[0.00126, 0.000000000005, 0.0000000000000063, false, null, 4, 1800000000100]
				]
			},
			"offset": 8501
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 — price / 100, quantity * 100
			ExchangeID: 4,
			PairID:     17,
			Simulation: 1,
			Asks: []events.PriceLevel{
				{Price: "0.0000124", Quantity: "300000"},
				{Price: "0.00001245", Quantity: "800000"},
			},
			Bids: []events.PriceLevel{
				{Price: "0.0000123456", Quantity: "500000"},
				{Price: "0.0000123", Quantity: "1200000"},
			},
		},
		{ // after 02 — 0.00001234567890123 truncates to 10 places; 5e-14 * 100 is still below
			// the lot precision and was deleted, 5e-12 * 100 is exactly at it and rests
			ExchangeID: 4,
			PairID:     17,
			Simulation: 1,
			Asks: []events.PriceLevel{
				{Price: "0.0000126", Quantity: "0.0000000005"},
			},
			Bids: []events.PriceLevel{
				{Price: "0.0000123456", Quantity: "400000"},
				{Price: "0.0000123", Quantity: "600000"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 4, Simulation: 1, Price: "0.0000126", Quantity: "0.0000000005"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 4, Simulation: 1, Price: "0.0000123456", Quantity: "400000"},
			{ExchangeID: 4, Simulation: 1, Price: "0.0000123", Quantity: "600000"},
		},
	},
}
