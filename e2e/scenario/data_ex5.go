// Scenarios for ex5/bitget — an explicit `action: "snapshot"` discriminator on a snapshot-only
// feed, in bitget's own `action`/`arg`/`data`-array envelope.
//
// What makes ex5 different:
//
//   - `data` is an ARRAY of book objects, not a single object. The parser emits one event per
//     element, so ONE Kafka record can produce SEVERAL snapshots (Ex5MultiBookFrame). If any
//     element is malformed the WHOLE record is dropped, including elements already read —
//     accuracy-first, never emit a partial book.
//   - `action` is the only accepted value `"snapshot"`; bitget sends no deltas here, so any other
//     action is noise and is dropped rather than treated as an update.
//   - The ordering field is `data[i].seq` (an integral JSON number) with jump 0, and the event
//     time is `data[i].ts` — a STRING of epoch millis, unlike the outer `ts` which is a number
//     and is ignored entirely.
//
// Because every frame is a snapshot with a sequence, job 2 takes its snapshot branch: `seq <=
// lastSeq` is stale_or_duplicate and any forward seq is accepted, with no contiguity rule. The
// non-monotonic `seq` seen in the real capture is exactly the out-of-order arrival that branch
// exists to drop (Ex5StaleSeq).
//
// Every bitget market in the seed is a USDT market with rebase 0/0, so job 3 is the identity here
// and there is nothing to assert about it — ex1 and ex4 are the only two exchanges that can.
// Pair 1 (BTCUSDT) is price_precision 2 / quantity_precision 8.

package scenario

import "orderbook-e2e/events"

// Ex5BitgetSnapshots — the happy path: each frame replaces both sides wholesale, the event time
// comes off the STRING `ts` inside the book object, and a zero quantity rests nowhere.
var Ex5BitgetSnapshots = Scenario{
	ExchangeID: 5,
	PairID:     1,
	Sources: []string{
		// 01 snapshot
		`{
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{
			"asks": [["62815", "0.021591"], ["62815.9", "0.001"], ["62817.32", "0.015919"]],
			"bids": [["62814.99", "6.180672"], ["62814.77", "0.1612"], ["62814.23", "0.910845"]],
			"ts": "1800000000000",
			"seq": 655666926391,
			"pseq": 0
		}
	],
	"ts": 1800000000005
}`,
		// 02 snapshot — a price that truncates onto a trailing zero, and a zero quantity
		`{
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{
			"asks": [["62820", "1.5"], ["62821.005", "0.5"], ["62822", "0"]],
			"bids": [["62819.999", "2"], ["62818", "0.75"]],
			"ts": "1800000001000",
			"seq": 655666926400,
			"pseq": 655666926391
		}
	],
	"ts": 1800000001005
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 5,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62815", Quantity: "0.021591"},
				{Price: "62815.9", Quantity: "0.001"},
				{Price: "62817.32", Quantity: "0.015919"},
			},
			Bids: []events.PriceLevel{
				{Price: "62814.99", Quantity: "6.180672"},
				{Price: "62814.77", Quantity: "0.1612"},
				{Price: "62814.23", Quantity: "0.910845"},
			},
		},
		{ // after 02 — 62821.005 truncates to 62821.00 and canonicalizes to 62821; 62822 never rested
			ExchangeID: 5,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "62820", Quantity: "1.5"},
				{Price: "62821", Quantity: "0.5"},
			},
			Bids: []events.PriceLevel{
				{Price: "62819.99", Quantity: "2"},
				{Price: "62818", Quantity: "0.75"},
			},
		},
	},
}

// Ex5MultiBookFrame — one Kafka record whose `data` array carries two book objects becomes two
// independent snapshots, each with its own seq and event time. No other exchange in the suite can
// fan one record out into several events, so this is the only place that wiring is exercised.
var Ex5MultiBookFrame = Scenario{
	ExchangeID: 5,
	PairID:     1,
	Sources: []string{
		// 01 one book
		`{
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{
			"asks": [["62900", "1"]],
			"bids": [["62899", "2"]],
			"ts": "1800000000000",
			"seq": 100,
			"pseq": 0
		}
	],
	"ts": 1800000000005
}`,
		// 02 two books in one record
		`{
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{
			"asks": [["62901", "0.5"]],
			"bids": [["62898", "1.5"]],
			"ts": "1800000001000",
			"seq": 101,
			"pseq": 100
		},
		{
			"asks": [["62902", "0.25"]],
			"bids": [["62897", "3"]],
			"ts": "1800000002000",
			"seq": 102,
			"pseq": 101
		}
	],
	"ts": 1800000002005
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 5,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{{Price: "62900", Quantity: "1"}},
			Bids:       []events.PriceLevel{{Price: "62899", Quantity: "2"}},
		},
		{ // after the first book of 02
			ExchangeID: 5,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks:       []events.PriceLevel{{Price: "62901", Quantity: "0.5"}},
			Bids:       []events.PriceLevel{{Price: "62898", Quantity: "1.5"}},
		},
		{ // after the second book of 02 — same record, its own snapshot
			ExchangeID: 5,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks:       []events.PriceLevel{{Price: "62902", Quantity: "0.25"}},
			Bids:       []events.PriceLevel{{Price: "62897", Quantity: "3"}},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{{ExchangeID: 5, Price: "62902", Quantity: "0.25"}},
		Bids: []events.AggregatedLevel{{ExchangeID: 5, Price: "62897", Quantity: "3"}},
	},
}

// Ex5StaleSeq — the non-monotonic `seq` the real capture showed is out-of-order arrival, and job 2
// drops it. 04 pins the other half of the snapshot branch: a forward seq is accepted however far
// it jumps, because a snapshot feed has no contiguity rule to break.
var Ex5StaleSeq = Scenario{
	ExchangeID: 5,
	PairID:     1,
	Sources: []string{
		// 01 snapshot
		`{
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{ "asks": [["62700", "1"]], "bids": [["62699", "2"]], "ts": "1800000000000", "seq": 500, "pseq": 0 }
	],
	"ts": 1800000000005
}`,
		// 02 the same seq again
		`{
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{ "asks": [["62701", "1"]], "bids": [["62698", "2"]], "ts": "1800000000100", "seq": 500, "pseq": 0 }
	],
	"ts": 1800000000105
}`,
		// 03 an older seq
		`{
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{ "asks": [["62702", "1"]], "bids": [["62697", "2"]], "ts": "1800000000200", "seq": 499, "pseq": 0 }
	],
	"ts": 1800000000205
}`,
		// 04 a far-forward seq
		`{
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{ "asks": [["62710", "0.5"]], "bids": [["62690", "1.5"]], "ts": "1800000001000", "seq": 900000, "pseq": 0 }
	],
	"ts": 1800000001005
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 5,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{{Price: "62700", Quantity: "1"}},
			Bids:       []events.PriceLevel{{Price: "62699", Quantity: "2"}},
		},
		{ // after 04 — 02 and 03 were dead-lettered before the book builder saw them
			ExchangeID: 5,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks:       []events.PriceLevel{{Price: "62710", Quantity: "0.5"}},
			Bids:       []events.PriceLevel{{Price: "62690", Quantity: "1.5"}},
		},
	},
	WantRejects: []string{"stale_or_duplicate", "stale_or_duplicate"},
}

// Ex5NoiseFrames — the parser's whitelist is strict about wire TYPES, not just shape: `seq` must
// be an integral number and `ts` must be a string, so a frame that swaps them is dropped even
// though a lenient parse would have produced the same values.
var Ex5NoiseFrames = Scenario{
	ExchangeID: 5,
	PairID:     1,
	Sources: []string{
		// 01 snapshot
		`{
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{ "asks": [["62600", "1"]], "bids": [["62599", "2"]], "ts": "1800000000000", "seq": 300, "pseq": 0 }
	],
	"ts": 1800000000005
}`,
		// 02 an action bitget does not send on this channel
		`{
	"action": "update",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{ "asks": [["62601", "1"]], "bids": [["62598", "2"]], "ts": "1800000000100", "seq": 301, "pseq": 300 }
	],
	"ts": 1800000000105
}`,
		// 03 seq as a string
		`{
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{ "asks": [["62602", "1"]], "bids": [["62597", "2"]], "ts": "1800000000200", "seq": "301", "pseq": 300 }
	],
	"ts": 1800000000205
}`,
		// 04 ts as a number — the outer ts is one, the inner one never is
		`{
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{ "asks": [["62603", "1"]], "bids": [["62596", "2"]], "ts": 1800000000300, "seq": 302, "pseq": 300 }
	],
	"ts": 1800000000305
}`,
		// 05 data as an object rather than the array it always is
		`{
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": { "asks": [["62604", "1"]], "bids": [["62595", "2"]], "ts": "1800000000400", "seq": 303, "pseq": 300 },
	"ts": 1800000000405
}`,
		// 06 no instId, so there is no market key
		`{
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50" },
	"data": [
		{ "asks": [["62605", "1"]], "bids": [["62594", "2"]], "ts": "1800000000500", "seq": 304, "pseq": 300 }
	],
	"ts": 1800000000505
}`,
		// 07 a market ex5 has no exchange_markets row for
		`{
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "FOOBARUSDT" },
	"data": [
		{ "asks": [["1.5", "10"]], "bids": [["1.4", "10"]], "ts": "1800000000600", "seq": 305, "pseq": 300 }
	],
	"ts": 1800000000605
}`,
		// 08 numeric levels — ex5's wire is string pairs, so the whole frame is unparseable
		`{
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{ "asks": [[62606, 1]], "bids": [[62593, 2]], "ts": "1800000000700", "seq": 306, "pseq": 300 }
	],
	"ts": 1800000000705
}`,
		// 09 snapshot
		`{
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{
			"asks": [["62601", "0.4"], ["62605", "1.1"]],
			"bids": [["62598", "0.9"]],
			"ts": "1800000001000",
			"seq": 301,
			"pseq": 300
		}
	],
	"ts": 1800000001005
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 5,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{{Price: "62600", Quantity: "1"}},
			Bids:       []events.PriceLevel{{Price: "62599", Quantity: "2"}},
		},
		{ // after 09 — 02 through 08 emitted nothing, so seq 301 is still forward of 300
			ExchangeID: 5,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "62601", Quantity: "0.4"},
				{Price: "62605", Quantity: "1.1"},
			},
			Bids: []events.PriceLevel{{Price: "62598", Quantity: "0.9"}},
		},
	},
}

// Ex5PrecisionDust — job 4 on a string-pair wire: prices truncate DOWN to the market's 2 places,
// colliding ones merge with their quantities summed BEFORE the quantity is truncated, and a
// quantity under the market's 8 places becomes "0", which job 5 reads as "no level here".
var Ex5PrecisionDust = Scenario{
	ExchangeID: 5,
	PairID:     1,
	Sources: []string{
		// 01 snapshot — two asks collide at 2 places, and both bids do too
		`{
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{
			"asks": [["62700.117", "0.3"], ["62700.119", "0.2"], ["62701.50", "0.000000004"]],
			"bids": [["62699.99999", "0.12345678999"], ["62699.999", "1.5"]],
			"ts": "1800000000000",
			"seq": 700,
			"pseq": 0
		}
	],
	"ts": 1800000000005
}`,
		// 02 snapshot — every ask is dust, so the side comes out empty
		`{
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{
			"asks": [["62702.001", "0.000000009"]],
			"bids": [["62698.5", "0.4"]],
			"ts": "1800000001000",
			"seq": 701,
			"pseq": 700
		}
	],
	"ts": 1800000001005
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 — .117 and .119 merged at .11 with 0.5; the two bids merged at 62699.99 and
			// their exact sum 1.62345678999 truncated once, to 1.62345678
			ExchangeID: 5,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{{Price: "62700.11", Quantity: "0.5"}},
			Bids:       []events.PriceLevel{{Price: "62699.99", Quantity: "1.62345678"}},
		},
		{ // after 02 — the only ask truncated to zero quantity, so nothing rests on that side
			ExchangeID: 5,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks:       []events.PriceLevel{},
			Bids:       []events.PriceLevel{{Price: "62698.5", Quantity: "0.4"}},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{},
		Bids: []events.AggregatedLevel{{ExchangeID: 5, Price: "62698.5", Quantity: "0.4"}},
	},
}
