// Scenarios for ex7/ompfinex — a REST snapshot with a real lastUpdateId, plus
// Centrifugo WS deltas carrying a Binance-style u/U range on channel
// "public-market:r-depth-{market}". The conventions these follow are in data.go.
//
// Confirmed from OmpfinexParser (io.tibobit.normalizer.pairextract.parser):
//   - The market comes from the channel suffix on updates
//     ("public-market:r-depth-14" -> market "14") and from the top-level
//     "pair" field on snapshots. They must agree, or the two streams route to
//     different pairs (or nowhere, if the suffix does not resolve at all — a
//     silent job-1 drop, not a dead-letter).
//   - Continuity is U_n == seq_{n-1} (NOT seq_{n-1}+1): a delta's U must equal
//     the previous event's seq (the snapshot's lastUpdateId, or the previous
//     delta's u). A break is reject_reason "sequence_gap".
//   - Deltas carry no wire timestamp; job 1 stamps processing time, so any
//     snapshot produced by a delta has a wall-clock, unassertable EventTime.
//     Every scenario that includes a delta therefore sets IgnoreEventTime and
//     blanks EventTime on every WantSnapshots entry, including the ones that
//     came from the REST snapshot — the flag is all-or-nothing per scenario.
//   - REST snapshot "time" is epoch MICROSECONDS on the wire (divided by 1000
//     for the platform's millis event time) — confirmed against a live sample.

package scenario

import "orderbook-e2e/events"

// Ex7RestThenWsUpdates — a REST snapshot establishes lastUpdateId, and two
// contiguous WS deltas (U == previous seq each time) apply cleanly.
var Ex7RestThenWsUpdates = Scenario{
	ExchangeID:      7,
	PairID:          1,
	IgnoreEventTime: true, // deltas carry no wire timestamp; see file header
	Sources: []string{
		// 01 rest snapshot, lastUpdateId 100000
		`{
	"id": "5f6b1e4a-2c9d-4b3a-8e7f-1a2b3c4d5e6f",
	"simulation": 1,
	"status": "OK",
	"action": "snapshot",
	"pair": "14",
	"data": {
		"lastUpdateId": 100000,
		"time": 1800000000000000,
		"bids": [
			["62649", "0.50000000"],
			["62648", "0.02744953"],
			["62640", "1.31062803"]
		],
		"asks": [
			["62650", "2.21924167"],
			["62651", "0.17447383"],
			["62660", "0.33476925"]
		]
	}
}`,
		// 02 ws update — U == 100000 (previous seq, the snapshot's lastUpdateId), u == 100001
		`{
	"id": "a1b2c3d4-e5f6-4a3b-8c9d-0e1f2a3b4c5d",
	"simulation": 1,
	"push": {
		"channel": "public-market:r-depth-14",
		"pub": {
			"data": {
				"U": 100000,
				"u": 100001,
				"a": [
					["62651", "0.29045069"],
					["62670", "0.40000000"]
				],
				"b": [
					["62649", "0.55175335"]
				]
			},
			"offset": 1000
		}
	}
}`,
		// 03 ws update — U == 100001 (previous delta's u), u == 100002
		`{
	"id": "b2c3d4e5-f6a7-4b3c-9d0e-1f2a3b4c5d6e",
	"simulation": 1,
	"push": {
		"channel": "public-market:r-depth-14",
		"pub": {
			"data": {
				"U": 100001,
				"u": 100002,
				"a": [
					["62660", "0"]
				],
				"b": [
					["62640", "0"],
					["62635", "2.00000000"]
				]
			},
			"offset": 1001
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 rest snapshot
			ExchangeID: 7,
			PairID:     1,
			Simulation: 1,
			EventTime:  "",
			Asks: []events.PriceLevel{
				{Price: "62650", Quantity: "2.21924167"},
				{Price: "62651", Quantity: "0.17447383"},
				{Price: "62660", Quantity: "0.33476925"},
			},
			Bids: []events.PriceLevel{
				{Price: "62649", Quantity: "0.5"},
				{Price: "62648", Quantity: "0.02744953"},
				{Price: "62640", Quantity: "1.31062803"},
			},
		},
		{ // after 02 ws update
			ExchangeID: 7,
			PairID:     1,
			Simulation: 1,
			EventTime:  "",
			Asks: []events.PriceLevel{
				{Price: "62650", Quantity: "2.21924167"},
				{Price: "62651", Quantity: "0.29045069"},
				{Price: "62660", Quantity: "0.33476925"},
				{Price: "62670", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62649", Quantity: "0.55175335"},
				{Price: "62648", Quantity: "0.02744953"},
				{Price: "62640", Quantity: "1.31062803"},
			},
		},
		{ // after 03 ws update
			ExchangeID: 7,
			PairID:     1,
			Simulation: 1,
			EventTime:  "",
			Asks: []events.PriceLevel{
				{Price: "62650", Quantity: "2.21924167"},
				{Price: "62651", Quantity: "0.29045069"},
				{Price: "62670", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62649", Quantity: "0.55175335"},
				{Price: "62648", Quantity: "0.02744953"},
				{Price: "62635", Quantity: "2"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 7, Simulation: 1, Price: "62650", Quantity: "2.21924167"},
			{ExchangeID: 7, Simulation: 1, Price: "62651", Quantity: "0.29045069"},
			{ExchangeID: 7, Simulation: 1, Price: "62670", Quantity: "0.4"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 7, Simulation: 1, Price: "62649", Quantity: "0.55175335"},
			{ExchangeID: 7, Simulation: 1, Price: "62648", Quantity: "0.02744953"},
			{ExchangeID: 7, Simulation: 1, Price: "62635", Quantity: "2"},
		},
	},
}

// Ex7SequenceGap — a delta whose U skips past the previous event's seq is a
// gap; only a fresh REST snapshot re-arms the baseline.
var Ex7SequenceGap = Scenario{
	ExchangeID:      7,
	PairID:          1,
	IgnoreEventTime: true,
	Sources: []string{
		// 01 rest snapshot, lastUpdateId 200000
		`{
	"id": "c3d4e5f6-a7b8-4c3d-0e1f-2a3b4c5d6e7f",
	"simulation": 1,
	"status": "OK",
	"action": "snapshot",
	"pair": "14",
	"data": {
		"lastUpdateId": 200000,
		"time": 1800000000000000,
		"bids": [
			["62649", "0.50000000"],
			["62640", "1.31062803"]
		],
		"asks": [
			["62650", "2.21924167"],
			["62660", "0.33476925"]
		]
	}
}`,
		// 02 ws update — U == 200000 (previous seq), u == 200001, applies cleanly
		`{
	"id": "d4e5f6a7-b8c9-4d3e-1f2a-3b4c5d6e7f8a",
	"simulation": 1,
	"push": {
		"channel": "public-market:r-depth-14",
		"pub": {
			"data": {
				"U": 200000,
				"u": 200001,
				"a": [
					["62670", "0.40000000"]
				],
				"b": []
			},
			"offset": 2000
		}
	}
}`,
		// 03 ws update — U == 200005, but previous seq was 200001: a gap
		`{
	"id": "e5f6a7b8-c9d0-4e3f-2a3b-4c5d6e7f8a9b",
	"simulation": 1,
	"push": {
		"channel": "public-market:r-depth-14",
		"pub": {
			"data": {
				"U": 200005,
				"u": 200006,
				"a": [
					["62690", "9.99900000"]
				],
				"b": [
					["62630", "9.99900000"]
				]
			},
			"offset": 2001
		}
	}
}`,
		// 04 rest snapshot resync, lastUpdateId 300000
		`{
	"id": "f6a7b8c9-d0e1-4f3a-3b4c-5d6e7f8a9b0c",
	"simulation": 1,
	"status": "OK",
	"action": "snapshot",
	"pair": "14",
	"data": {
		"lastUpdateId": 300000,
		"time": 1800000000300000,
		"bids": [
			["63049", "0.60000000"],
			["63040", "1.20000000"]
		],
		"asks": [
			["63050", "1.80000000"],
			["63060", "0.40000000"]
		]
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 rest snapshot
			ExchangeID: 7,
			PairID:     1,
			Simulation: 1,
			EventTime:  "",
			Asks: []events.PriceLevel{
				{Price: "62650", Quantity: "2.21924167"},
				{Price: "62660", Quantity: "0.33476925"},
			},
			Bids: []events.PriceLevel{
				{Price: "62649", Quantity: "0.5"},
				{Price: "62640", Quantity: "1.31062803"},
			},
		},
		{ // after 02 ws update
			ExchangeID: 7,
			PairID:     1,
			Simulation: 1,
			EventTime:  "",
			Asks: []events.PriceLevel{
				{Price: "62650", Quantity: "2.21924167"},
				{Price: "62660", Quantity: "0.33476925"},
				{Price: "62670", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62649", Quantity: "0.5"},
				{Price: "62640", Quantity: "1.31062803"},
			},
		},
		{ // after 03 ws update gap — synthetic reset, book emptied
			ExchangeID: 7,
			PairID:     1,
			Simulation: 1,
			EventTime:  "",
			Asks:       []events.PriceLevel{},
			Bids:       []events.PriceLevel{},
		},
		{ // after 04 rest snapshot resync
			ExchangeID: 7,
			PairID:     1,
			Simulation: 1,
			EventTime:  "",
			Asks: []events.PriceLevel{
				{Price: "63050", Quantity: "1.8"},
				{Price: "63060", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "63049", Quantity: "0.6"},
				{Price: "63040", Quantity: "1.2"},
			},
		},
	},
	WantRejects: []string{"sequence_gap"},
	// The gap is what makes job 2 ask NiFi for a fresh snapshot: one command
	// for the episode, matching the one rejected event (see Ex1SequenceGap's
	// own note on this — there is no second command if a further update
	// arrives before resync, but this scenario resyncs right away).
	WantControlCommands: []events.ControlCommand{
		{Action: "snapshot_request", ExchangeID: 7, PairID: 1, Simulation: 1},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 7, Simulation: 1, Price: "63050", Quantity: "1.8"},
			{ExchangeID: 7, Simulation: 1, Price: "63060", Quantity: "0.4"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 7, Simulation: 1, Price: "63049", Quantity: "0.6"},
			{ExchangeID: 7, Simulation: 1, Price: "63040", Quantity: "1.2"},
		},
	},
}

// Ex7PrecisionDust — job 4 on an ompfinex feed: BTCUSDT (internal pair 1,
// ompfinex market "14") truncates price to 2 places and quantity to 8. Rebase
// is 0/0, so the numbers that move here moved in job 4 and nowhere else — same
// shape as Ex1PrecisionDust.
var Ex7PrecisionDust = Scenario{
	ExchangeID:      7,
	PairID:          1,
	IgnoreEventTime: true,
	Sources: []string{
		// 01 rest snapshot — two bids collide once truncated to 2 places
		`{
	"id": "a7b8c9d0-e1f2-4a3b-4c5d-6e7f8a9b0c1d",
	"simulation": 1,
	"status": "OK",
	"action": "snapshot",
	"pair": "14",
	"data": {
		"lastUpdateId": 400000,
		"time": 1800000000000000,
		"bids": [
			["62650.123", "0.40000000"],
			["62650.129", "0.25000000"],
			["62649.5", "1.00000000"]
		],
		"asks": [
			["62651.006", "0.30000000"],
			["62652", "0.000000005"]
		]
	}
}`,
		// 02 ws update — U == 400000 (previous seq), dust ask deletes the resting 62651 level
		`{
	"id": "b8c9d0e1-f2a3-4b3c-5d6e-7f8a9b0c1d2e",
	"simulation": 1,
	"push": {
		"channel": "public-market:r-depth-14",
		"pub": {
			"data": {
				"U": 400000,
				"u": 400001,
				"a": [
					["62651.004", "0.000000009"],
					["62653.999", "0.75000000"]
				],
				"b": [
					["62650.121", "0.10000000"],
					["62650.128", "0.20000000"]
				]
			},
			"offset": 4000
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 rest snapshot — .123 and .129 merge at .12 (0.4+0.25); dust ask never rests
			ExchangeID: 7,
			PairID:     1,
			Simulation: 1,
			EventTime:  "",
			Asks: []events.PriceLevel{
				{Price: "62651", Quantity: "0.3"},
			},
			Bids: []events.PriceLevel{
				{Price: "62650.12", Quantity: "0.65"},
				{Price: "62649.5", Quantity: "1"},
			},
		},
		{ // after 02 ws update — dust deletes the 62651 ask already resting there
			ExchangeID: 7,
			PairID:     1,
			Simulation: 1,
			EventTime:  "",
			Asks: []events.PriceLevel{
				{Price: "62653.99", Quantity: "0.75"},
			},
			Bids: []events.PriceLevel{
				{Price: "62650.12", Quantity: "0.3"},
				{Price: "62649.5", Quantity: "1"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{{ExchangeID: 7, Simulation: 1, Price: "62653.99", Quantity: "0.75"}},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 7, Simulation: 1, Price: "62650.12", Quantity: "0.3"},
			{ExchangeID: 7, Simulation: 1, Price: "62649.5", Quantity: "1"},
		},
	},
}