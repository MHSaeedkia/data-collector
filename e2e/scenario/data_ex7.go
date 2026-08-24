// Scenarios for ex7/ompfinex — a REST snapshot with a real lastUpdateId, plus
// Centrifugo WS deltas carrying a Binance-style u/U range on channel
// "public-market:r-depth-{market}". The conventions these follow are in data.go.
//
// Confirmed from OmpfinexParser (io.tibobit.normalizer.pairextract.parser),
// verified against live samples:
//   - The market comes from the channel suffix on updates
//     ("public-market:r-depth-14" -> market "14") and from the top-level
//     "pair" field on snapshots. They must agree, or the two streams route to
//     different pairs (or nowhere, if the suffix does not resolve at all — a
//     silent job-1 drop, not a dead-letter).
//   - Continuity is U_n == seq_{n-1} (NOT seq_{n-1}+1): a delta's U must equal
//     the previous event's seq (the snapshot's lastUpdateId, or the previous
//     delta's u) — confirmed against two consecutive live samples where the
//     second message's U (859075) equalled the first's u (859075) exactly.
//   - A side key (a/b) is always present on the wire, possibly an empty array,
//     and an empty array is a no-op — NOT a "null side" the way bybit/okx have
//     one. If a side key is missing entirely (not just empty), isArray() fails
//     and the WHOLE event is dropped, both sides — there is no partial-event
//     case for this exchange.
//   - Deltas carry no wire timestamp; job 1 stamps processing time, so any
//     snapshot produced by a delta has a wall-clock, unassertable EventTime.
//     Every scenario that includes a delta therefore sets IgnoreEventTime and
//     blanks EventTime on every WantSnapshots entry, including ones that came
//     from the REST snapshot — the flag is all-or-nothing per scenario.
//   - REST snapshot "time" is epoch MICROSECONDS on the wire (divided by 1000
//     for the platform's millis event time) — confirmed against a live sample.
//   - Control commands carry a Reason matching the reject_reason that
//     triggered them (confirmed live: "no_baseline").
//
// Two cases the peer exchange blocks have are still missing here. Both are
// buildable now — the reasons this file originally gave for skipping them do
// not hold:
//
//   - A stale/duplicate replay (ex8 has 41, ex1/ex2 have 05/13). The
//     reject_reason is NOT unconfirmed: TypeValidateFunction's update branch
//     rejects sequence_id <= lastSeq as "stale_or_duplicate". Worth knowing
//     when writing it — a genuinely REPLAYED ompfinex message cannot reach
//     that branch, because it carries the old U as well and so fails the
//     U == lastSeq contiguity check first. Reaching stale_or_duplicate takes
//     a delta whose U is right but whose u has gone backwards.
//   - A rebase scenario (ex1 has 07/08, ex4 has 23/24). ompfinex DOES have
//     markets with non-zero factors: 02_seed.sql gives market "1" (pair 2)
//     price_amount_rebase -1, and roughly half the ex7 rows carry it. This
//     needs the ex7 pair-2 row present wherever the suite runs, nothing more.

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
	WantControlCommands: []events.ControlCommand{
		{Action: "snapshot_request", Reason: "sequence_gap", ExchangeID: 7, PairID: 1, Simulation: 1},
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

// Ex7NoBaseline — a WS delta before any REST snapshot has no baseline to apply
// to. This is what makes job 2 ask NiFi for a snapshot: only a snapshot can
// give the pair a baseline.
var Ex7NoBaseline = Scenario{
	ExchangeID:      7,
	PairID:          1,
	IgnoreEventTime: true,
	Sources: []string{
		// 01 ws update, no baseline yet
		`{
	"id": "10a2b3c4-d5e6-4f3a-8b9c-0d1e2f3a4b5c",
	"simulation": 1,
	"push": {
		"channel": "public-market:r-depth-14",
		"pub": {
			"data": {
				"U": 600000,
				"u": 600001,
				"a": [
					["62651", "0.29045069"]
				],
				"b": [
					["62649", "0.55175335"]
				]
			},
			"offset": 6000
		}
	}
}`,
		// 02 rest snapshot, lastUpdateId 700000
		`{
	"id": "21b3c4d5-e6f7-4a3b-9c0d-1e2f3a4b5c6d",
	"simulation": 1,
	"status": "OK",
	"action": "snapshot",
	"pair": "14",
	"data": {
		"lastUpdateId": 700000,
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
		// 03 ws update — U == 700000 (the snapshot's lastUpdateId), applies cleanly
		`{
	"id": "32c4d5e6-f7a8-4b3c-0d1e-2f3a4b5c6d7e",
	"simulation": 1,
	"push": {
		"channel": "public-market:r-depth-14",
		"pub": {
			"data": {
				"U": 700000,
				"u": 700001,
				"a": [
					["62670", "0.40000000"]
				],
				"b": []
			},
			"offset": 6001
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 02 rest snapshot
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
		{ // after 03 ws update
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
	},
	WantRejects: []string{"no_baseline"},
	WantControlCommands: []events.ControlCommand{
		{Action: "snapshot_request", Reason: "no_baseline", ExchangeID: 7, PairID: 1, Simulation: 1},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 7, Simulation: 1, Price: "62650", Quantity: "2.21924167"},
			{ExchangeID: 7, Simulation: 1, Price: "62660", Quantity: "0.33476925"},
			{ExchangeID: 7, Simulation: 1, Price: "62670", Quantity: "0.4"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 7, Simulation: 1, Price: "62649", Quantity: "0.5"},
			{ExchangeID: 7, Simulation: 1, Price: "62640", Quantity: "1.31062803"},
		},
	},
}

// Ex7NoiseFrames — ompfinex frames job 1 drops WITHOUT consuming a sequence id
// or emitting anything: no "push" at all, a channel outside the r-depth
// prefix, and an update missing one of its side keys entirely (as opposed to
// carrying it as an empty array, which IS valid — see Ex7OneSidedUpdate).
// None of these reach job 2, so none of them dead-letter either.
var Ex7NoiseFrames = Scenario{
	ExchangeID:      7,
	PairID:          1,
	IgnoreEventTime: true,
	Sources: []string{
		// 01 connect ack — no "push" key at all
		`{
	"id": "43d5e6f7-a8b9-4c3d-1e2f-3a4b5c6d7e8f",
	"simulation": 1,
	"connect": {
		"client": "9f8e7d6c-5b4a-4392-8102-abcdef012345",
		"version": "5.0.0"
	}
}`,
		// 02 foreign channel — starts with "public-market:" but not the r-depth prefix
		`{
	"id": "54e6f7a8-b9c0-4d3e-2f3a-4b5c6d7e8f90",
	"simulation": 1,
	"push": {
		"channel": "public-market:r-trades-14",
		"pub": {
			"data": {
				"price": "62650",
				"quantity": "0.01"
			},
			"offset": 7000
		}
	}
}`,
		// 03 rest snapshot, lastUpdateId 800000
		`{
	"id": "65f7a8b9-c0d1-4e3f-3a4b-5c6d7e8f9012",
	"simulation": 1,
	"status": "OK",
	"action": "snapshot",
	"pair": "14",
	"data": {
		"lastUpdateId": 800000,
		"time": 1800000000000000,
		"bids": [
			["62949", "0.50000000"],
			["62940", "1.31062803"]
		],
		"asks": [
			["62950", "2.21924167"],
			["62960", "0.33476925"]
		]
	}
}`,
		// 04 malformed update — "b" key missing entirely (not an empty array), dropped whole
		`{
	"id": "76a8b9c0-d1e2-4f3a-4b5c-6d7e8f901234",
	"simulation": 1,
	"push": {
		"channel": "public-market:r-depth-14",
		"pub": {
			"data": {
				"U": 800000,
				"u": 800001,
				"a": [
					["62970", "0.10000000"]
				]
			},
			"offset": 7001
		}
	}
}`,
		// 05 ws update — valid, U == 800000 (the snapshot's lastUpdateId, since 04 never advanced it)
		`{
	"id": "87b9c0d1-e2f3-4a3b-5c6d-7e8f90123456",
	"simulation": 1,
	"push": {
		"channel": "public-market:r-depth-14",
		"pub": {
			"data": {
				"U": 800000,
				"u": 800001,
				"a": [
					["62951", "0.15000000"]
				],
				"b": [
					["62938", "1.10000000"]
				]
			},
			"offset": 7002
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 03 rest snapshot
			ExchangeID: 7,
			PairID:     1,
			Simulation: 1,
			EventTime:  "",
			Asks: []events.PriceLevel{
				{Price: "62950", Quantity: "2.21924167"},
				{Price: "62960", Quantity: "0.33476925"},
			},
			Bids: []events.PriceLevel{
				{Price: "62949", Quantity: "0.5"},
				{Price: "62940", Quantity: "1.31062803"},
			},
		},
		{ // after 05 ws update — 04 never touched the book or the baseline
			ExchangeID: 7,
			PairID:     1,
			Simulation: 1,
			EventTime:  "",
			Asks: []events.PriceLevel{
				{Price: "62950", Quantity: "2.21924167"},
				{Price: "62951", Quantity: "0.15"},
				{Price: "62960", Quantity: "0.33476925"},
			},
			Bids: []events.PriceLevel{
				{Price: "62949", Quantity: "0.5"},
				{Price: "62940", Quantity: "1.31062803"},
				{Price: "62938", Quantity: "1.1"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 7, Simulation: 1, Price: "62950", Quantity: "2.21924167"},
			{ExchangeID: 7, Simulation: 1, Price: "62951", Quantity: "0.15"},
			{ExchangeID: 7, Simulation: 1, Price: "62960", Quantity: "0.33476925"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 7, Simulation: 1, Price: "62949", Quantity: "0.5"},
			{ExchangeID: 7, Simulation: 1, Price: "62940", Quantity: "1.31062803"},
			{ExchangeID: 7, Simulation: 1, Price: "62938", Quantity: "1.1"},
		},
	},
}

// Ex7OneSidedUpdate — a side key carried as an EMPTY ARRAY is a valid no-op on
// that side, not a drop and not an error — distinct from Ex7NoiseFrames'
// missing-key case, which drops the whole message. Matches a live sample
// pattern (one message touching bids only with "a": [], the next touching
// asks only with "b": []).
var Ex7OneSidedUpdate = Scenario{
	ExchangeID:      7,
	PairID:          1,
	IgnoreEventTime: true,
	Sources: []string{
		// 01 rest snapshot, lastUpdateId 900000
		`{
	"id": "98c0d1e2-f3a4-4b3c-6d7e-8f9012345678",
	"simulation": 1,
	"status": "OK",
	"action": "snapshot",
	"pair": "14",
	"data": {
		"lastUpdateId": 900000,
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
		// 02 ws update — "a" empty (no ask changes), only bids move
		`{
	"id": "a9d1e2f3-a4b5-4c3d-7e8f-901234567890",
	"simulation": 1,
	"push": {
		"channel": "public-market:r-depth-14",
		"pub": {
			"data": {
				"U": 900000,
				"u": 900001,
				"a": [],
				"b": [
					["62649", "0"],
					["62638", "1.10000000"]
				]
			},
			"offset": 9000
		}
	}
}`,
		// 03 ws update — "b" empty (no bid changes), only asks move
		`{
	"id": "bae2f3a4-b5c6-4d3e-8f90-123456789012",
	"simulation": 1,
	"push": {
		"channel": "public-market:r-depth-14",
		"pub": {
			"data": {
				"U": 900001,
				"u": 900002,
				"a": [
					["62650", "0"],
					["62670", "0.40000000"]
				],
				"b": []
			},
			"offset": 9001
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
				{Price: "62660", Quantity: "0.33476925"},
			},
			Bids: []events.PriceLevel{
				{Price: "62649", Quantity: "0.5"},
				{Price: "62640", Quantity: "1.31062803"},
			},
		},
		{ // after 02 ws update — asks untouched, bids changed
			ExchangeID: 7,
			PairID:     1,
			Simulation: 1,
			EventTime:  "",
			Asks: []events.PriceLevel{
				{Price: "62650", Quantity: "2.21924167"},
				{Price: "62660", Quantity: "0.33476925"},
			},
			Bids: []events.PriceLevel{
				{Price: "62640", Quantity: "1.31062803"},
				{Price: "62638", Quantity: "1.1"},
			},
		},
		{ // after 03 ws update — bids untouched, asks changed
			ExchangeID: 7,
			PairID:     1,
			Simulation: 1,
			EventTime:  "",
			Asks: []events.PriceLevel{
				{Price: "62660", Quantity: "0.33476925"},
				{Price: "62670", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62640", Quantity: "1.31062803"},
				{Price: "62638", Quantity: "1.1"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 7, Simulation: 1, Price: "62660", Quantity: "0.33476925"},
			{ExchangeID: 7, Simulation: 1, Price: "62670", Quantity: "0.4"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 7, Simulation: 1, Price: "62640", Quantity: "1.31062803"},
			{ExchangeID: 7, Simulation: 1, Price: "62638", Quantity: "1.1"},
		},
	},
}
