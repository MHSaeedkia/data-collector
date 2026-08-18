// Scenarios for ex6/bybit — a true snapshot/delta feed with a contiguous counter, in bybit's own
// `topic`/`ts`/`type`/`data`/`cts` envelope.
//
// What makes ex6 different:
//
//   - `type: "snapshot" | "delta"` is the regime discriminator; "delta" becomes our "update".
//   - The sequence id is `data.u` with jump 1, so job 2's FULL delta ruleset applies here —
//     no_baseline, sequence_gap (plus the synthetic reset that empties the book),
//     awaiting_snapshot and stale_or_duplicate are all reachable. The sibling `data.seq` is
//     non-contiguous bybit-internal metadata and is never read.
//   - Sides are the abbreviated `b`/`a`, and a delta may carry only one of them. A MISSING key is
//     a null side — "no report" — which job 5 leaves untouched, while a present-but-empty array
//     clears it. ex3 proves that rule on snapshots; ex6 is the only place it can be proved on
//     updates (Ex6OneSidedDelta).
//   - The event time is `cts` (matching-engine time), not the outer `ts` (gateway send time).
//
// Every bybit market in the seed is a USDT market with rebase 0/0, so job 3 is the identity here.
// Pair 1 (BTCUSDT) is price_precision 2 / quantity_precision 8.

package scenario

import "orderbook-e2e/events"

// Ex6SnapshotThenDeltas — the happy path: a snapshot seeds the book, then contiguous deltas merge
// into it. 03 carries the delete frame the raw-data capture never caught: quantity "0" removes a
// resting level, and a price absent from the snapshot is inserted.
var Ex6SnapshotThenDeltas = Scenario{
	ExchangeID: 6,
	PairID:     1,
	Sources: []string{
		// 01 snapshot
		`{
	"id": "d0e56d81-a635-4769-8ee3-77e516ac6d0d",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000000006,
	"type": "snapshot",
	"data": {
		"s": "BTCUSDT",
		"b": [["62724.1", "0.407233"], ["62723.6", "0.00012"], ["62722.6", "0.002"]],
		"a": [["62724.2", "0.529827"], ["62724.3", "0.029207"], ["62724.4", "0.029554"]],
		"u": 126776811,
		"seq": 111416318484
	},
	"cts": 1800000000000
}`,
		// 02 delta — an existing ask is re-quoted, a brand-new bid appears
		`{
	"id": "22f1bfca-ed3b-4f3b-b27b-0016817bb18d",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000001006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"b": [["62709.4", "0.096404"]],
		"a": [["62724.2", "0.529037"]],
		"u": 126776812,
		"seq": 111416318490
	},
	"cts": 1800000001000
}`,
		// 03 delta — quantity "0" deletes on both sides, and 62725 is inserted
		`{
	"id": "58156014-f789-4930-818f-f5ad68ff65d3",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000002006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"b": [["62722.6", "0"]],
		"a": [["62724.3", "0"], ["62725", "0.75"]],
		"u": 126776813,
		"seq": 111416318501
	},
	"cts": 1800000002000
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62724.2", Quantity: "0.529827"},
				{Price: "62724.3", Quantity: "0.029207"},
				{Price: "62724.4", Quantity: "0.029554"},
			},
			Bids: []events.PriceLevel{
				{Price: "62724.1", Quantity: "0.407233"},
				{Price: "62723.6", Quantity: "0.00012"},
				{Price: "62722.6", Quantity: "0.002"},
			},
		},
		{ // after 02 — a delta merges, it does not replace
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "62724.2", Quantity: "0.529037"},
				{Price: "62724.3", Quantity: "0.029207"},
				{Price: "62724.4", Quantity: "0.029554"},
			},
			Bids: []events.PriceLevel{
				{Price: "62724.1", Quantity: "0.407233"},
				{Price: "62723.6", Quantity: "0.00012"},
				{Price: "62722.6", Quantity: "0.002"},
				{Price: "62709.4", Quantity: "0.096404"},
			},
		},
		{ // after 03 — 62724.3 and 62722.6 removed, 62725 inserted
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks: []events.PriceLevel{
				{Price: "62724.2", Quantity: "0.529037"},
				{Price: "62724.4", Quantity: "0.029554"},
				{Price: "62725", Quantity: "0.75"},
			},
			Bids: []events.PriceLevel{
				{Price: "62724.1", Quantity: "0.407233"},
				{Price: "62723.6", Quantity: "0.00012"},
				{Price: "62709.4", Quantity: "0.096404"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 6, Simulation: 1, Price: "62724.2", Quantity: "0.529037"},
			{ExchangeID: 6, Simulation: 1, Price: "62724.4", Quantity: "0.029554"},
			{ExchangeID: 6, Simulation: 1, Price: "62725", Quantity: "0.75"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 6, Simulation: 1, Price: "62724.1", Quantity: "0.407233"},
			{ExchangeID: 6, Simulation: 1, Price: "62723.6", Quantity: "0.00012"},
			{ExchangeID: 6, Simulation: 1, Price: "62709.4", Quantity: "0.096404"},
		},
	},
}

// Ex6OneSidedDelta — a missing side key is a null side, and null is not empty. 02 and 03 leave the
// unreported side exactly as it was, and 04 pushes the same rule onto a SNAPSHOT: it replaces the
// asks wholesale while the bids, which it says nothing about, survive untouched.
var Ex6OneSidedDelta = Scenario{
	ExchangeID: 6,
	PairID:     1,
	Sources: []string{
		// 01 snapshot
		`{
	"id": "4ae5cddb-2695-41c4-b6d9-b223176b0de7",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000000006,
	"type": "snapshot",
	"data": {
		"s": "BTCUSDT",
		"b": [["62799", "3"], ["62798", "4"]],
		"a": [["62800", "1"], ["62801", "2"]],
		"u": 200,
		"seq": 111416318484
	},
	"cts": 1800000000000
}`,
		// 02 delta, asks only — no "b" key at all
		`{
	"id": "d0d03ef3-1eec-4350-a8be-045632686521",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000001006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"a": [["62800", "0.5"]],
		"u": 201,
		"seq": 111416318490
	},
	"cts": 1800000001000
}`,
		// 03 delta, bids only — deletes 62799, says nothing about the asks
		`{
	"id": "1b99dfd1-c182-4f58-bb25-2c1c01aaa9ff",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000002006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"b": [["62799", "0"]],
		"u": 202,
		"seq": 111416318501
	},
	"cts": 1800000002000
}`,
		// 04 snapshot, asks only — a wholesale replace of one side, the other untouched
		`{
	"id": "a407fd1c-cf0e-41d8-a057-d48e46d555a6",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000003006,
	"type": "snapshot",
	"data": {
		"s": "BTCUSDT",
		"a": [["62810", "1.5"]],
		"u": 203,
		"seq": 111416318510
	},
	"cts": 1800000003000
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62800", Quantity: "1"},
				{Price: "62801", Quantity: "2"},
			},
			Bids: []events.PriceLevel{
				{Price: "62799", Quantity: "3"},
				{Price: "62798", Quantity: "4"},
			},
		},
		{ // after 02 — bids untouched by an asks-only delta
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "62800", Quantity: "0.5"},
				{Price: "62801", Quantity: "2"},
			},
			Bids: []events.PriceLevel{
				{Price: "62799", Quantity: "3"},
				{Price: "62798", Quantity: "4"},
			},
		},
		{ // after 03 — asks untouched by a bids-only delta
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks: []events.PriceLevel{
				{Price: "62800", Quantity: "0.5"},
				{Price: "62801", Quantity: "2"},
			},
			Bids: []events.PriceLevel{
				{Price: "62798", Quantity: "4"},
			},
		},
		{ // after 04 — the asks were replaced wholesale, the unreported bids survived
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:03Z",
			Asks: []events.PriceLevel{
				{Price: "62810", Quantity: "1.5"},
			},
			Bids: []events.PriceLevel{
				{Price: "62798", Quantity: "4"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{{ExchangeID: 6, Simulation: 1, Price: "62810", Quantity: "1.5"}},
		Bids: []events.AggregatedLevel{{ExchangeID: 6, Simulation: 1, Price: "62798", Quantity: "4"}},
	},
}

// Ex6SequenceGap — the whole gap episode. A jump in `u` dead-letters the gap event AND puts a
// synthetic reset on the main stream, which job 5 turns into a fully emptied book so bybit drops
// out of the aggregated view instead of serving a book it can no longer trust. Every delta after
// that is awaiting_snapshot until a real snapshot re-syncs.
var Ex6SequenceGap = Scenario{
	ExchangeID: 6,
	PairID:     1,
	Sources: []string{
		// 01 snapshot
		`{
	"id": "26bd49f0-7a9f-4f7e-ad61-a38f9bbec9f5",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000000006,
	"type": "snapshot",
	"data": {
		"s": "BTCUSDT",
		"b": [["62899", "2"]],
		"a": [["62900", "1"]],
		"u": 300,
		"seq": 111416318484
	},
	"cts": 1800000000000
}`,
		// 02 delta, contiguous
		`{
	"id": "83164cc5-a429-4886-bfb8-153ac9587188",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000001006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"b": [["62898", "1.5"]],
		"a": [["62901", "0.5"]],
		"u": 301,
		"seq": 111416318490
	},
	"cts": 1800000001000
}`,
		// 03 delta, u jumps 301 -> 305
		`{
	"id": "98baddf3-c999-48e1-b976-0b6760b4b7db",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000002006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"b": [["62897", "1"]],
		"a": [["62902", "0.25"]],
		"u": 305,
		"seq": 111416318520
	},
	"cts": 1800000002000
}`,
		// 04 delta while still waiting for a re-sync
		`{
	"id": "ed7fb71d-bd80-49b1-aded-d8ce98fe2574",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000003006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"b": [["62896", "1"]],
		"a": [["62903", "0.25"]],
		"u": 306,
		"seq": 111416318530
	},
	"cts": 1800000003000
}`,
		// 05 snapshot — re-sync, on a fresh sequence
		`{
	"id": "f56a2b5a-3c51-4878-bfc5-eceee0a81749",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000004006,
	"type": "snapshot",
	"data": {
		"s": "BTCUSDT",
		"b": [["62909", "2"]],
		"a": [["62910", "1"]],
		"u": 400,
		"seq": 111416318600
	},
	"cts": 1800000004000
}`,
		// 06 delta, contiguous again
		`{
	"id": "6ae295da-f218-452f-a3f4-d179aa862370",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000005006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"a": [["62911", "0.25"]],
		"u": 401,
		"seq": 111416318610
	},
	"cts": 1800000005000
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{{Price: "62900", Quantity: "1"}},
			Bids:       []events.PriceLevel{{Price: "62899", Quantity: "2"}},
		},
		{ // after 02
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "62900", Quantity: "1"},
				{Price: "62901", Quantity: "0.5"},
			},
			Bids: []events.PriceLevel{
				{Price: "62899", Quantity: "2"},
				{Price: "62898", Quantity: "1.5"},
			},
		},
		{ // the reset job 2 emitted for 03 — an empty book carrying the gap event's own time.
			// 03's own levels never reached the book; it was dead-lettered.
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks:       []events.PriceLevel{},
			Bids:       []events.PriceLevel{},
		},
		{ // after 05 — 04 was rejected, so the book is exactly the re-sync snapshot
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:04Z",
			Asks:       []events.PriceLevel{{Price: "62910", Quantity: "1"}},
			Bids:       []events.PriceLevel{{Price: "62909", Quantity: "2"}},
		},
		{ // after 06 — contiguity resumed from the snapshot's u
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:05Z",
			Asks: []events.PriceLevel{
				{Price: "62910", Quantity: "1"},
				{Price: "62911", Quantity: "0.25"},
			},
			Bids: []events.PriceLevel{{Price: "62909", Quantity: "2"}},
		},
	},
	WantRejects: []string{"sequence_gap", "awaiting_snapshot"},
	// One command for the episode, not one per rejected event: the second update
	// rejects on the same unresolved gap, and job 2 does not re-ask.
	WantControlCommands: []events.ControlCommand{
		{Action: "snapshot_request", ExchangeID: 6, PairID: 1, Simulation: 1},
	},
	// The reset already emptied the book once; what the web app finally reads is the re-synced
	// one, so a gap costs bybit its place in the union only until the next snapshot.
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 6, Simulation: 1, Price: "62910", Quantity: "1"},
			{ExchangeID: 6, Simulation: 1, Price: "62911", Quantity: "0.25"},
		},
		Bids: []events.AggregatedLevel{{ExchangeID: 6, Simulation: 1, Price: "62909", Quantity: "2"}},
	},
}

// Ex6NoBaseline — a delta that arrives before any snapshot has nothing to merge into, so it is
// dead-lettered rather than seeding a book from a partial frame. 03 then covers the last delta
// rule: a `u` that does not move forward is stale_or_duplicate, not a gap.
var Ex6NoBaseline = Scenario{
	ExchangeID: 6,
	PairID:     1,
	Sources: []string{
		// 01 delta, cold — nothing to merge into
		`{
	"id": "de4ad080-f54d-42ad-a0c4-2b2b2cf85f2f",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000000006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"b": [["62998", "1"]],
		"a": [["63002", "1"]],
		"u": 500,
		"seq": 111416318484
	},
	"cts": 1800000000000
}`,
		// 02 snapshot — the first real baseline
		`{
	"id": "758413f7-b452-4b4d-8bd0-d8c2867dc8c3",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000001006,
	"type": "snapshot",
	"data": {
		"s": "BTCUSDT",
		"b": [["62999", "2"]],
		"a": [["63000", "1"]],
		"u": 501,
		"seq": 111416318490
	},
	"cts": 1800000001000
}`,
		// 03 delta replaying the snapshot's own u
		`{
	"id": "a1a20d49-f77b-47c7-bc6d-b96902281049",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000002006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"b": [["62997", "1"]],
		"a": [["63003", "1"]],
		"u": 501,
		"seq": 111416318500
	},
	"cts": 1800000002000
}`,
		// 04 delta, contiguous with the snapshot
		`{
	"id": "e0ee1c2e-6ef8-48d1-800a-5bc5df81b883",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000003006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"a": [["63001", "0.5"]],
		"u": 502,
		"seq": 111416318510
	},
	"cts": 1800000003000
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 02 — 01 never reached the book builder
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks:       []events.PriceLevel{{Price: "63000", Quantity: "1"}},
			Bids:       []events.PriceLevel{{Price: "62999", Quantity: "2"}},
		},
		{ // after 04 — 03 was rejected, so 62997/63003 are nowhere
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:03Z",
			Asks: []events.PriceLevel{
				{Price: "63000", Quantity: "1"},
				{Price: "63001", Quantity: "0.5"},
			},
			Bids: []events.PriceLevel{{Price: "62999", Quantity: "2"}},
		},
	},
	WantRejects: []string{"no_baseline", "stale_or_duplicate"},
	// Only the cold delta asks for a snapshot. The stale_or_duplicate one does
	// not: a replayed `u` is a duplicate, not a hole, and the book it would have
	// applied to is intact — so there is nothing for NiFi to re-send.
	WantControlCommands: []events.ControlCommand{
		{Action: "snapshot_request", ExchangeID: 6, PairID: 1, Simulation: 1},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 6, Simulation: 1, Price: "63000", Quantity: "1"},
			{ExchangeID: 6, Simulation: 1, Price: "63001", Quantity: "0.5"},
		},
		Bids: []events.AggregatedLevel{{ExchangeID: 6, Simulation: 1, Price: "62999", Quantity: "2"}},
	},
}

// Ex6NoiseFrames — everything that is not a well-formed bybit book frame for a known market is
// dropped by job 1 without a dead-letter and without touching the book or the sequence state.
var Ex6NoiseFrames = Scenario{
	ExchangeID: 6,
	PairID:     1,
	Sources: []string{
		// 01 snapshot
		`{
	"id": "a0c75a39-9364-4c36-86d8-94e27e2e3490",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000000006,
	"type": "snapshot",
	"data": {
		"s": "BTCUSDT",
		"b": [["62499", "2"]],
		"a": [["62500", "1"]],
		"u": 600,
		"seq": 111416318484
	},
	"cts": 1800000000000
}`,
		// 02 a type that is neither snapshot nor delta
		`{
	"id": "9ea02701-4a40-494b-8b45-a9bf13113890",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000000106,
	"type": "unsubscribe",
	"data": { "s": "BTCUSDT", "a": [["62501", "1"]], "u": 601, "seq": 111416318490 },
	"cts": 1800000000100
}`,
		// 03 no cts — the event time is not optional
		`{
	"id": "6d783a1c-2049-4885-9416-5ca307aab603",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000000206,
	"type": "delta",
	"data": { "s": "BTCUSDT", "a": [["62502", "1"]], "u": 601, "seq": 111416318500 }
}`,
		// 04 u as a string
		`{
	"id": "70257e0a-eaf2-434e-9c72-991834bb88f7",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000000306,
	"type": "delta",
	"data": { "s": "BTCUSDT", "a": [["62503", "1"]], "u": "601", "seq": 111416318510 },
	"cts": 1800000000300
}`,
		// 05 neither side present — nothing to report
		`{
	"id": "b35972bf-b618-42bc-9998-0b55e6c9001a",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000000406,
	"type": "delta",
	"data": { "s": "BTCUSDT", "u": 601, "seq": 111416318520 },
	"cts": 1800000000400
}`,
		// 06 a market ex6 has no exchange_markets row for — dropped before job 2, so its far
		// forward u never poisons the sequence state
		`{
	"id": "ad2b97da-1cd3-4987-803b-fc9ba4f18c3e",
	"simulation": 1,
	"topic": "orderbook.50.FOOBARUSDT",
	"ts": 1800000000506,
	"type": "snapshot",
	"data": { "s": "FOOBARUSDT", "a": [["1.5", "10"]], "b": [["1.4", "10"]], "u": 999999, "seq": 111416318530 },
	"cts": 1800000000500
}`,
		// 07 numeric levels — ex6's wire is string pairs, so the whole frame is unparseable
		`{
	"id": "b466d056-c291-4d15-9f10-8283f697750f",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000000606,
	"type": "delta",
	"data": { "s": "BTCUSDT", "a": [[62504, 1]], "u": 601, "seq": 111416318540 },
	"cts": 1800000000600
}`,
		// 08 delta, contiguous with 01
		`{
	"id": "888f7948-2392-4e1c-a3c3-c4720980ea1b",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000001006,
	"type": "delta",
	"data": { "s": "BTCUSDT", "a": [["62501", "0.4"]], "u": 601, "seq": 111416318550 },
	"cts": 1800000001000
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{{Price: "62500", Quantity: "1"}},
			Bids:       []events.PriceLevel{{Price: "62499", Quantity: "2"}},
		},
		{ // after 08 — 02 through 07 emitted nothing, so u 601 is still contiguous with 600
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "62500", Quantity: "1"},
				{Price: "62501", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{{Price: "62499", Quantity: "2"}},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 6, Simulation: 1, Price: "62500", Quantity: "1"},
			{ExchangeID: 6, Simulation: 1, Price: "62501", Quantity: "0.4"},
		},
		Bids: []events.AggregatedLevel{{ExchangeID: 6, Simulation: 1, Price: "62499", Quantity: "2"}},
	},
}

// Ex6PrecisionDust — job 4 on a delta feed, where the truncation rules bite hardest. 02 is an
// UPDATE whose ask quantity is real on the wire but truncates to zero, which deletes a level that
// was already resting; and whose two bids collide at 2 places, so the merged sum replaces the
// resting quantity rather than adding to it.
var Ex6PrecisionDust = Scenario{
	ExchangeID: 6,
	PairID:     1,
	Sources: []string{
		// 01 snapshot — two asks collide, two bids collide, one ask is dust
		`{
	"id": "a3f4530e-cb87-4d72-b810-417f4378ff3d",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000000006,
	"type": "snapshot",
	"data": {
		"s": "BTCUSDT",
		"b": [["62799.999", "1.5"], ["62799.994", "0.25"]],
		"a": [["62800.117", "0.3"], ["62800.119", "0.2"], ["62801.5", "0.000000004"]],
		"u": 700,
		"seq": 111416318484
	},
	"cts": 1800000000000
}`,
		// 02 delta — dust on a price that is already in the book, and a colliding bid pair
		`{
	"id": "c7321a32-424c-49a1-ad91-0a476d83b520",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000001006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"b": [["62799.998", "0.2"], ["62799.991", "0.1"]],
		"a": [["62800.113", "0.000000009"]],
		"u": 701,
		"seq": 111416318490
	},
	"cts": 1800000001000
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 — .117 and .119 merged at 62800.11 with 0.5; the bids merged at 62799.99
			// with 1.75; 62801.5's 4e-9 truncated to zero and rested nowhere
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{{Price: "62800.11", Quantity: "0.5"}},
			Bids:       []events.PriceLevel{{Price: "62799.99", Quantity: "1.75"}},
		},
		{ // after 02 — 9e-9 truncated to "0" at the price the book was holding, so job 5 deleted
			// the resting level; the merged 0.3 REPLACED the resting 1.75, it did not add to it
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks:       []events.PriceLevel{},
			Bids:       []events.PriceLevel{{Price: "62799.99", Quantity: "0.3"}},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{},
		Bids: []events.AggregatedLevel{{ExchangeID: 6, Simulation: 1, Price: "62799.99", Quantity: "0.3"}},
	},
}
