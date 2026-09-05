// Scenarios for ex5/bitget — REVISED 2026-08-22, when the feed moved from the snapshot-only
// `books50` channel to the price-GROUPED `depth` channel and became a true delta feed.
//
// What makes ex5 different:
//
//   - `data` is an ARRAY of book objects, not a single object. The parser emits one event per
//     element, so ONE Kafka record can produce SEVERAL events (Ex5MultiBookFrame). If any
//     element is malformed the WHOLE record is dropped, including elements already read —
//     accuracy-first, never emit a partial book.
//   - `action` is the regime discriminator and now has TWO values, `"snapshot"` and `"update"`,
//     with qty `"0"` meaning "delete this level". Any other action is noise and is dropped.
//   - There is NO `seq` on the wire any more (nor `pseq`). The `checksum` that replaced them is
//     a CRC book-integrity value — not monotonic, not a sequence. So the sequence id is
//     `data[i].ts`, the STRING epoch millis that is also the event time.
//   - Because that sequence is a CLOCK rather than a counter it never lands on an exact
//     multiple, so ex5 is the only exchange with a nonzero `sequence_jump_tolerance`: job 2
//     accepts `last + 650 ± 110` instead of an exact `last + jump` (Ex5JumpTolerance).
//
//   - Since 2026-08-23 there is a SECOND stream on the topic: bitget's REST depth response, a
//     wholly different shape carrying the same `action: "snapshot"` (Ex5RestSnapshotResync).
//
// REVISED 2026-08-23 (2), after measuring the live dev feed — 4569 frames over 34 minutes,
// BTCUSDT only. Two numbers here were wrong:
//
//   - The WS feed sends NO snapshots at all (3538 updates, 0 snapshot frames), so the REST body
//     is ex5's ONLY baseline. Ex5RestSnapshotResync is therefore the scenario closest to the
//     live shape, not an edge case.
//   - The REST `data.ts` runs on the endpoint's own clock — BEHIND the last WS update 57% of
//     the time — so sequencing the REST body by it made ~90% of resyncs gap immediately:
//     accept → gap → empty the book → ask again, 22 times a minute. It is now null-seq and
//     takes the `baselinePending` bootstrap, exactly as ex1/ex2 REST bodies do.
//   - update→update is bimodal (a 575–625 mass plus a real 725–775 cluster), so the old
//     `600 ± 10` also dead-lettered ~6.7 legitimate updates a minute. Now `650 ± 110`.
//
// Every bitget market in the seed is a USDT market with rebase 0/0, so job 3 is the identity here
// and there is nothing to assert about it — ex1 and ex4 are the only two exchanges that can.
// Pair 1 (BTCUSDT) is price_precision 2 / quantity_precision 8.
//
// Note on event times: the assertions format with time.RFC3339, which has no sub-second field,
// so a 600 ms jump is invisible and consecutive snapshots often share a displayed second.

package scenario

import "orderbook-e2e/events"

// Ex5SnapshotThenUpdates — the happy path on the new delta feed: a snapshot establishes the
// book, then updates merge into it, a qty-"0" level is a delete, and the second update lands
// inside the tolerance window rather than on an exact 600.
var Ex5SnapshotThenUpdates = Scenario{
	ExchangeID: 5,
	PairID:     1,
	Sources: []string{
		// 01 snapshot — the baseline
		`{
	"id": "aed46d6a-f822-458b-820f-3cdb35b2684a",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{
			"asks": [["77208.71", "0.755945"], ["77209.31", "0.14"], ["77209.32", "0.259388"]],
			"bids": [["77208.70", "0.141942"], ["77208.54", "0.005"], ["77206.03", "0.000019"]],
			"checksum": 0,
			"ts": "1800000000000"
		}
	],
	"ts": 1800000000000
}`,
		// 02 update at exactly +600 — deletes the best ask, inserts a new ask and a new bid
		`{
	"id": "df70b5e7-9e70-4f2a-88d2-eee3488e6bf3",
	"simulation": 1,
	"action": "update",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{
			"asks": [["77208.71", "0"], ["77209.34", "0.005"]],
			"bids": [["77209.33", "1.636034"]],
			"checksum": -1105358608,
			"ts": "1800000000600"
		}
	],
	"ts": 1800000000600
}`,
		// 03 update at +595 — inside the window, so contiguous even though it is not 650
		`{
	"id": "0a1d5f6c-4b82-4f7e-9a30-2c8e5b1d7043",
	"simulation": 1,
	"action": "update",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{
			"asks": [["77209.31", "0"]],
			"bids": [["77206.03", "0"], ["77208.54", "0.01"]],
			"checksum": 733915822,
			"ts": "1800000001195"
		}
	],
	"ts": 1800000001195
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 — 77208.70 canonicalizes to 77208.7
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "77208.71", Quantity: "0.755945"},
				{Price: "77209.31", Quantity: "0.14"},
				{Price: "77209.32", Quantity: "0.259388"},
			},
			Bids: []events.PriceLevel{
				{Price: "77208.7", Quantity: "0.141942"},
				{Price: "77208.54", Quantity: "0.005"},
				{Price: "77206.03", Quantity: "0.000019"},
			},
		},
		{ // after 02 — 77208.71 deleted, 77209.34 and 77209.33 inserted; the rest rests on
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "77209.31", Quantity: "0.14"},
				{Price: "77209.32", Quantity: "0.259388"},
				{Price: "77209.34", Quantity: "0.005"},
			},
			Bids: []events.PriceLevel{
				{Price: "77209.33", Quantity: "1.636034"},
				{Price: "77208.7", Quantity: "0.141942"},
				{Price: "77208.54", Quantity: "0.005"},
				{Price: "77206.03", Quantity: "0.000019"},
			},
		},
		{ // after 03 — one delete per side, and 77208.54 re-sized
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "77209.32", Quantity: "0.259388"},
				{Price: "77209.34", Quantity: "0.005"},
			},
			Bids: []events.PriceLevel{
				{Price: "77209.33", Quantity: "1.636034"},
				{Price: "77208.7", Quantity: "0.141942"},
				{Price: "77208.54", Quantity: "0.01"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 5, Simulation: 1, Price: "77209.32", Quantity: "0.259388"},
			{ExchangeID: 5, Simulation: 1, Price: "77209.34", Quantity: "0.005"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 5, Simulation: 1, Price: "77209.33", Quantity: "1.636034"},
			{ExchangeID: 5, Simulation: 1, Price: "77208.7", Quantity: "0.141942"},
			{ExchangeID: 5, Simulation: 1, Price: "77208.54", Quantity: "0.01"},
		},
	},
}

// Ex5UpdateBeforeSnapshot — ex5 is a delta feed now, so it has a cold start for the first time:
// an update with no baseline is dead-lettered AND asks the collector for a snapshot, which the
// old snapshot-only feed could never do.
var Ex5UpdateBeforeSnapshot = Scenario{
	ExchangeID: 5,
	PairID:     1,
	Sources: []string{
		// 01 update with no baseline — rejected, and never reaches the book
		`{
	"id": "8c59c2f2-2b06-4a0c-8b3e-1a025a859d1a",
	"simulation": 1,
	"action": "update",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{
			"asks": [["77212.00", "9"]],
			"bids": [["77208.00", "9"]],
			"checksum": 12345,
			"ts": "1800000000000"
		}
	],
	"ts": 1800000000000
}`,
		// 02 snapshot — becomes the baseline and answers the outstanding request
		`{
	"id": "bb03003a-84a5-4aef-86f1-c8e78b6b2919",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{
			"asks": [["77210.50", "2.5"], ["77211.00", "1.25"]],
			"bids": [["77210.00", "3"], ["77209.50", "0.5"]],
			"checksum": 23456,
			"ts": "1800000000600"
		}
	],
	"ts": 1800000000600
}`,
		// 03 update at +600 — contiguous now that there is a baseline
		`{
	"id": "5f0b2a91-6c37-4d84-b1e5-7a92c04f8d16",
	"simulation": 1,
	"action": "update",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{
			"asks": [["77210.50", "0"]],
			"bids": [["77209.50", "1.75"]],
			"checksum": 34567,
			"ts": "1800000001200"
		}
	],
	"ts": 1800000001200
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 02 — 01 never rested, so the book is exactly the snapshot
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "77210.5", Quantity: "2.5"},
				{Price: "77211", Quantity: "1.25"},
			},
			Bids: []events.PriceLevel{
				{Price: "77210", Quantity: "3"},
				{Price: "77209.5", Quantity: "0.5"},
			},
		},
		{ // after 03
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks:       []events.PriceLevel{{Price: "77211", Quantity: "1.25"}},
			Bids: []events.PriceLevel{
				{Price: "77210", Quantity: "3"},
				{Price: "77209.5", Quantity: "1.75"},
			},
		},
	},
	WantRejects: []string{"no_baseline"},
	WantControlCommands: []events.ControlCommand{
		{Action: "snapshot_request", Reason: "no_baseline", ExchangeID: 5, PairID: 1, Simulation: 1},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{{ExchangeID: 5, Simulation: 1, Price: "77211", Quantity: "1.25"}},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 5, Simulation: 1, Price: "77210", Quantity: "3"},
			{ExchangeID: 5, Simulation: 1, Price: "77209.5", Quantity: "1.75"},
		},
	},
}

// Ex5JumpTolerance — the rule that exists only for ex5. BOTH edges of the last+650±110 window are
// contiguous, and one millisecond past the edge is a real gap: the book is emptied by a reset,
// the update is dead-lettered, a snapshot is requested, and the resync restores the book. This
// is the scenario that would fail if the tolerance were dropped back to an exact check.
var Ex5JumpTolerance = Scenario{
	ExchangeID: 5,
	PairID:     1,
	Sources: []string{
		// 01 snapshot — baseline at t0
		`{
	"id": "135f6334-909e-4bfe-a7a4-94a7c13a1fbc",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{
			"asks": [["77300.00", "1"], ["77301.00", "2"]],
			"bids": [["77299.00", "3"], ["77298.00", "4"]],
			"checksum": 111,
			"ts": "1800000000000"
		}
	],
	"ts": 1800000000000
}`,
		// 02 update at +540 — the LOW edge of the window, accepted
		`{
	"id": "3dd2f930-7e58-491e-9381-8acc008a8199",
	"simulation": 1,
	"action": "update",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{ "asks": [["77300.00", "1.5"]], "checksum": 222, "ts": "1800000000540" }
	],
	"ts": 1800000000540
}`,
		// 03 update at +760 — the HIGH edge, accepted; also a one-sided (bids-only) update
		`{
	"id": "a6b66bd5-9818-489a-8588-dd0791e8cb44",
	"simulation": 1,
	"action": "update",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{ "bids": [["77298.00", "0"]], "checksum": 333, "ts": "1800000001300" }
	],
	"ts": 1800000001300
}`,
		// 04 update at +761 — one millisecond past the edge, so a gap
		`{
	"id": "bedec549-33fa-4387-b39b-6b941619b164",
	"simulation": 1,
	"action": "update",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{ "asks": [["77302.00", "5"]], "checksum": 444, "ts": "1800000002061" }
	],
	"ts": 1800000002061
}`,
		// 05 snapshot — the resync that answers the request
		`{
	"id": "9c1e7b04-2d58-4a36-8f71-b3e0d5a29c68",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{
			"asks": [["77310.00", "0.5"]],
			"bids": [["77309.00", "0.75"]],
			"checksum": 555,
			"ts": "1800000002400"
		}
	],
	"ts": 1800000002400
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "77300", Quantity: "1"},
				{Price: "77301", Quantity: "2"},
			},
			Bids: []events.PriceLevel{
				{Price: "77299", Quantity: "3"},
				{Price: "77298", Quantity: "4"},
			},
		},
		{ // after 02 — low edge accepted, 77300 re-sized, bids untouched
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "77300", Quantity: "1.5"},
				{Price: "77301", Quantity: "2"},
			},
			Bids: []events.PriceLevel{
				{Price: "77299", Quantity: "3"},
				{Price: "77298", Quantity: "4"},
			},
		},
		{ // after 03 — high edge accepted; the asks side is null on the wire, so it rests as-is
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "77300", Quantity: "1.5"},
				{Price: "77301", Quantity: "2"},
			},
			Bids: []events.PriceLevel{{Price: "77299", Quantity: "3"}},
		},
		{ // after 04 — +761 is a gap, so the reset empties the book
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks:       []events.PriceLevel{},
			Bids:       []events.PriceLevel{},
		},
		{ // after 05 — the resync snapshot restores it
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks:       []events.PriceLevel{{Price: "77310", Quantity: "0.5"}},
			Bids:       []events.PriceLevel{{Price: "77309", Quantity: "0.75"}},
		},
	},
	WantRejects: []string{"sequence_gap"},
	WantControlCommands: []events.ControlCommand{
		{Action: "snapshot_request", Reason: "sequence_gap", ExchangeID: 5, PairID: 1, Simulation: 1},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{{ExchangeID: 5, Simulation: 1, Price: "77310", Quantity: "0.5"}},
		Bids: []events.AggregatedLevel{{ExchangeID: 5, Simulation: 1, Price: "77309", Quantity: "0.75"}},
	},
}

// Ex5MultiBookFrame — one Kafka record whose `data` array carries two book objects becomes two
// independent events, each with its own ts and event time. No other exchange in the suite can
// fan one record out into several events, so this is the only place that wiring is exercised.
// Both elements are snapshots, which job 2 orders by "seq must move forward" with no jump rule —
// so the window never applies here.
var Ex5MultiBookFrame = Scenario{
	ExchangeID: 5,
	PairID:     1,
	Sources: []string{
		// 01 one book
		`{
	"id": "34a00995-a54f-46c4-8363-ece4ce6dc058",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{ "asks": [["77400.00", "1"]], "bids": [["77399.00", "2"]], "checksum": 11, "ts": "1800000000000" }
	],
	"ts": 1800000000000
}`,
		// 02 two books in one record
		`{
	"id": "79df1b2e-f1a8-4a65-91e3-424e87fab74a",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{ "asks": [["77401.00", "0.5"]], "bids": [["77398.00", "1.5"]], "checksum": 22, "ts": "1800000000600" },
		{ "asks": [["77402.00", "0.25"]], "bids": [["77397.00", "3"]], "checksum": 33, "ts": "1800000001200" }
	],
	"ts": 1800000001200
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{{Price: "77400", Quantity: "1"}},
			Bids:       []events.PriceLevel{{Price: "77399", Quantity: "2"}},
		},
		{ // after the first book of 02
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{{Price: "77401", Quantity: "0.5"}},
			Bids:       []events.PriceLevel{{Price: "77398", Quantity: "1.5"}},
		},
		{ // after the second book of 02 — same record, its own snapshot
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks:       []events.PriceLevel{{Price: "77402", Quantity: "0.25"}},
			Bids:       []events.PriceLevel{{Price: "77397", Quantity: "3"}},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{{ExchangeID: 5, Simulation: 1, Price: "77402", Quantity: "0.25"}},
		Bids: []events.AggregatedLevel{{ExchangeID: 5, Simulation: 1, Price: "77397", Quantity: "3"}},
	},
}

// Ex5NoiseFrames — the parser's whitelist is strict about wire TYPES and shape, not just about
// the action. Note what is NO LONGER noise: `action: "update"` is a first-class frame now, and
// `seq`/`pseq` are not read at all, so their wire types cannot reject anything.
var Ex5NoiseFrames = Scenario{
	ExchangeID: 5,
	PairID:     1,
	Sources: []string{
		// 01 snapshot
		`{
	"id": "2e4f7086-2462-416a-b74e-45240c3170f0",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{ "asks": [["77500.00", "1"]], "bids": [["77499.00", "2"]], "checksum": 1, "ts": "1800000000000" }
	],
	"ts": 1800000000000
}`,
		// 02 an action bitget does not send on this channel
		`{
	"id": "c402ced8-5299-4049-9e82-58d22c06c34b",
	"simulation": 1,
	"action": "delete",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{ "asks": [["77501.00", "1"]], "bids": [["77498.00", "2"]], "checksum": 2, "ts": "1800000000100" }
	],
	"ts": 1800000000100
}`,
		// 03 inner ts as a number — the outer ts is one, the inner one never is
		`{
	"id": "30411edc-d74f-473b-bd33-ce9a7fbfbd51",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{ "asks": [["77502.00", "1"]], "bids": [["77497.00", "2"]], "checksum": 3, "ts": 1800000000200 }
	],
	"ts": 1800000000200
}`,
		// 04 data as an object rather than the array it always is
		`{
	"id": "853918ee-c9e4-4476-814c-f9072c491307",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": { "asks": [["77503.00", "1"]], "bids": [["77496.00", "2"]], "checksum": 4, "ts": "1800000000300" },
	"ts": 1800000000300
}`,
		// 05 no instId, so there is no market key
		`{
	"id": "ce6ab79c-f3e8-4f9c-8b3a-f5351f73465a",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "sp", "channel": "depth", "params": { "scale": "0.01" } },
	"data": [
		{ "asks": [["77504.00", "1"]], "bids": [["77495.00", "2"]], "checksum": 5, "ts": "1800000000400" }
	],
	"ts": 1800000000400
}`,
		// 06 a market ex5 has no exchange_markets row for
		`{
	"id": "64d64024-2400-4667-aa15-7d6629b5f52d",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "sp", "channel": "depth", "instId": "FOOBARUSDT", "params": { "scale": "0.01" } },
	"data": [
		{ "asks": [["1.5", "10"]], "bids": [["1.4", "10"]], "checksum": 6, "ts": "1800000000500" }
	],
	"ts": 1800000000500
}`,
		// 07 numeric levels — ex5's wire is string pairs, so the whole frame is unparseable
		`{
	"id": "52bc814f-db21-4d8e-946e-63ae72068233",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{ "asks": [[77505, 1]], "bids": [[77494, 2]], "checksum": 7, "ts": "1800000000600" }
	],
	"ts": 1800000000600
}`,
		// 08 neither side present — a book object with no book in it
		`{
	"id": "6d1a3c85-9f47-42b0-8e63-0c25a7d1f984",
	"simulation": 1,
	"action": "update",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{ "checksum": 8, "ts": "1800000000700" }
	],
	"ts": 1800000000700
}`,
		// 09 snapshot — 02 through 08 emitted nothing, so this is still forward of 01
		`{
	"id": "7e93b0d2-5c18-4a76-9b42-e1f6038c5a27",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{
			"asks": [["77501.00", "0.4"], ["77505.00", "1.1"]],
			"bids": [["77498.00", "0.9"]],
			"checksum": 9,
			"ts": "1800000001000"
		}
	],
	"ts": 1800000001000
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{{Price: "77500", Quantity: "1"}},
			Bids:       []events.PriceLevel{{Price: "77499", Quantity: "2"}},
		},
		{ // after 09
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "77501", Quantity: "0.4"},
				{Price: "77505", Quantity: "1.1"},
			},
			Bids: []events.PriceLevel{{Price: "77498", Quantity: "0.9"}},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 5, Simulation: 1, Price: "77501", Quantity: "0.4"},
			{ExchangeID: 5, Simulation: 1, Price: "77505", Quantity: "1.1"},
		},
		Bids: []events.AggregatedLevel{{ExchangeID: 5, Simulation: 1, Price: "77498", Quantity: "0.9"}},
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
	"id": "6af630ff-7a3d-4c79-a35d-928349ec1178",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{
			"asks": [["77600.117", "0.3"], ["77600.119", "0.2"], ["77601.50", "0.000000004"]],
			"bids": [["77599.99999", "0.12345678999"], ["77599.999", "1.5"]],
			"checksum": 101,
			"ts": "1800000000000"
		}
	],
	"ts": 1800000000000
}`,
		// 02 snapshot — every ask is dust, so the side comes out empty
		`{
	"id": "d59edd13-8182-4b1f-80cf-b12bb92a1776",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{
			"asks": [["77602.001", "0.000000009"]],
			"bids": [["77598.5", "0.4"]],
			"checksum": 102,
			"ts": "1800000000600"
		}
	],
	"ts": 1800000000600
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 — .117 and .119 merged at .11 with 0.5; the two bids merged at 77599.99 and
			// their exact sum 1.62345678999 truncated once, to 1.62345678
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{{Price: "77600.11", Quantity: "0.5"}},
			Bids:       []events.PriceLevel{{Price: "77599.99", Quantity: "1.62345678"}},
		},
		{ // after 02 — the only ask truncated to zero quantity, so nothing rests on that side
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{},
			Bids:       []events.PriceLevel{{Price: "77598.5", Quantity: "0.4"}},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{},
		Bids: []events.AggregatedLevel{{ExchangeID: 5, Simulation: 1, Price: "77598.5", Quantity: "0.4"}},
	},
}

// Ex5RestSnapshotResync — the SECOND stream on `ex5-raw` (added 2026-08-23): bitget's REST depth
// response, which NiFi tags `action: "snapshot"` and stamps with the market as a top-level
// `pair`. It is the same exchange but a different shape on every axis that matters — `data` is a
// single OBJECT rather than an array, the sides are `a`/`b` rather than `asks`/`bids`, and the
// levels are JSON NUMBERS rather than string pairs — so `action` alone cannot tell the two
// streams apart and the parser discriminates on the shape of `data` (same trap as ex1/ex2).
//
// The scenario runs it where it actually appears: a WS gap empties the book and asks the control
// plane for a snapshot, and the REST body is what answers.
//
// REVISED 2026-08-23 (2) to the shape measured on the live feed, and it is now the regression
// test for the resync loop. The REST `data.ts` is BEHIND the WS update that preceded it (source
// 04 vs 03) — the live case 57% of the time — and source 05 is an ordinary WS tick that is NOT
// `REST ts + 650`. Both were fatal under the old sequencing: the REST body seeded the window from
// the wrong clock, so the next update gapped, emptied the book and asked again, ~22 times a
// minute. Null-seq fixes it by never comparing the two clocks — job 2 orders the REST body by
// event time and lets source 05 re-anchor the baseline. ONE reject and ONE control command is
// the whole assertion: a second of either means the loop is back.
var Ex5RestSnapshotResync = Scenario{
	ExchangeID: 5,
	PairID:     1,
	Sources: []string{
		// 01 WS snapshot — the baseline
		`{
	"id": "b1d0a5c8-7e42-4f19-9c3a-6d51e0b4a772",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{
			"asks": [["77300.00", "1"], ["77301.00", "2"]],
			"bids": [["77299.00", "3"]],
			"checksum": 111,
			"ts": "1800000000000"
		}
	],
	"ts": 1800000000000
}`,
		// 02 WS update at +600 — accepted, re-sizes the best ask
		`{
	"id": "2f8c4b16-93ad-4e70-8b25-1c7fa9d6e084",
	"simulation": 1,
	"action": "update",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{ "asks": [["77300.00", "1.25"]], "checksum": 222, "ts": "1800000000600" }
	],
	"ts": 1800000000600
}`,
		// 03 WS update at +1400 — far outside the window, so a gap: reset, dead-letter, and ask
		`{
	"id": "7a3e9d52-c018-4b6f-a94d-58e2b0f31c67",
	"simulation": 1,
	"action": "update",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{ "asks": [["77302.00", "5"]], "checksum": 333, "ts": "1800000002000" }
	],
	"ts": 1800000002000
}`,
		// 04 REST snapshot — the resync answer, in the OTHER wire shape. Note `pair` instead of
		// arg.instId, `data` an object, `a`/`b`, and numeric levels: 77311 is an integer literal
		// and 77310.5 has one decimal place, so both prove BigDecimal-from-the-literal rather
		// than a double round-trip or an invented trailing zero.
		//
		// Its `ts` is 50 ms BEHIND source 03 — the endpoint's clock, not the WS one. Because the
		// parser leaves the sequence id null this is never compared against a WS ts, and job 2
		// accepts it on the resync exemption.
		`{
	"id": "c5091ef7-4b8a-4d63-9e21-0f7c3a8b5d94",
	"simulation": 1,
	"code": "00000",
	"msg": "success",
	"requestTime": 1800000001948,
	"data": {
		"a": [[77310.5, 0.5], [77311, 1.25]],
		"b": [[77309.75, 2], [77308, 0.125]],
		"ts": "1800000001950"
	},
	"pair": "BTCUSDT",
	"action": "snapshot"
}`,
		// 05 WS update on the WS clock — deliberately NOT `REST ts + 650` (that would be
		// ...002600). `baselinePending` adopts its ts as the fresh baseline unconditionally, so
		// the two clocks never meet.
		`{
	"id": "e42b7c30-8d95-4a1e-b6f8-27d5019ac3be",
	"simulation": 1,
	"action": "update",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{
			"asks": [["77310.50", "0"]],
			"bids": [["77307.00", "3"]],
			"checksum": 444,
			"ts": "1800000002610"
		}
	],
	"ts": 1800000002610
}`,
		// 06 WS update at +750 from 05 — contiguity has resumed from the WS clock, and this is
		// the upper live cluster the old 600 ± 10 window used to dead-letter.
		`{
	"id": "0be3f157-6c24-4a89-9d10-84f2ca7b3e56",
	"simulation": 1,
	"action": "update",
	"arg": { "instType": "sp", "channel": "depth", "instId": "BTCUSDT", "params": { "scale": "0.01" } },
	"data": [
		{ "asks": [["77311.00", "2"]], "checksum": 555, "ts": "1800000003360" }
	],
	"ts": 1800000003360
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "77300", Quantity: "1"},
				{Price: "77301", Quantity: "2"},
			},
			Bids: []events.PriceLevel{{Price: "77299", Quantity: "3"}},
		},
		{ // after 02 — +600, best ask re-sized
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "77300", Quantity: "1.25"},
				{Price: "77301", Quantity: "2"},
			},
			Bids: []events.PriceLevel{{Price: "77299", Quantity: "3"}},
		},
		{ // after 03 — the gap's reset empties the book
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks:       []events.PriceLevel{},
			Bids:       []events.PriceLevel{},
		},
		{ // after 04 — the REST body restores it; the numeric levels land as plain decimals, and
			// the event time steps BACK to 08:00:01Z because that is the other clock
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "77310.5", Quantity: "0.5"},
				{Price: "77311", Quantity: "1.25"},
			},
			Bids: []events.PriceLevel{
				{Price: "77309.75", Quantity: "2"},
				{Price: "77308", Quantity: "0.125"},
			},
		},
		{ // after 05 — the baseline re-anchors: 77310.5 deleted, 77307 inserted
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks:       []events.PriceLevel{{Price: "77311", Quantity: "1.25"}},
			Bids: []events.PriceLevel{
				{Price: "77309.75", Quantity: "2"},
				{Price: "77308", Quantity: "0.125"},
				{Price: "77307", Quantity: "3"},
			},
		},
		{ // after 06 — +750 accepted, 77311 re-sized
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:03Z",
			Asks:       []events.PriceLevel{{Price: "77311", Quantity: "2"}},
			Bids: []events.PriceLevel{
				{Price: "77309.75", Quantity: "2"},
				{Price: "77308", Quantity: "0.125"},
				{Price: "77307", Quantity: "3"},
			},
		},
	},
	WantRejects: []string{"sequence_gap"},
	WantControlCommands: []events.ControlCommand{
		{Action: "snapshot_request", Reason: "sequence_gap", ExchangeID: 5, PairID: 1, Simulation: 1},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{{ExchangeID: 5, Simulation: 1, Price: "77311", Quantity: "2"}},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 5, Simulation: 1, Price: "77309.75", Quantity: "2"},
			{ExchangeID: 5, Simulation: 1, Price: "77308", Quantity: "0.125"},
			{ExchangeID: 5, Simulation: 1, Price: "77307", Quantity: "3"},
		},
	},
}
