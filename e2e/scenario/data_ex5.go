// Scenarios for ex5/bitget — REVISED 2026-09-07, when the feed moved back off the price-GROUPED
// `depth` channel onto `books50` and became snapshot-only again.
//
// What makes ex5 different:
//
//   - `data` is an ARRAY of book objects, not a single object. The parser emits one event per
//     element, so ONE Kafka record can produce SEVERAL snapshots (Ex5MultiBookFrame). If any
//     element is malformed the WHOLE record is dropped, including elements already read —
//     accuracy-first, never emit a partial book.
//   - `action` has ONE accepted value, `"snapshot"`; this channel sends no deltas, so any other
//     action is noise and is dropped rather than treated as an update.
//   - The ordering field is `data[i].seq` (an integral JSON number) with jump 0, and the event
//     time is `data[i].ts` — a STRING of epoch millis, unlike the outer `ts` which is a number
//     (1-2 ms later) and is ignored entirely.
//   - `pseq` is on the wire, reads 0 on EVERY captured frame, and is read by nobody. It is not a
//     predecessor pointer, so the sources below all carry 0 — do not "fix" them to chain.
//   - `seq` is a fast underlying book counter that these snapshots are SAMPLED from: over the
//     5-frame live capture it stepped 2816-11122 between frames only 97-351 ms apart. That is why
//     jump 0 is the only possibility, not merely the default, and why the levels below move by
//     large arbitrary seq steps rather than by 1.
//
// Because every frame is a full book with a sequence, job 2 takes its snapshot branch and nothing
// else: `seq <= lastSeq` is stale_or_duplicate (Ex5StaleSeq) and any forward seq is accepted, with
// no contiguity rule. ex5 therefore has NO gap case, NO cold-start case, and can emit NO control
// command — every scenario here asserts an empty control stream. It joins ex3, ex4 and ex9 in that
// group.
//
// Three things that were true between 2026-08-22 and 2026-09-07 are gone, and so are their
// scenarios (26 Ex5UpdateBeforeSnapshot, 27 Ex5JumpTolerance, 31 Ex5RestSnapshotResync — numbers
// retired, not reused, per the convention in scenarios.go):
//
//   - `action: "update"` frames, with qty "0" as a level delete. A qty of "0" on this channel is
//     just an empty level: it rests nowhere, it deletes nothing.
//   - The SECOND stream on `ex5-raw`, bitget's REST depth response (`data` an OBJECT, `a`/`b`
//     NUMERIC sides, an injected `pair`). The poller is gone; the shape is now noise, which
//     Ex5NoiseFrames pins.
//   - The `sequence_jump_tolerance` window. ex5 was the only exchange that ever stamped a nonzero
//     one, so the field is now 0 platform-wide and no scenario exercises it.
//
// Every bitget market in the seed is a USDT market with rebase 0/0, so job 3 is the identity here
// and there is nothing to assert about it — ex1 and ex4 are the only two exchanges that can.
// Pair 1 (BTCUSDT) is price_precision 2 / quantity_precision 8.

package scenario

import "orderbook-e2e/events"

// Ex5SnapshotStream — the happy path: every frame replaces both sides WHOLESALE, so a level that
// rested after 01 and is absent from 02 is gone rather than carried forward. That is the shape of
// the live feed, not an edge case: across the 5-frame capture, levels vanish between consecutive
// snapshots with NO qty-"0" marker anywhere (frame 1 -> 2 silently dropped ask 79429.65 and bid
// 79425.21; frame 4 -> 5 dropped 11 asks and 4 bids). Wholesale replacement is the only correct
// way to apply this feed.
//
// The event time comes off the STRING `ts` inside the book object and the ordering field off
// `seq`. The qty-"0" ask in 02 asserts such a level rests nowhere — ⚠ DEFENSIVE, not observed:
// there was no zero quantity in the 500 captured levels. On a snapshot feed "0" would be an empty
// level, never a delete marker, and this pins that reading in case one ever appears.
var Ex5SnapshotStream = Scenario{
	ExchangeID: 5,
	PairID:     1,
	Sources: []string{
		// 01 snapshot — three levels a side
		`{
	"id": "aed46d6a-f822-458b-820f-3cdb35b2684a",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{
			"asks": [["79427.25", "0.122814"], ["79429.65", "0.000377"], ["79429.75", "0.001992"]],
			"bids": [["79427.24", "0.460398"], ["79426.96", "0.005434"], ["79425.21", "0.000377"]],
			"ts": "1800000000000",
			"seq": 787944892031,
			"pseq": 0
		}
	],
	"ts": 1800000000005
}`,
		// 02 snapshot — a wholly different book. Nothing from 01 survives, 79430.00 canonicalizes
		// to 79430, and the qty-"0" ask never rests.
		`{
	"id": "df70b5e7-9e70-4f2a-88d2-eee3488e6bf3",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{
			"asks": [["79430.00", "1.5"], ["79431.5", "0.25"], ["79432.00", "0"]],
			"bids": [["79429.99", "2"], ["79428.00", "0.75"]],
			"ts": "1800000000600",
			"seq": 787944892099,
			"pseq": 0
		}
	],
	"ts": 1800000000605
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "79427.25", Quantity: "0.122814"},
				{Price: "79429.65", Quantity: "0.000377"},
				{Price: "79429.75", Quantity: "0.001992"},
			},
			Bids: []events.PriceLevel{
				{Price: "79427.24", Quantity: "0.460398"},
				{Price: "79426.96", Quantity: "0.005434"},
				{Price: "79425.21", Quantity: "0.000377"},
			},
		},
		{ // after 02 — every level from 01 is gone; 79432 never rested
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "79430", Quantity: "1.5"},
				{Price: "79431.5", Quantity: "0.25"},
			},
			Bids: []events.PriceLevel{
				{Price: "79429.99", Quantity: "2"},
				{Price: "79428", Quantity: "0.75"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 5, Simulation: 1, Price: "79430", Quantity: "1.5"},
			{ExchangeID: 5, Simulation: 1, Price: "79431.5", Quantity: "0.25"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 5, Simulation: 1, Price: "79429.99", Quantity: "2"},
			{ExchangeID: 5, Simulation: 1, Price: "79428", Quantity: "0.75"},
		},
	},
}

// Ex5StaleSeq — the whole of job 2's rule set for this exchange, in one scenario. A repeated or
// older `seq` is out-of-order arrival and is dead-lettered stale_or_duplicate; 04 pins the other
// half of the snapshot branch, that a forward seq is accepted however far it jumps, because a
// snapshot feed has no contiguity rule to break. Note 02 and 03 carry LATER event times than 01
// and are still rejected: with a non-null sequence id, `seq` is the whole ordering test and `ts`
// is not consulted.
//
// The control stream must be EMPTY. ex5 cannot reach a branch that asks for a snapshot.
var Ex5StaleSeq = Scenario{
	ExchangeID: 5,
	PairID:     1,
	Sources: []string{
		// 01 snapshot — the baseline
		`{
	"id": "135f6334-909e-4bfe-a7a4-94a7c13a1fbc",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{ "asks": [["79500.00", "1"]], "bids": [["79499.00", "2"]], "ts": "1800000000000", "seq": 500, "pseq": 0 }
	],
	"ts": 1800000000005
}`,
		// 02 the same seq again
		`{
	"id": "3dd2f930-7e58-491e-9381-8acc008a8199",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{ "asks": [["79501.00", "1"]], "bids": [["79498.00", "2"]], "ts": "1800000000100", "seq": 500, "pseq": 0 }
	],
	"ts": 1800000000105
}`,
		// 03 an older seq
		`{
	"id": "a6b66bd5-9818-489a-8588-dd0791e8cb44",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{ "asks": [["79502.00", "1"]], "bids": [["79497.00", "2"]], "ts": "1800000000200", "seq": 499, "pseq": 0 }
	],
	"ts": 1800000000205
}`,
		// 04 a far-forward seq — accepted, because a snapshot re-anchors rather than continues
		`{
	"id": "bedec549-33fa-4387-b39b-6b941619b164",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{ "asks": [["79510.00", "0.5"]], "bids": [["79490.00", "1.5"]], "ts": "1800000001000", "seq": 900000, "pseq": 0 }
	],
	"ts": 1800000001005
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{{Price: "79500", Quantity: "1"}},
			Bids:       []events.PriceLevel{{Price: "79499", Quantity: "2"}},
		},
		{ // after 04 — 02 and 03 were dead-lettered before the book builder saw them
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks:       []events.PriceLevel{{Price: "79510", Quantity: "0.5"}},
			Bids:       []events.PriceLevel{{Price: "79490", Quantity: "1.5"}},
		},
	},
	WantRejects: []string{"stale_or_duplicate", "stale_or_duplicate"},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{{ExchangeID: 5, Simulation: 1, Price: "79510", Quantity: "0.5"}},
		Bids: []events.AggregatedLevel{{ExchangeID: 5, Simulation: 1, Price: "79490", Quantity: "1.5"}},
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
	"id": "34a00995-a54f-46c4-8363-ece4ce6dc058",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{ "asks": [["79400.00", "1"]], "bids": [["79399.00", "2"]], "ts": "1800000000000", "seq": 1000, "pseq": 0 }
	],
	"ts": 1800000000005
}`,
		// 02 two books in one record
		`{
	"id": "79df1b2e-f1a8-4a65-91e3-424e87fab74a",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{ "asks": [["79401.00", "0.5"]], "bids": [["79398.00", "1.5"]], "ts": "1800000000600", "seq": 1001, "pseq": 0 },
		{ "asks": [["79402.00", "0.25"]], "bids": [["79397.00", "3"]], "ts": "1800000001200", "seq": 1002, "pseq": 0 }
	],
	"ts": 1800000001205
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{{Price: "79400", Quantity: "1"}},
			Bids:       []events.PriceLevel{{Price: "79399", Quantity: "2"}},
		},
		{ // after the first book of 02
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{{Price: "79401", Quantity: "0.5"}},
			Bids:       []events.PriceLevel{{Price: "79398", Quantity: "1.5"}},
		},
		{ // after the second book of 02 — same record, its own snapshot
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks:       []events.PriceLevel{{Price: "79402", Quantity: "0.25"}},
			Bids:       []events.PriceLevel{{Price: "79397", Quantity: "3"}},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{{ExchangeID: 5, Simulation: 1, Price: "79402", Quantity: "0.25"}},
		Bids: []events.AggregatedLevel{{ExchangeID: 5, Simulation: 1, Price: "79397", Quantity: "3"}},
	},
}

// Ex5NoiseFrames — the parser's whitelist is strict about wire TYPES and shape, not just about the
// action: `seq` must be an integral number and `ts` a string, so a frame that swaps them is
// dropped even though a lenient parse would have produced the same values.
//
// Sources 02 and 09 are the REVISION of 2026-09-07 made testable. `action: "update"` was a
// first-class frame on the `depth` channel and a one-sided body was a legal delta there; on a
// snapshot feed both are noise, and a half book is dropped rather than half-applied because
// applying it would wipe a live side. Source 05 is the retired REST depth body, which the parser
// used to recognise by the shape of its `data` and now does not recognise at all.
var Ex5NoiseFrames = Scenario{
	ExchangeID: 5,
	PairID:     1,
	Sources: []string{
		// 01 snapshot
		`{
	"id": "2e4f7086-2462-416a-b74e-45240c3170f0",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{ "asks": [["79600.00", "1"]], "bids": [["79599.00", "2"]], "ts": "1800000000000", "seq": 2000, "pseq": 0 }
	],
	"ts": 1800000000005
}`,
		// 02 an action this channel does not send any more
		`{
	"id": "c402ced8-5299-4049-9e82-58d22c06c34b",
	"simulation": 1,
	"action": "update",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{ "asks": [["79601.00", "1"]], "bids": [["79598.00", "2"]], "ts": "1800000000100", "seq": 2001, "pseq": 0 }
	],
	"ts": 1800000000105
}`,
		// 03 inner ts as a number — the outer ts is one, the inner one never is
		`{
	"id": "30411edc-d74f-473b-bd33-ce9a7fbfbd51",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{ "asks": [["79602.00", "1"]], "bids": [["79597.00", "2"]], "ts": 1800000000200, "seq": 2002, "pseq": 0 }
	],
	"ts": 1800000000205
}`,
		// 04 seq as a string — the ordering field must be an integral number
		`{
	"id": "853918ee-c9e4-4476-814c-f9072c491307",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{ "asks": [["79603.00", "1"]], "bids": [["79596.00", "2"]], "ts": "1800000000300", "seq": "2003", "pseq": 0 }
	],
	"ts": 1800000000305
}`,
		// 05 the RETIRED REST depth body — `data` an object, `a`/`b` numeric sides, an injected
		// `pair` and no `arg`. The poller is gone, so this shape has no branch any more.
		`{
	"id": "ce6ab79c-f3e8-4f9c-8b3a-f5351f73465a",
	"simulation": 1,
	"code": "00000",
	"msg": "success",
	"requestTime": 1800000000395,
	"data": { "a": [[79604.0, 1.0]], "b": [[79595.0, 2.0]], "ts": "1800000000400" },
	"pair": "BTCUSDT",
	"action": "snapshot"
}`,
		// 06 no instId, so there is no market key
		`{
	"id": "64d64024-2400-4667-aa15-7d6629b5f52d",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50" },
	"data": [
		{ "asks": [["79605.00", "1"]], "bids": [["79594.00", "2"]], "ts": "1800000000500", "seq": 2005, "pseq": 0 }
	],
	"ts": 1800000000505
}`,
		// 07 a market ex5 has no exchange_markets row for
		`{
	"id": "52bc814f-db21-4d8e-946e-63ae72068233",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "FOOBARUSDT" },
	"data": [
		{ "asks": [["1.5", "10"]], "bids": [["1.4", "10"]], "ts": "1800000000600", "seq": 2006, "pseq": 0 }
	],
	"ts": 1800000000605
}`,
		// 08 numeric levels — ex5's wire is string pairs, so the whole frame is unparseable
		`{
	"id": "6d1a3c85-9f47-42b0-8e63-0c25a7d1f984",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{ "asks": [[79607, 1]], "bids": [[79592, 2]], "ts": "1800000000700", "seq": 2007, "pseq": 0 }
	],
	"ts": 1800000000705
}`,
		// 09 asks only — a half book on a snapshot feed would wipe the bid side, so it is dropped
		`{
	"id": "7e93b0d2-5c18-4a76-9b42-e1f6038c5a27",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{ "asks": [["79608.00", "1"]], "ts": "1800000000800", "seq": 2008, "pseq": 0 }
	],
	"ts": 1800000000805
}`,
		// 10 snapshot — 02 through 09 emitted nothing, so this is still forward of 01
		`{
	"id": "0be3f157-6c24-4a89-9d10-84f2ca7b3e56",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{
			"asks": [["79601.00", "0.4"], ["79605.00", "1.1"]],
			"bids": [["79598.00", "0.9"]],
			"ts": "1800000001000",
			"seq": 2010,
			"pseq": 0
		}
	],
	"ts": 1800000001005
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{{Price: "79600", Quantity: "1"}},
			Bids:       []events.PriceLevel{{Price: "79599", Quantity: "2"}},
		},
		{ // after 10
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "79601", Quantity: "0.4"},
				{Price: "79605", Quantity: "1.1"},
			},
			Bids: []events.PriceLevel{{Price: "79598", Quantity: "0.9"}},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 5, Simulation: 1, Price: "79601", Quantity: "0.4"},
			{ExchangeID: 5, Simulation: 1, Price: "79605", Quantity: "1.1"},
		},
		Bids: []events.AggregatedLevel{{ExchangeID: 5, Simulation: 1, Price: "79598", Quantity: "0.9"}},
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
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{
			"asks": [["79700.117", "0.3"], ["79700.119", "0.2"], ["79701.50", "0.000000004"]],
			"bids": [["79699.99999", "0.12345678999"], ["79699.999", "1.5"]],
			"ts": "1800000000000",
			"seq": 3000,
			"pseq": 0
		}
	],
	"ts": 1800000000005
}`,
		// 02 snapshot — every ask is dust, so the side comes out empty
		`{
	"id": "d59edd13-8182-4b1f-80cf-b12bb92a1776",
	"simulation": 1,
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "BTCUSDT" },
	"data": [
		{
			"asks": [["79702.001", "0.000000009"]],
			"bids": [["79698.5", "0.4"]],
			"ts": "1800000000600",
			"seq": 3001,
			"pseq": 0
		}
	],
	"ts": 1800000000605
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 — .117 and .119 merged at .11 with 0.5; the two bids merged at 79699.99 and
			// their exact sum 1.62345678999 truncated once, to 1.62345678
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{{Price: "79700.11", Quantity: "0.5"}},
			Bids:       []events.PriceLevel{{Price: "79699.99", Quantity: "1.62345678"}},
		},
		{ // after 02 — the only ask truncated to zero quantity, so nothing rests on that side
			ExchangeID: 5,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{},
			Bids:       []events.PriceLevel{{Price: "79698.5", Quantity: "0.4"}},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{},
		Bids: []events.AggregatedLevel{{ExchangeID: 5, Simulation: 1, Price: "79698.5", Quantity: "0.4"}},
	},
}
