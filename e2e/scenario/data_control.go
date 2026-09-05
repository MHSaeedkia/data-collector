package scenario

import "orderbook-e2e/events"

// Control-plane scenarios. Every other data_*.go file is organized by exchange,
// because what it exercises is that exchange's wire quirks. These two are
// organized around one cross-cutting feature instead: the `control-plane` topic
// job 2 writes to when a market's stream goes untrustworthy, asking NiFi to
// re-send a snapshot.
//
// The rest of the suite already covers a SINGLE request — eight scenarios end on
// one `no_baseline` or `sequence_gap` and now declare the command it produced.
// What only these two reach is the part of the rule that a single episode cannot
// show: that a command is sent once per EPISODE and that a resync re-arms the
// next one. Both run the loop the feature exists for, end to end — break the
// book, watch the request go out, feed the snapshot NiFi would have sent back,
// and confirm the pipeline both recovers and is willing to ask again. Both are
// on ex6/bybit — the ex1-flavored pair that used to sit alongside them (a
// null-sequence REST snapshot whose offset is adopted by the next WS delta) was
// removed 2026-09-02, since nobitex's WS pushes are snapshots now, not deltas
// (see NobitexParser's javadoc), and that code path can no longer be reached
// through this exchange.

// ControlEx6GapResyncGap — two full gap episodes on one bybit stream, with a
// re-sync between them.
//
// The first gap asks for a snapshot; the update held behind it does NOT ask
// again, because nothing has changed about why the stream is untrustworthy. The
// re-sync snapshot both restores the book and re-arms the request, so the second
// gap is a new episode and asks again. Two gaps, four dead letters, two
// commands.
//
// It is deliberately a superset of Ex6SequenceGap: the first six sources are
// that scenario's, so a failure in the first half points at the gap rule and a
// failure only in the second half points at the control plane's episode state.
var ControlEx6GapResyncGap = Scenario{
	ExchangeID: 6,
	PairID:     1,
	Sources: []string{
		// 01 snapshot — the baseline
		`{
	"id": "aaec49d0-31f7-41d6-a972-5e7b65f4af5d",
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
	"id": "161a9664-7ce3-4121-997e-753e98bf4acc",
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
		// 03 delta, u jumps 301 -> 305 — gap #1, asks NiFi for a snapshot
		`{
	"id": "fb0e67cf-4840-4408-b3e2-507deb79894d",
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
		// 04 delta while still waiting — rejected, and must NOT ask again
		`{
	"id": "6d42c818-5454-4015-add3-492e7ad03fa4",
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
		// 05 snapshot — the snapshot the request asked for; re-syncs and re-arms
		`{
	"id": "285df032-33bf-493b-a07a-a297cbaa138e",
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
	"id": "d5f16398-1d02-4dba-a0a0-fd98ac0f649f",
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
		// 07 delta, u jumps 401 -> 405 — gap #2, a NEW episode, asks again
		`{
	"id": "da8eec4b-7f5c-4eb9-96a0-e34e9630b92d",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000006006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"b": [["62895", "1"]],
		"a": [["62912", "0.25"]],
		"u": 405,
		"seq": 111416318700
	},
	"cts": 1800000006000
}`,
		// 08 delta while waiting on the second request — again no third command
		`{
	"id": "34585425-9250-472b-b57e-4fd0e5356272",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000007006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"b": [["62894", "1"]],
		"a": [["62913", "0.25"]],
		"u": 406,
		"seq": 111416318710
	},
	"cts": 1800000007000
}`,
		// 09 snapshot — second re-sync
		`{
	"id": "58cf0da7-1457-40e1-b45e-d6c7abbf9004",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000008006,
	"type": "snapshot",
	"data": {
		"s": "BTCUSDT",
		"b": [["62919", "2"]],
		"a": [["62920", "1"]],
		"u": 500,
		"seq": 111416318800
	},
	"cts": 1800000008000
}`,
		// 10 delta, contiguous with the second re-sync
		`{
	"id": "ab69067c-de6f-4bd6-b2fd-47eba8a8bdb8",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000009006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"a": [["62921", "0.25"]],
		"u": 501,
		"seq": 111416318810
	},
	"cts": 1800000009000
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
		{ // the reset for gap #1 — 03's own levels never reached the book
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
		{ // after 06
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
		{ // the reset for gap #2 — the second episode empties the book again
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:06Z",
			Asks:       []events.PriceLevel{},
			Bids:       []events.PriceLevel{},
		},
		{ // after 09 — 08 was rejected, so the book is exactly the second re-sync
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:08Z",
			Asks:       []events.PriceLevel{{Price: "62920", Quantity: "1"}},
			Bids:       []events.PriceLevel{{Price: "62919", Quantity: "2"}},
		},
		{ // after 10 — contiguity resumed from the second snapshot's u
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:09Z",
			Asks: []events.PriceLevel{
				{Price: "62920", Quantity: "1"},
				{Price: "62921", Quantity: "0.25"},
			},
			Bids: []events.PriceLevel{{Price: "62919", Quantity: "2"}},
		},
	},
	WantRejects: []string{"sequence_gap", "awaiting_snapshot", "sequence_gap", "awaiting_snapshot"},
	// Four dead letters, two commands. The count is the whole point: one per
	// episode, and the second only because a snapshot re-synced in between.
	WantControlCommands: []events.ControlCommand{
		{Action: "snapshot_request", Reason: "sequence_gap", ExchangeID: 6, PairID: 1, Simulation: 1},
		{Action: "snapshot_request", Reason: "sequence_gap", ExchangeID: 6, PairID: 1, Simulation: 1},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 6, Simulation: 1, Price: "62920", Quantity: "1"},
			{ExchangeID: 6, Simulation: 1, Price: "62921", Quantity: "0.25"},
		},
		Bids: []events.AggregatedLevel{{ExchangeID: 6, Simulation: 1, Price: "62919", Quantity: "2"}},
	},
}

// ControlEx1NoBaselineThenGap and ControlEx1LaggingRestResync were removed
// 2026-09-02: both were built on nobitex WS pushes being deltas (a WS "update"
// with no_baseline / sequence_gap), which is no longer true — see NobitexParser's
// javadoc. A WS snapshot with no prior baseline is simply accepted (no
// no_baseline path exists any more), and there is no jump check to gap on. The
// generic episode-per-request / resync-clears-the-flag machinery these exercised
// is still covered here by the ex6 pair below, and at the unit level by
// TypeValidateFunctionTest's ex6-labeled cases.

// ControlEx6StaleResyncAccepted re-syncs with a snapshot the sequenced ordering guard
// (`seq <= lastSeq`) would have THROWN AWAY, which is the case the deadlock fixed on 2026-08-19
// lived in: the guard rejected the answer to the request, nothing cleared `snapshotRequested`, so
// no further command was ever sent and the market stayed dark until the job restarted.
//
// It asserts two things a passing run must show together: the resync snapshot came out as a
// SNAPSHOT rather than a rejection, and the episode really closed — proven by a later gap opening a
// new one and asking again. Without the fix it fails twice over: the resync appears in WantRejects
// instead of WantSnapshots, and the second command never arrives. `Ex8StaleDuplicate` remains the
// negative control — the same guard with no request outstanding, where a stale snapshot must still
// be rejected.

// ControlEx6StaleResyncAccepted — the resync snapshot's offset is BEHIND the
// book it is replacing, and it is accepted anyway.
//
// On bybit the resync arrives with its own `u`, and nothing says it must be
// ahead of the deltas that were flowing before the gap. When it is not, the
// `stale_or_duplicate` guard used to eat it. It must not here: the gap already
// emptied the book downstream via the reset, so there is no good book for that
// guard to protect and an old book beats no book.
//
// 04 is the flood check (an update held behind the gap must not ask again) and
// 07 is the re-arm check (a second gap is a new episode and must).
var ControlEx6StaleResyncAccepted = Scenario{
	ExchangeID: 6,
	PairID:     1,
	Sources: []string{
		// 01 snapshot — the baseline, u = 300
		`{
	"id": "3d85bcc0-9cbd-444a-9b15-8a4303c23d32",
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
		// 02 delta, contiguous — u = 301, the offset the gap will be measured from
		`{
	"id": "50497c60-8b51-4754-9b5b-b1713a93a390",
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
		// 03 delta, u jumps 301 -> 305 — gap, asks for a snapshot, empties the book
		`{
	"id": "0d1841fc-2492-4e1b-95ad-1c6e5ced0d75",
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
		// 04 delta while waiting — rejected, and must NOT ask again
		`{
	"id": "7efa2d93-3904-4edb-b798-3ef3b7e0224a",
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
		// 05 THE CASE: the requested snapshot comes back at u = 250, BELOW the
		// pre-gap baseline of 301. Pre-fix this was rejected stale_or_duplicate and
		// the key wedged forever. It must be accepted and become the new book.
		`{
	"id": "ac5cb760-a825-4cce-9a26-8eb2e4e1e717",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000004006,
	"type": "snapshot",
	"data": {
		"s": "BTCUSDT",
		"b": [["62880", "3"]],
		"a": [["62881", "2"]],
		"u": 250,
		"seq": 111416318400
	},
	"cts": 1800000004000
}`,
		// 06 delta, contiguous on the RE-ANCHORED baseline — u = 251, not 302
		`{
	"id": "75e8e18c-b111-4e00-9410-dbea2897243f",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000005006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"a": [["62882", "0.5"]],
		"u": 251,
		"seq": 111416318410
	},
	"cts": 1800000005000
}`,
		// 07 delta, u jumps 251 -> 255 — gap #2. The command here is the proof the
		// episode actually closed rather than the flag merely being stuck on.
		`{
	"id": "dae377f8-2959-448c-ae1a-04521a7328ad",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000006006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"b": [["62879", "1"]],
		"a": [["62883", "0.25"]],
		"u": 255,
		"seq": 111416318450
	},
	"cts": 1800000006000
}`,
		// 08 snapshot — second re-sync, this one ahead. Leaves a healthy book.
		`{
	"id": "76988fd3-fdc3-44cc-a88b-a7442ff2fe40",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000007006,
	"type": "snapshot",
	"data": {
		"s": "BTCUSDT",
		"b": [["62890", "1"]],
		"a": [["62891", "1"]],
		"u": 600,
		"seq": 111416318900
	},
	"cts": 1800000007000
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
		{ // the reset for the gap
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks:       []events.PriceLevel{},
			Bids:       []events.PriceLevel{},
		},
		{ // after 05 — the stale resync IS the book. Its absence here is the bug.
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:04Z",
			Asks:       []events.PriceLevel{{Price: "62881", Quantity: "2"}},
			Bids:       []events.PriceLevel{{Price: "62880", Quantity: "3"}},
		},
		{ // after 06
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:05Z",
			Asks: []events.PriceLevel{
				{Price: "62881", Quantity: "2"},
				{Price: "62882", Quantity: "0.5"},
			},
			Bids: []events.PriceLevel{{Price: "62880", Quantity: "3"}},
		},
		{ // the reset for gap #2
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:06Z",
			Asks:       []events.PriceLevel{},
			Bids:       []events.PriceLevel{},
		},
		{ // after 08
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:07Z",
			Asks:       []events.PriceLevel{{Price: "62891", Quantity: "1"}},
			Bids:       []events.PriceLevel{{Price: "62890", Quantity: "1"}},
		},
	},
	WantRejects: []string{"sequence_gap", "awaiting_snapshot", "sequence_gap"},
	// Two episodes, two commands. The second one only exists if the stale resync
	// at 05 was accepted AND cleared the flag.
	WantControlCommands: []events.ControlCommand{
		{Action: "snapshot_request", Reason: "sequence_gap", ExchangeID: 6, PairID: 1, Simulation: 1},
		{Action: "snapshot_request", Reason: "sequence_gap", ExchangeID: 6, PairID: 1, Simulation: 1},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{{ExchangeID: 6, Simulation: 1, Price: "62891", Quantity: "1"}},
		Bids: []events.AggregatedLevel{{ExchangeID: 6, Simulation: 1, Price: "62890", Quantity: "1"}},
	},
}

// ControlEx6LaggingRestResync — the resync snapshot's CLOCK trails the deltas it is
// replacing, and it is accepted anyway.
//
// The event-time twin of ControlEx6StaleResyncAccepted above. That one proves the
// SEQUENCED guard (`seq <= lastSeq`) yields to an outstanding request; this one proves
// the EVENT-TIME guard (`event_time < lastEventTime` on a null-seq snapshot) does too.
// They are separate branches in job 2 and only one of them was ever covered end to end.
//
// Ported from ControlEx1LaggingRestResync, which was deleted 2026-09-02 along with the
// ex1-as-delta-feed premise it rested on. ex6 is the natural home for it now: bybit's
// REST snapshot is the platform's live null-seq resync (`result.u` is on a different
// counter, so the parser drops it), and bybit still sends real WS deltas, so it is the
// one exchange that can still reach this branch at all.
//
// The shape: `result.cts` on the resync at 06 is stamped 08:00:00, BEHIND the 08:00:02
// of the last accepted delta. Because `lastEventTime` only advances on an ACCEPTED
// event, pre-fix this rejected `out_of_order` and every later REST snapshot failed the
// identical comparison — the guard that rejected the resync was the guard that could
// never afterwards be satisfied.
//
// 06 replays 01's body verbatim on purpose: a REST endpoint returning the same book
// with a stamp older than the live deltas is exactly what the skew looks like.
//
// The load-bearing assertions are that the book after 06 APPEARS AT ALL (its absence is
// the bug) and that its event time steps BACKWARDS relative to the reset before it —
// the only scenario in the suite where an emitted book's event time regresses. The two
// commands prove the lagging resync really closed the first episode.
var ControlEx6LaggingRestResync = Scenario{
	ExchangeID: 6,
	PairID:     1,
	Sources: []string{
		// 01 REST snapshot — null-seq (result.u is on a foreign counter), so it only
		// flags a resync. Event time = result.cts = 08:00:00.
		`{
	"id": "c1f4a2d7-3b58-4e19-9a06-8d27e5b3f014",
	"simulation": 1,
	"retCode": 0,
	"retMsg": "OK",
	"result": {
		"s": "BTCUSDT",
		"a": [["62900", "1"], ["62901", "0.5"], ["62910", "0.25"]],
		"b": [["62899", "2"], ["62898", "1.5"], ["62890", "1"]],
		"ts": 1800000000012,
		"u": 38992362,
		"seq": 113017010359,
		"cts": 1800000000000
	},
	"retExtInfo": {},
	"time": 1800000000100,
	"action": "snapshot",
	"pair": "BTCUSDT"
}`,
		// 02 WS delta — baselinePending adopts u = 1000 unconditionally
		`{
	"id": "2a7e9c31-6d04-4f82-b153-90ae7c4d2b68",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000001006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"b": [["62899", "2.5"]],
		"a": [["62901", "0.75"]],
		"u": 1000,
		"seq": 113017010370
	},
	"cts": 1800000001000
}`,
		// 03 WS delta, contiguous — pushes lastEventTime out to 08:00:02, the stamp
		// the resync at 06 has to beat and does not
		`{
	"id": "3b8f0d42-7e15-4a93-c264-01bf8d5e3c79",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000002006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"b": [["62880", "1.1"]],
		"a": [["62920", "0.4"]],
		"u": 1001,
		"seq": 113017010381
	},
	"cts": 1800000002000
}`,
		// 04 WS delta, u jumps 1001 -> 1005 — gap, asks for a snapshot, empties the book
		`{
	"id": "4c90e153-8f26-4ba4-d375-12c09e6f4d8a",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000003006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"b": [["62870", "0.2"]],
		"a": [["62930", "0.1"]],
		"u": 1005,
		"seq": 113017010392
	},
	"cts": 1800000003000
}`,
		// 05 WS delta while waiting — rejected awaiting_snapshot, and must NOT ask again
		`{
	"id": "5da1f264-9037-4cb5-e486-23d1af705e9b",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000004006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"b": [["62869", "0.2"]],
		"a": [["62931", "0.1"]],
		"u": 1006,
		"seq": 113017010403
	},
	"cts": 1800000004000
}`,
		// 06 THE CASE: the REST snapshot answering the request is stamped 08:00:00,
		// BEHIND the 08:00:02 of the last accepted delta. Pre-fix this was rejected
		// out_of_order and the market never recovered.
		`{
	"id": "6eb20375-a148-4dc6-f597-34e2b0816fac",
	"simulation": 1,
	"retCode": 0,
	"retMsg": "OK",
	"result": {
		"s": "BTCUSDT",
		"a": [["62900", "1"], ["62901", "0.5"], ["62910", "0.25"]],
		"b": [["62899", "2"], ["62898", "1.5"], ["62890", "1"]],
		"ts": 1800000000012,
		"u": 38992362,
		"seq": 113017010359,
		"cts": 1800000000000
	},
	"retExtInfo": {},
	"time": 1800000000100,
	"action": "snapshot",
	"pair": "BTCUSDT"
}`,
		// 07 WS delta — adopts u = 2000 as the fresh baseline, healthy again
		`{
	"id": "7fc31486-b259-4ed7-06a8-45f3c1927bbd",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000005006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"b": [["62899", "0.6"]],
		"a": [["62901", "0.1"]],
		"u": 2000,
		"seq": 113017010414
	},
	"cts": 1800000005000
}`,
		// 08 WS delta, u jumps 2000 -> 2005 — gap #2. Its command is the proof the
		// lagging resync at 06 really closed the first episode.
		`{
	"id": "80d42597-c360-4fe8-17b9-5604d2a38cce",
	"simulation": 1,
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1800000006006,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"b": [["62860", "0.2"]],
		"a": [["62940", "0.1"]],
		"u": 2005,
		"seq": 113017010425
	},
	"cts": 1800000006000
}`,
		// 09 REST snapshot — second re-sync, this one ahead. Leaves a clean book.
		`{
	"id": "91e536a8-d471-40f9-28ca-6715e3b49ddf",
	"simulation": 1,
	"retCode": 0,
	"retMsg": "OK",
	"result": {
		"s": "BTCUSDT",
		"a": [["62710", "1.5"], ["62720", "0.25"]],
		"b": [["62700", "1"], ["62690", "2"]],
		"ts": 1800000007012,
		"u": 38992400,
		"seq": 113017010436,
		"cts": 1800000007000
	},
	"retExtInfo": {},
	"time": 1800000007100,
	"action": "snapshot",
	"pair": "BTCUSDT"
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62900", Quantity: "1"},
				{Price: "62901", Quantity: "0.5"},
				{Price: "62910", Quantity: "0.25"},
			},
			Bids: []events.PriceLevel{
				{Price: "62899", Quantity: "2"},
				{Price: "62898", Quantity: "1.5"},
				{Price: "62890", Quantity: "1"},
			},
		},
		{ // after 02 — a delta, so it MERGES into the book above
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "62900", Quantity: "1"},
				{Price: "62901", Quantity: "0.75"},
				{Price: "62910", Quantity: "0.25"},
			},
			Bids: []events.PriceLevel{
				{Price: "62899", Quantity: "2.5"},
				{Price: "62898", Quantity: "1.5"},
				{Price: "62890", Quantity: "1"},
			},
		},
		{ // after 03
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks: []events.PriceLevel{
				{Price: "62900", Quantity: "1"},
				{Price: "62901", Quantity: "0.75"},
				{Price: "62910", Quantity: "0.25"},
				{Price: "62920", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62899", Quantity: "2.5"},
				{Price: "62898", Quantity: "1.5"},
				{Price: "62890", Quantity: "1"},
				{Price: "62880", Quantity: "1.1"},
			},
		},
		{ // the reset for the gap — 04's own levels never reached the book
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:03Z",
			Asks:       []events.PriceLevel{},
			Bids:       []events.PriceLevel{},
		},
		{ // after 06 — the lagging resync IS the book, and its event time goes
			// BACKWARDS relative to the reset above. Its absence here is the bug.
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62900", Quantity: "1"},
				{Price: "62901", Quantity: "0.5"},
				{Price: "62910", Quantity: "0.25"},
			},
			Bids: []events.PriceLevel{
				{Price: "62899", Quantity: "2"},
				{Price: "62898", Quantity: "1.5"},
				{Price: "62890", Quantity: "1"},
			},
		},
		{ // after 07
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:05Z",
			Asks: []events.PriceLevel{
				{Price: "62900", Quantity: "1"},
				{Price: "62901", Quantity: "0.1"},
				{Price: "62910", Quantity: "0.25"},
			},
			Bids: []events.PriceLevel{
				{Price: "62899", Quantity: "0.6"},
				{Price: "62898", Quantity: "1.5"},
				{Price: "62890", Quantity: "1"},
			},
		},
		{ // the reset for gap #2
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:06Z",
			Asks:       []events.PriceLevel{},
			Bids:       []events.PriceLevel{},
		},
		{ // after 09
			ExchangeID: 6,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:07Z",
			Asks: []events.PriceLevel{
				{Price: "62710", Quantity: "1.5"},
				{Price: "62720", Quantity: "0.25"},
			},
			Bids: []events.PriceLevel{
				{Price: "62700", Quantity: "1"},
				{Price: "62690", Quantity: "2"},
			},
		},
	},
	WantRejects: []string{"sequence_gap", "awaiting_snapshot", "sequence_gap"},
	// Two episodes, two commands. The second one only exists if the lagging REST
	// snapshot at 06 was accepted AND cleared the flag.
	WantControlCommands: []events.ControlCommand{
		{Action: "snapshot_request", Reason: "sequence_gap", ExchangeID: 6, PairID: 1, Simulation: 1},
		{Action: "snapshot_request", Reason: "sequence_gap", ExchangeID: 6, PairID: 1, Simulation: 1},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 6, Simulation: 1, Price: "62710", Quantity: "1.5"},
			{ExchangeID: 6, Simulation: 1, Price: "62720", Quantity: "0.25"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 6, Simulation: 1, Price: "62700", Quantity: "1"},
			{ExchangeID: 6, Simulation: 1, Price: "62690", Quantity: "2"},
		},
	},
}
