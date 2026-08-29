// Scenarios for ex9/lbank — a SNAPSHOT-ONLY feed with no sequence field anywhere on the wire.
//
// What makes ex9 different from every other exchange in the suite:
//
//   - Every frame is a WHOLE book under `depth`. There are no deltas, so there is nothing to be
//     contiguous with and no `action`/`type` regime discriminator to read: an accepted frame
//     REPLACES the book outright, and a level that vanishes between two frames is simply gone.
//     ex3/wallex is the only other snapshot-only exchange, and it sends one SIDE per message
//     where ex9 always sends both.
//   - `sequence_id` is NULL and `sequence_jump` is 0 (user decision 2026-08-26). The wire has no
//     counter at all, and `TS` is deliberately NOT re-used as one the way ex5's and ex8's `ts`
//     are — a timestamp-as-sequence imposes a cadence the exchange never promised. So ex9 runs on
//     job 2's EVENT-TIME branch, where the whole test is "not older than the last accepted frame".
//   - `TS` is an ISO-8601 local date-time with NO zone marker (`"2027-01-15T08:00:00.000"`), read
//     as UTC. Every other exchange sends epoch millis. That conversion is asserted here rather
//     than mocked out: unlike ex3, ex9 has a real wire clock, so no scenario sets IgnoreEventTime
//     and the timestamps below are chosen a second or more apart so each snapshot's EventTime is
//     a DIFFERENT RFC3339 value.
//   - The market key is `pair`, lowercase with an underscore (`btc_usdt`) — the only exchange
//     that spells it that way, and nothing in job 1 normalizes case. Ex9NoiseFrames pins that.
//
// ex9 can reach exactly one reject reason, `out_of_order`, and NO control command: job 2 only
// asks for a snapshot on `no_baseline` or `sequence_gap`, and both live in the update branch a
// null-seq feed never enters. An empty WantControlCommands is the assertion that it stays that way.
//
// Pair 1 (BTC/USDT) is price_precision 2 / quantity_precision 8, and ex9's rebase is 0/0, so
// every number that moves in these scenarios moved in job 4 and nowhere else.

package scenario

import "orderbook-e2e/events"

// Ex9SnapshotStream — the defining property of a snapshot-only feed: each frame replaces the
// book whole, so a level present in one frame and absent from the next is DELETED with no
// qty-"0" marker anywhere. Also the end-to-end proof that the zone-less `TS` is read as UTC:
// the three frames land on three different RFC3339 seconds.
var Ex9SnapshotStream = Scenario{
	ExchangeID: 9,
	PairID:     1,
	Sources: []string{
		// 01 snapshot
		`{
	"id": "5a1f6c02-9b47-4d38-8c11-0e7f2a95d413",
	"simulation": 1,
	"depth": {
		"asks": [
			["79654.45", "1.04718"],
			["79654.46", "0.00083"],
			["79654.47", "0.00016"]
		],
		"bids": [
			["79654.44", "2.89166"],
			["79654.43", "0.00083"],
			["79654.42", "0.00016"]
		]
	},
	"SERVER": "V3",
	"count": 200,
	"limit": 50,
	"type": "fdepth",
	"pair": "btc_usdt",
	"TS": "2027-01-15T08:00:00.000"
}`,
		// 02 snapshot — drops the outermost level on each side, moves a quantity, adds a new price
		`{
	"id": "c74b8e51-2d90-4a6f-b3e8-19c05fd7a862",
	"simulation": 1,
	"depth": {
		"asks": [
			["79654.45", "0.50000"],
			["79654.46", "0.00083"],
			["79654.90", "1.20000"]
		],
		"bids": [
			["79654.44", "3.10000"],
			["79654.43", "0.00083"],
			["79654.00", "5.00000"]
		]
	},
	"SERVER": "V3",
	"count": 200,
	"limit": 50,
	"type": "fdepth",
	"pair": "btc_usdt",
	"TS": "2027-01-15T08:00:01.500"
}`,
		// 03 snapshot — a much thinner book; everything not restated here is gone
		`{
	"id": "0fbd3a97-6e25-41c8-9a70-4d81b6e2c50f",
	"simulation": 1,
	"depth": {
		"asks": [
			["79655.00", "2.00000"]
		],
		"bids": [
			["79654.44", "3.10000"],
			["79654.43", "0.00083"]
		]
	},
	"SERVER": "V3",
	"count": 200,
	"limit": 50,
	"type": "fdepth",
	"pair": "btc_usdt",
	"TS": "2027-01-15T08:00:03.250"
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 9,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "79654.45", Quantity: "1.04718"},
				{Price: "79654.46", Quantity: "0.00083"},
				{Price: "79654.47", Quantity: "0.00016"},
			},
			Bids: []events.PriceLevel{
				{Price: "79654.44", Quantity: "2.89166"},
				{Price: "79654.43", Quantity: "0.00083"},
				{Price: "79654.42", Quantity: "0.00016"},
			},
		},
		{ // after 02 — 79654.47 and 79654.42 are gone because the snapshot did not restate them
			ExchangeID: 9,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "79654.45", Quantity: "0.5"},
				{Price: "79654.46", Quantity: "0.00083"},
				{Price: "79654.9", Quantity: "1.2"},
			},
			Bids: []events.PriceLevel{
				{Price: "79654.44", Quantity: "3.1"},
				{Price: "79654.43", Quantity: "0.00083"},
				{Price: "79654", Quantity: "5"},
			},
		},
		{ // after 03
			ExchangeID: 9,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:03Z",
			Asks: []events.PriceLevel{
				{Price: "79655", Quantity: "2"},
			},
			Bids: []events.PriceLevel{
				{Price: "79654.44", Quantity: "3.1"},
				{Price: "79654.43", Quantity: "0.00083"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 9, Simulation: 1, Price: "79655", Quantity: "2"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 9, Simulation: 1, Price: "79654.44", Quantity: "3.1"},
			{ExchangeID: 9, Simulation: 1, Price: "79654.43", Quantity: "0.00083"},
		},
	},
}

// Ex9StaleSnapshotReplay — the whole reason ex9 needs no sequence id. A replayed older book is
// caught by event time alone and dead-lettered `out_of_order`, leaving the newer book untouched;
// the next in-order frame is accepted normally, so one stale replay does not wedge the key.
// This is the ex1 REST-replay guard, reached by an exchange that has nothing BUT null-seq frames.
var Ex9StaleSnapshotReplay = Scenario{
	ExchangeID: 9,
	PairID:     1,
	Sources: []string{
		// 01 snapshot
		`{
	"id": "9d2e4b6a-51c7-4f03-8b29-6ea7c418d095",
	"simulation": 1,
	"depth": {
		"asks": [
			["79700.00", "1.00000"],
			["79701.00", "2.00000"]
		],
		"bids": [
			["79699.00", "3.00000"],
			["79698.00", "4.00000"]
		]
	},
	"SERVER": "V3",
	"count": 200,
	"limit": 50,
	"type": "fdepth",
	"pair": "btc_usdt",
	"TS": "2027-01-15T08:00:00.000"
}`,
		// 02 snapshot, newer
		`{
	"id": "e81a70cf-3b95-42d6-a0f4-7c25b93e6d18",
	"simulation": 1,
	"depth": {
		"asks": [
			["79710.00", "1.50000"]
		],
		"bids": [
			["79709.00", "2.50000"]
		]
	},
	"SERVER": "V3",
	"count": 200,
	"limit": 50,
	"type": "fdepth",
	"pair": "btc_usdt",
	"TS": "2027-01-15T08:00:02.000"
}`,
		// 03 the frame from 01 again, replayed after the newer one — strictly older, so rejected
		`{
	"id": "4c60d92b-8f17-4ae5-93c8-2b0e5a71f634",
	"simulation": 1,
	"depth": {
		"asks": [
			["79700.00", "1.00000"],
			["79701.00", "2.00000"]
		],
		"bids": [
			["79699.00", "3.00000"],
			["79698.00", "4.00000"]
		]
	},
	"SERVER": "V3",
	"count": 200,
	"limit": 50,
	"type": "fdepth",
	"pair": "btc_usdt",
	"TS": "2027-01-15T08:00:00.000"
}`,
		// 04 snapshot, newer again — proves the rejection did not leave the key stuck
		`{
	"id": "b3572e08-c41d-49fa-8e60-93af17c2d85b",
	"simulation": 1,
	"depth": {
		"asks": [
			["79720.00", "0.75000"]
		],
		"bids": [
			["79719.00", "1.25000"]
		]
	},
	"SERVER": "V3",
	"count": 200,
	"limit": 50,
	"type": "fdepth",
	"pair": "btc_usdt",
	"TS": "2027-01-15T08:00:04.000"
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 9,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "79700", Quantity: "1"},
				{Price: "79701", Quantity: "2"},
			},
			Bids: []events.PriceLevel{
				{Price: "79699", Quantity: "3"},
				{Price: "79698", Quantity: "4"},
			},
		},
		{ // after 02
			ExchangeID: 9,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks: []events.PriceLevel{
				{Price: "79710", Quantity: "1.5"},
			},
			Bids: []events.PriceLevel{
				{Price: "79709", Quantity: "2.5"},
			},
		},
		// 03 produced NOTHING — the old book never reached job 5.
		{ // after 04
			ExchangeID: 9,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:04Z",
			Asks: []events.PriceLevel{
				{Price: "79720", Quantity: "0.75"},
			},
			Bids: []events.PriceLevel{
				{Price: "79719", Quantity: "1.25"},
			},
		},
	},
	WantRejects: []string{"out_of_order"},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 9, Simulation: 1, Price: "79720", Quantity: "0.75"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 9, Simulation: 1, Price: "79719", Quantity: "1.25"},
		},
	},
}

// Ex9DuplicateTimestamp — pins the deliberate edge of the ordering guard (user decision
// 2026-08-26): job 2 compares `event_time < lastEventTime`, STRICTLY older, so frames sharing a
// `TS` are all accepted and the book follows the last one in. It is written down as a scenario
// rather than left implicit because it is the one case where "ordered by timestamp" does NOT
// mean "deduplicated by timestamp" — 03 walks the book back to 01's levels with no rejection
// anywhere. Tightening the guard to `<=` would break this test, which is the point: that change
// would also hit ex3 and the ex1/ex2 REST snapshots, so it must never be made for ex9 alone.
var Ex9DuplicateTimestamp = Scenario{
	ExchangeID: 9,
	PairID:     1,
	Sources: []string{
		// 01 snapshot
		`{
	"id": "7f19c3d5-4a82-4b06-9e71-58c2d094a3e7",
	"simulation": 1,
	"depth": {
		"asks": [["79800.00", "1.00000"]],
		"bids": [["79799.00", "2.00000"]]
	},
	"SERVER": "V3",
	"count": 200,
	"limit": 50,
	"type": "fdepth",
	"pair": "btc_usdt",
	"TS": "2027-01-15T08:00:00.000"
}`,
		// 02 a DIFFERENT book on the SAME TS — accepted, not stale
		`{
	"id": "2ab7e604-9d31-4c58-83f2-16b0e7a5c9d4",
	"simulation": 1,
	"depth": {
		"asks": [["79810.00", "3.00000"]],
		"bids": [["79809.00", "4.00000"]]
	},
	"SERVER": "V3",
	"count": 200,
	"limit": 50,
	"type": "fdepth",
	"pair": "btc_usdt",
	"TS": "2027-01-15T08:00:00.000"
}`,
		// 03 01's book again on that same TS — also accepted, so the book goes BACKWARDS
		`{
	"id": "d5806f1c-72b4-4e39-a1d0-6f93c28b407e",
	"simulation": 1,
	"depth": {
		"asks": [["79800.00", "1.00000"]],
		"bids": [["79799.00", "2.00000"]]
	},
	"SERVER": "V3",
	"count": 200,
	"limit": 50,
	"type": "fdepth",
	"pair": "btc_usdt",
	"TS": "2027-01-15T08:00:00.000"
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 9,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{{Price: "79800", Quantity: "1"}},
			Bids:       []events.PriceLevel{{Price: "79799", Quantity: "2"}},
		},
		{ // after 02
			ExchangeID: 9,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{{Price: "79810", Quantity: "3"}},
			Bids:       []events.PriceLevel{{Price: "79809", Quantity: "4"}},
		},
		{ // after 03 — back to 01's book, and nothing was dead-lettered
			ExchangeID: 9,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{{Price: "79800", Quantity: "1"}},
			Bids:       []events.PriceLevel{{Price: "79799", Quantity: "2"}},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 9, Simulation: 1, Price: "79800", Quantity: "1"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 9, Simulation: 1, Price: "79799", Quantity: "2"},
		},
	},
}

// Ex9NoiseFrames — everything that is not a well-formed depth book for a known market is dropped
// by job 1 without a dead-letter and without touching the book. Two of these are ex9-specific and
// worth the lines: the parser selects frames by SHAPE rather than by the `type` field, so a
// half-populated book (05) must be dropped WHOLE rather than emitted with one side null; and the
// market key is case-sensitive all the way to the `"{exchange_id}|{market}"` lookup, so the
// uppercase pair in 08 is an unknown market, not the same market.
var Ex9NoiseFrames = Scenario{
	ExchangeID: 9,
	PairID:     1,
	Sources: []string{
		// 01 snapshot
		`{
	"id": "6e0c47a9-1d83-4f52-b7c6-90a5e21df384",
	"simulation": 1,
	"depth": {
		"asks": [["79900.00", "1.00000"]],
		"bids": [["79899.00", "2.00000"]]
	},
	"SERVER": "V3",
	"count": 200,
	"limit": 50,
	"type": "fdepth",
	"pair": "btc_usdt",
	"TS": "2027-01-15T08:00:00.000"
}`,
		// 02 lbank's keepalive
		`{ "id": "a1d9f572-6c30-4b8e-9e14-83b06f2ac5d7", "simulation": 1, "action": "ping", "ping": "0c1c1a4b" }`,
		// 03 subscribe ack
		`{ "id": "38b5c0e7-4a19-4d62-95f3-b7e08c214a6d", "simulation": 1, "status": "success", "pair": "btc_usdt" }`,
		// 04 the incremental-depth channel — a book, but its levels hang off `incrDepth`
		`{
	"id": "c02f6b83-7e45-41d9-8a37-15de9b40c728",
	"simulation": 1,
	"incrDepth": {
		"asks": [["79901.00", "1.00000"]],
		"bids": [["79898.00", "1.00000"]]
	},
	"type": "incrDepth",
	"pair": "btc_usdt",
	"TS": "2027-01-15T08:00:01.000"
}`,
		// 05 a book with only one side — dropped whole, never emitted half
		`{
	"id": "51a8d3f6-9b07-4e21-84c5-6fa07d13b295",
	"simulation": 1,
	"depth": {
		"asks": [["79902.00", "1.00000"]]
	},
	"SERVER": "V3",
	"type": "fdepth",
	"pair": "btc_usdt",
	"TS": "2027-01-15T08:00:02.000"
}`,
		// 06 a book with no TS — ex9's event time IS its ordering field, so there is nothing to
		// place this frame against and job 1 will not invent a clock for it
		`{
	"id": "9c14e7b2-05d8-436a-91f7-2ea6c83b04d5",
	"simulation": 1,
	"depth": {
		"asks": [["79903.00", "1.00000"]],
		"bids": [["79897.00", "1.00000"]]
	},
	"SERVER": "V3",
	"type": "fdepth",
	"pair": "btc_usdt"
}`,
		// 07 a market ex9 has no exchange_markets row for
		`{
	"id": "7d3b95c1-8e46-4207-b9a0-4c15f8e27d63",
	"simulation": 1,
	"depth": {
		"asks": [["1.50000", "10.00000"]],
		"bids": [["1.49000", "10.00000"]]
	},
	"SERVER": "V3",
	"type": "fdepth",
	"pair": "foobar_usdt",
	"TS": "2027-01-15T08:00:03.000"
}`,
		// 08 the RIGHT market in the WRONG case — the lookup key is "9|btc_usdt" and nothing
		// normalizes case, so this is an unknown market and is dropped
		`{
	"id": "0b6a2e94-c73f-4d15-86b2-59e04a7cf381",
	"simulation": 1,
	"depth": {
		"asks": [["79904.00", "1.00000"]],
		"bids": [["79896.00", "1.00000"]]
	},
	"SERVER": "V3",
	"type": "fdepth",
	"pair": "BTC_USDT",
	"TS": "2027-01-15T08:00:04.000"
}`,
		// 09 JSON-number levels where ex9's wire sends strings — the whole frame is unparseable
		`{
	"id": "e4c78d05-b291-4f63-a8d7-3b1c60e295a4",
	"simulation": 1,
	"depth": {
		"asks": [[79905.00, 1.00000]],
		"bids": [[79895.00, 1.00000]]
	},
	"SERVER": "V3",
	"type": "fdepth",
	"pair": "btc_usdt",
	"TS": "2027-01-15T08:00:05.000"
}`,
		// 10 snapshot — the first good frame since 01, and the book is still 01's until it lands
		`{
	"id": "af205e71-9c36-4b80-a1d4-7e63c05f2b98",
	"simulation": 1,
	"depth": {
		"asks": [["79910.00", "5.00000"]],
		"bids": [["79890.00", "6.00000"]]
	},
	"SERVER": "V3",
	"count": 200,
	"limit": 50,
	"type": "fdepth",
	"pair": "btc_usdt",
	"TS": "2027-01-15T08:00:06.000"
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 9,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{{Price: "79900", Quantity: "1"}},
			Bids:       []events.PriceLevel{{Price: "79899", Quantity: "2"}},
		},
		{ // after 10 — 02 through 09 emitted nothing at all
			ExchangeID: 9,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:06Z",
			Asks:       []events.PriceLevel{{Price: "79910", Quantity: "5"}},
			Bids:       []events.PriceLevel{{Price: "79890", Quantity: "6"}},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 9, Simulation: 1, Price: "79910", Quantity: "5"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 9, Simulation: 1, Price: "79890", Quantity: "6"},
		},
	},
}

// Ex9PrecisionDust — job 4 on an lbank feed: prices that collide once truncated to the market's 2
// places merge into ONE level with their quantities summed, and a quantity that truncates to zero
// at the market's 8 places leaves no level behind. The order matters — job 4 truncates the price,
// groups, sums the RAW quantities, and only then truncates the sum, which is why the two
// 0.000000006 asks at 79656.12 survive as 0.00000001 while the lone 0.000000009 at 79655.1 does
// not. Pair 1 rebases by 10^0, so every number that moves here moved in job 4 and nowhere else.
var Ex9PrecisionDust = Scenario{
	ExchangeID: 9,
	PairID:     1,
	Sources: []string{
		// 01 snapshot
		`{
	"id": "3e9b1a47-6d05-4c82-97f1-0b58e2d4a76c",
	"simulation": 1,
	"depth": {
		"asks": [
			["79654.1234", "3.1234567891"],
			["79654.1289", "2.0000000099"],
			["79654.55", "1.50000000"],
			["79654.999", "4.00000000"],
			["79655.10", "0.0000000090"],
			["79656.1234", "0.0000000060"],
			["79656.1299", "0.0000000060"]
		],
		"bids": [
			["79653.0567", "5.9876543219"],
			["79653.0512", "1.00000000"],
			["79652.9999", "7.25000000"],
			["79652.50", "2.10000000"],
			["79651.4444", "9.00000000"]
		]
	},
	"SERVER": "V3",
	"count": 200,
	"limit": 50,
	"type": "fdepth",
	"pair": "btc_usdt",
	"TS": "2027-01-15T08:00:00.000"
}`,
		// 02 snapshot — a whole new book in which two of 01's levels have decayed to dust. On a
		// delta feed this would be a qty-"0" delete; on a snapshot feed the level is just absent.
		`{
	"id": "8c47f0d3-2b96-4e15-a380-5df19c62e4b7",
	"simulation": 1,
	"depth": {
		"asks": [
			["79654.1234", "5.1234567891"],
			["79654.55", "0.0000000041"],
			["79657.25", "6.00000000"],
			["79657.2599", "1.50000000"]
		],
		"bids": [
			["79653.0567", "2.00000000"],
			["79652.50", "0.0000000099"]
		]
	},
	"SERVER": "V3",
	"count": 200,
	"limit": 50,
	"type": "fdepth",
	"pair": "btc_usdt",
	"TS": "2027-01-15T08:00:01.000"
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 9,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "79654.12", Quantity: "5.12345679"},
				{Price: "79654.55", Quantity: "1.5"},
				{Price: "79654.99", Quantity: "4"},
				{Price: "79656.12", Quantity: "0.00000001"},
			},
			Bids: []events.PriceLevel{
				{Price: "79653.05", Quantity: "6.98765432"},
				{Price: "79652.99", Quantity: "7.25"},
				{Price: "79652.5", Quantity: "2.1"},
				{Price: "79651.44", Quantity: "9"},
			},
		},
		{ // after 02
			ExchangeID: 9,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "79654.12", Quantity: "5.12345678"},
				{Price: "79657.25", Quantity: "7.5"},
			},
			Bids: []events.PriceLevel{
				{Price: "79653.05", Quantity: "2"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 9, Simulation: 1, Price: "79654.12", Quantity: "5.12345678"},
			{ExchangeID: 9, Simulation: 1, Price: "79657.25", Quantity: "7.5"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 9, Simulation: 1, Price: "79653.05", Quantity: "2"},
		},
	},
}
