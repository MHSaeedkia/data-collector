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
// and confirm the pipeline both recovers and is willing to ask again.
//
// The two differ in how the book is re-synced, which is the branch that clears
// the "already asked" flag: ex6 resyncs with a sequenced snapshot, ex1 with a
// null-sequence REST snapshot whose offset is adopted by the next delta. They
// are separate code paths in job 2 and each has to clear the flag on its own.

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
		{Action: "snapshot_request", ExchangeID: 6, PairID: 1, Simulation: 1},
		{Action: "snapshot_request", ExchangeID: 6, PairID: 1, Simulation: 1},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 6, Simulation: 1, Price: "62920", Quantity: "1"},
			{ExchangeID: 6, Simulation: 1, Price: "62921", Quantity: "0.25"},
		},
		Bids: []events.AggregatedLevel{{ExchangeID: 6, Simulation: 1, Price: "62919", Quantity: "2"}},
	},
}

// ControlEx1NoBaselineThenGap — the same episode rule on nobitex, where the
// re-sync arrives as a REST snapshot with no offset at all.
//
// Two episodes again, but reached by the two different triggers: the first is
// `no_baseline` (a WS delta before any snapshot has ever landed — the cold-start
// case, and the one a job restart produces), the second a `sequence_gap`. What
// clears the flag between them is the ex1-specific path: the REST snapshot
// carries no offset, so it only FLAGS a resync, and the baseline is not actually
// established until the next delta adopts that delta's own offset. Both of those
// steps clear the flag in job 2, and a scenario that only ever re-synced with a
// sequenced snapshot would not touch either.
var ControlEx1NoBaselineThenGap = Scenario{
	ExchangeID: 1,
	PairID:     1,
	Sources: []string{
		// 01 ws delta, cold — no baseline, asks NiFi for a snapshot
		`{
	"id": "fa4e41a8-7fd5-4d3d-9166-532500f34981",
	"simulation": 1,
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"asks": [
					["62651", "0.29045069"]
				],
				"bids": [
					["62649", "0.55175335"]
				],
				"lastTradePrice": "62650",
				"lastUpdate": 1800000000000
			},
			"offset": 1000
		}
	}
}`,
		// 02 rest snapshot — the snapshot the request asked for. No offset of its
		// own, so it flags a resync rather than setting a baseline outright.
		`{
	"id": "1352ce88-db2a-4c08-9714-bf010a377a22",
	"simulation": 1,
	"action": "snapshot",
	"pair": "BTCUSDT",
	"status": "ok",
	"lastUpdate": 1800000001000,
	"lastTradePrice": "62650",
	"bids": [
		["62649", "0.50000000"],
		["62648", "0.02744953"],
		["62647", "0.20630833"],
		["62645", "0.90000000"],
		["62640", "1.31062803"]
	],
	"asks": [
		["62650", "2.21924167"],
		["62651", "0.17447383"],
		["62652", "0.19067482"],
		["62655", "1.05000000"],
		["62660", "0.33476925"]
	]
}`,
		// 03 ws delta — adopts its own offset as the baseline
		`{
	"id": "38bedc13-5ab5-4646-8bd8-50de65a192d9",
	"simulation": 1,
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"asks": [
					["62651", "0.29045069"],
					["62652", "0"],
					["62670", "0.40000000"]
				],
				"bids": [
					["62649", "0.55175335"],
					["62638", "1.10000000"]
				],
				"lastTradePrice": "62650",
				"lastUpdate": 1800000002000
			},
			"offset": 2000
		}
	}
}`,
		// 04 ws delta, offset jumps 2000 -> 2005 — gap, a new episode, asks again
		`{
	"id": "7a9e2ef2-6c19-45dd-b222-defaf4685c54",
	"simulation": 1,
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"asks": [
					["62680", "0.10000000"]
				],
				"bids": [
					["62630", "0.20000000"]
				],
				"lastTradePrice": "62650",
				"lastUpdate": 1800000003000
			},
			"offset": 2005
		}
	}
}`,
		// 05 ws delta while still waiting — rejected, and must NOT ask again
		`{
	"id": "fd752631-1907-42bd-80ae-2a05bb13d208",
	"simulation": 1,
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"asks": [
					["62681", "0.10000000"]
				],
				"bids": [
					["62629", "0.20000000"]
				],
				"lastTradePrice": "62650",
				"lastUpdate": 1800000004000
			},
			"offset": 2006
		}
	}
}`,
		// 06 rest snapshot — newer than the last accepted event, so it re-arms
		`{
	"id": "a8274b8c-6ba1-4b7d-a767-7cf1088db8e9",
	"simulation": 1,
	"action": "snapshot",
	"pair": "BTCUSDT",
	"status": "ok",
	"lastUpdate": 1800000005000,
	"lastTradePrice": "62650",
	"bids": [
		["62649", "0.50000000"],
		["62648", "0.02744953"],
		["62647", "0.20630833"],
		["62645", "0.90000000"],
		["62640", "1.31062803"]
	],
	"asks": [
		["62650", "2.21924167"],
		["62651", "0.17447383"],
		["62652", "0.19067482"],
		["62655", "1.05000000"],
		["62660", "0.33476925"]
	]
}`,
		// 07 ws delta — adopts 3000 unconditionally, the stream is healthy again
		`{
	"id": "3311c396-af2c-4272-b191-13d61b7e8414",
	"simulation": 1,
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"asks": [
					["62651", "0.29045069"]
				],
				"bids": [
					["62649", "0.55175335"]
				],
				"lastTradePrice": "62650",
				"lastUpdate": 1800000006000
			},
			"offset": 3000
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 02 — 01 never reached the book builder
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "62650", Quantity: "2.21924167"},
				{Price: "62651", Quantity: "0.17447383"},
				{Price: "62652", Quantity: "0.19067482"},
				{Price: "62655", Quantity: "1.05"},
				{Price: "62660", Quantity: "0.33476925"},
			},
			Bids: []events.PriceLevel{
				{Price: "62649", Quantity: "0.5"},
				{Price: "62648", Quantity: "0.02744953"},
				{Price: "62647", Quantity: "0.20630833"},
				{Price: "62645", Quantity: "0.9"},
				{Price: "62640", Quantity: "1.31062803"},
			},
		},
		{ // after 03 — 62652 deleted by its "0", 62670 and 62638 added
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks: []events.PriceLevel{
				{Price: "62650", Quantity: "2.21924167"},
				{Price: "62651", Quantity: "0.29045069"},
				{Price: "62655", Quantity: "1.05"},
				{Price: "62660", Quantity: "0.33476925"},
				{Price: "62670", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62649", Quantity: "0.55175335"},
				{Price: "62648", Quantity: "0.02744953"},
				{Price: "62647", Quantity: "0.20630833"},
				{Price: "62645", Quantity: "0.9"},
				{Price: "62640", Quantity: "1.31062803"},
				{Price: "62638", Quantity: "1.1"},
			},
		},
		{ // the reset for the gap — 04's own levels never reached the book
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:03Z",
			Asks:       []events.PriceLevel{},
			Bids:       []events.PriceLevel{},
		},
		{ // after 06 — 05 was rejected, so the book is exactly the REST snapshot
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:05Z",
			Asks: []events.PriceLevel{
				{Price: "62650", Quantity: "2.21924167"},
				{Price: "62651", Quantity: "0.17447383"},
				{Price: "62652", Quantity: "0.19067482"},
				{Price: "62655", Quantity: "1.05"},
				{Price: "62660", Quantity: "0.33476925"},
			},
			Bids: []events.PriceLevel{
				{Price: "62649", Quantity: "0.5"},
				{Price: "62648", Quantity: "0.02744953"},
				{Price: "62647", Quantity: "0.20630833"},
				{Price: "62645", Quantity: "0.9"},
				{Price: "62640", Quantity: "1.31062803"},
			},
		},
		{ // after 07 — two levels re-quoted on top of the re-synced book. 62670
			// and 62638 are gone: the snapshot cleared both sides first.
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:06Z",
			Asks: []events.PriceLevel{
				{Price: "62650", Quantity: "2.21924167"},
				{Price: "62651", Quantity: "0.29045069"},
				{Price: "62652", Quantity: "0.19067482"},
				{Price: "62655", Quantity: "1.05"},
				{Price: "62660", Quantity: "0.33476925"},
			},
			Bids: []events.PriceLevel{
				{Price: "62649", Quantity: "0.55175335"},
				{Price: "62648", Quantity: "0.02744953"},
				{Price: "62647", Quantity: "0.20630833"},
				{Price: "62645", Quantity: "0.9"},
				{Price: "62640", Quantity: "1.31062803"},
			},
		},
	},
	WantRejects: []string{"no_baseline", "sequence_gap", "awaiting_snapshot"},
	// One for the cold start, one for the gap. The REST snapshot at 02 and the
	// delta at 03 that adopted its offset are what let the second one be sent.
	WantControlCommands: []events.ControlCommand{
		{Action: "snapshot_request", ExchangeID: 1, PairID: 1, Simulation: 1},
		{Action: "snapshot_request", ExchangeID: 1, PairID: 1, Simulation: 1},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 1, Simulation: 1, Price: "62650", Quantity: "2.21924167"},
			{ExchangeID: 1, Simulation: 1, Price: "62651", Quantity: "0.29045069"},
			{ExchangeID: 1, Simulation: 1, Price: "62652", Quantity: "0.19067482"},
			{ExchangeID: 1, Simulation: 1, Price: "62655", Quantity: "1.05"},
			{ExchangeID: 1, Simulation: 1, Price: "62660", Quantity: "0.33476925"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 1, Simulation: 1, Price: "62649", Quantity: "0.55175335"},
			{ExchangeID: 1, Simulation: 1, Price: "62648", Quantity: "0.02744953"},
			{ExchangeID: 1, Simulation: 1, Price: "62647", Quantity: "0.20630833"},
			{ExchangeID: 1, Simulation: 1, Price: "62645", Quantity: "0.9"},
			{ExchangeID: 1, Simulation: 1, Price: "62640", Quantity: "1.31062803"},
		},
	},
}
