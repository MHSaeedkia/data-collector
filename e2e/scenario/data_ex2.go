// Scenarios for ex2/bitpin — ex1's shape, with event_time arriving in two wire types. WS pushes
// are themselves full snapshots (REVISED 2026-09-02 — see BitpinParser's javadoc; WS pushes were
// wrongly treated as deltas before this).
// The conventions these follow are in data.go.

package scenario

import "orderbook-e2e/events"

// Ex2WsSnapshotsReplaceWholesale — REVISED 2026-09-02: WS pushes are full snapshots, not deltas
// (see BitpinParser's javadoc), same shape as ex1's equivalent. Each WS push REPLACES the book
// wholesale, and job 2 never checks the gap between WS offsets, so 05's jump from offset 1001 to
// 9000 is silently accepted. Used to be named Ex2RestThenWsResync and tested the opposite (delta)
// assumption's REST-resync bootstrap, which no longer applies.
var Ex2WsSnapshotsReplaceWholesale = Scenario{
	ExchangeID: 2,
	PairID:     1,
	Sources: []string{
		// 01 rest snapshot
		`{
	"id": "cee81c36-e0fb-4beb-86d2-f44cdb415849",
	"simulation": 1,
	"action": "snapshot",
	"pair": "BTC_USDT",
	"event_time": 1800000000000,
	"asks": [
		["62700.00", "2.21924167"],
		["62701.30", "0.17447383"],
		["62702.60", "0.19067482"],
		["62705.00", "1.05000000"],
		["62710.80", "0.33476925"]
	],
	"bids": [
		["62699.50", "0.50000000"],
		["62698.20", "0.02744953"],
		["62697.10", "0.20630833"],
		["62695.00", "0.90000000"],
		["62690.40", "1.31062803"]
	]
}`,
		// 02 ws snapshot
		`{
	"id": "1f01d147-eee3-41d0-9b45-e7dd49ec8beb",
	"simulation": 1,
	"push": {
		"channel": "orderbook:BTC_USDT",
		"pub": {
			"data": {
				"asks": [
					["62701.30", "0.29045069"],
					["62702.60", "0"],
					["62720.00", "0.40000000"]
				],
				"bids": [
					["62699.50", "0.55175335"],
					["62688.00", "1.10000000"]
				],
				"symbol": "BTC_USDT",
				"event": "market_data",
				"event_time": "2027-01-15T08:00:01Z"
			},
			"offset": 1000
		}
	}
}`,
		// 03 ws snapshot
		`{
	"id": "1cdf2108-3407-4396-93de-10ebf1aafbc5",
	"simulation": 1,
	"push": {
		"channel": "orderbook:BTC_USDT",
		"pub": {
			"data": {
				"asks": [
					["62701.30", "0"],
					["62730.00", "0.75000000"]
				],
				"bids": [
					["62688.00", "0"],
					["62685.00", "2.00000000"]
				],
				"symbol": "BTC_USDT",
				"event": "market_data",
				"event_time": "2027-01-15T08:00:02Z"
			},
			"offset": 1001
		}
	}
}`,
		// 04 rest snapshot
		`{
	"id": "e46996eb-2acc-46b4-a891-10fd9c479737",
	"simulation": 1,
	"action": "snapshot",
	"pair": "BTC_USDT",
	"event_time": 1800000003000,
	"asks": [
		["62900.00", "1.80000000"],
		["62901.30", "0.22000000"],
		["62905.00", "0.70000000"],
		["62910.80", "0.40000000"]
	],
	"bids": [
		["62899.50", "0.60000000"],
		["62898.20", "0.11000000"],
		["62895.00", "0.90000000"],
		["62890.40", "1.20000000"]
	]
}`,
		// 05 ws snapshot
		`{
	"id": "929ea9f7-b7dc-402b-a0df-019d979f6845",
	"simulation": 1,
	"push": {
		"channel": "orderbook:BTC_USDT",
		"pub": {
			"data": {
				"asks": [
					["62901.30", "0"],
					["62920.00", "0.55000000"]
				],
				"bids": [
					["62899.50", "0.95000000"],
					["62880.00", "1.40000000"]
				],
				"symbol": "BTC_USDT",
				"event": "market_data",
				"event_time": "2027-01-15T08:00:04Z"
			},
			"offset": 9000
		}
	}
}`,
		// 06 ws snapshot
		`{
	"id": "10bb8616-7e4f-4700-9faf-d8a06a106381",
	"simulation": 1,
	"push": {
		"channel": "orderbook:BTC_USDT",
		"pub": {
			"data": {
				"asks": [
					["62920.00", "0"],
					["62930.00", "0.30000000"]
				],
				"bids": [
					["62880.00", "0"],
					["62875.00", "1.10000000"]
				],
				"symbol": "BTC_USDT",
				"event": "market_data",
				"event_time": "2027-01-15T08:00:05Z"
			},
			"offset": 9001
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 rest snapshot
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62700", Quantity: "2.21924167"},
				{Price: "62701.3", Quantity: "0.17447383"},
				{Price: "62702.6", Quantity: "0.19067482"},
				{Price: "62705", Quantity: "1.05"},
				{Price: "62710.8", Quantity: "0.33476925"},
			},
			Bids: []events.PriceLevel{
				{Price: "62699.5", Quantity: "0.5"},
				{Price: "62698.2", Quantity: "0.02744953"},
				{Price: "62697.1", Quantity: "0.20630833"},
				{Price: "62695", Quantity: "0.9"},
				{Price: "62690.4", Quantity: "1.31062803"},
			},
		},
		{ // after 02 ws snapshot — REPLACES the REST book wholesale; only 02's own levels survive
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "62701.3", Quantity: "0.29045069"},
				{Price: "62720", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62699.5", Quantity: "0.55175335"},
				{Price: "62688", Quantity: "1.1"},
			},
		},
		{ // after 03 ws snapshot
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks: []events.PriceLevel{
				{Price: "62730", Quantity: "0.75"},
			},
			Bids: []events.PriceLevel{
				{Price: "62685", Quantity: "2"},
			},
		},
		{ // after 04 rest snapshot
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:03Z",
			Asks: []events.PriceLevel{
				{Price: "62900", Quantity: "1.8"},
				{Price: "62901.3", Quantity: "0.22"},
				{Price: "62905", Quantity: "0.7"},
				{Price: "62910.8", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62899.5", Quantity: "0.6"},
				{Price: "62898.2", Quantity: "0.11"},
				{Price: "62895", Quantity: "0.9"},
				{Price: "62890.4", Quantity: "1.2"},
			},
		},
		{ // after 05 ws snapshot — offset jumps 1001->9000; no gap check on a snapshot, so it's just accepted
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:04Z",
			Asks: []events.PriceLevel{
				{Price: "62920", Quantity: "0.55"},
			},
			Bids: []events.PriceLevel{
				{Price: "62899.5", Quantity: "0.95"},
				{Price: "62880", Quantity: "1.4"},
			},
		},
		{ // after 06 ws snapshot
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:05Z",
			Asks: []events.PriceLevel{
				{Price: "62930", Quantity: "0.3"},
			},
			Bids: []events.PriceLevel{
				{Price: "62875", Quantity: "1.1"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 2, Simulation: 1, Price: "62930", Quantity: "0.3"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 2, Simulation: 1, Price: "62875", Quantity: "1.1"},
		},
	},
}

// Ex2WsSnapshotAloneEstablishesBaseline — REVISED 2026-09-02: a WS push is a full snapshot, so
// unlike a true delta feed it needs no prior baseline from REST — the first-ever event on a key is
// simply accepted as the book. Used to be named Ex2UpdateBeforeSnapshot and asserted a no_baseline
// rejection that no longer happens because there is no "update" type on this exchange any more.
var Ex2WsSnapshotAloneEstablishesBaseline = Scenario{
	ExchangeID: 2,
	PairID:     1,
	Sources: []string{
		// 01 ws snapshot no baseline
		`{
	"id": "ecbb0fd6-a501-482e-bd70-4bf2c964a009",
	"simulation": 1,
	"push": {
		"channel": "orderbook:BTC_USDT",
		"pub": {
			"data": {
				"asks": [
					["62701.30", "0.29045069"],
					["62720.00", "0.40000000"]
				],
				"bids": [
					["62699.50", "0.55175335"]
				],
				"symbol": "BTC_USDT",
				"event": "market_data",
				"event_time": "2027-01-15T08:00:00Z"
			},
			"offset": 1000
		}
	}
}`,
		// 02 rest snapshot
		`{
	"id": "b2e29062-cbf1-4dce-ad90-515eeb14b566",
	"simulation": 1,
	"action": "snapshot",
	"pair": "BTC_USDT",
	"event_time": 1800000001000,
	"asks": [
		["62700.00", "2.21924167"],
		["62701.30", "0.17447383"],
		["62702.60", "0.19067482"],
		["62705.00", "1.05000000"],
		["62710.80", "0.33476925"]
	],
	"bids": [
		["62699.50", "0.50000000"],
		["62698.20", "0.02744953"],
		["62697.10", "0.20630833"],
		["62695.00", "0.90000000"],
		["62690.40", "1.31062803"]
	]
}`,
		// 03 ws snapshot
		`{
	"id": "baecb42c-9c5d-401d-8055-b0dc09a576a9",
	"simulation": 1,
	"push": {
		"channel": "orderbook:BTC_USDT",
		"pub": {
			"data": {
				"asks": [
					["62701.30", "0.29045069"],
					["62702.60", "0"],
					["62720.00", "0.40000000"]
				],
				"bids": [
					["62699.50", "0.55175335"],
					["62688.00", "1.10000000"]
				],
				"symbol": "BTC_USDT",
				"event": "market_data",
				"event_time": "2027-01-15T08:00:02Z"
			},
			"offset": 2000
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 ws snapshot — accepted immediately, no baseline needed
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62701.3", Quantity: "0.29045069"},
				{Price: "62720", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62699.5", Quantity: "0.55175335"},
			},
		},
		{ // after 02 rest snapshot — replaces wholesale, as always
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "62700", Quantity: "2.21924167"},
				{Price: "62701.3", Quantity: "0.17447383"},
				{Price: "62702.6", Quantity: "0.19067482"},
				{Price: "62705", Quantity: "1.05"},
				{Price: "62710.8", Quantity: "0.33476925"},
			},
			Bids: []events.PriceLevel{
				{Price: "62699.5", Quantity: "0.5"},
				{Price: "62698.2", Quantity: "0.02744953"},
				{Price: "62697.1", Quantity: "0.20630833"},
				{Price: "62695", Quantity: "0.9"},
				{Price: "62690.4", Quantity: "1.31062803"},
			},
		},
		{ // after 03 ws snapshot — replaces the REST book wholesale in turn
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks: []events.PriceLevel{
				{Price: "62701.3", Quantity: "0.29045069"},
				{Price: "62720", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62699.5", Quantity: "0.55175335"},
				{Price: "62688", Quantity: "1.1"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 2, Simulation: 1, Price: "62701.3", Quantity: "0.29045069"},
			{ExchangeID: 2, Simulation: 1, Price: "62720", Quantity: "0.4"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 2, Simulation: 1, Price: "62699.5", Quantity: "0.55175335"},
			{ExchangeID: 2, Simulation: 1, Price: "62688", Quantity: "1.1"},
		},
	},
}

// Ex2WsGapAcceptedStaleRejected — REVISED 2026-09-02: same shape as ex1's equivalent, under
// bitpin's two-typed event_time. A WS push is a snapshot, so job 2 never jump-checks it — 04's
// offset skip (1001 -> 1005) is silently ACCEPTED. The seq<=last guard still applies to a sequenced
// snapshot though: 05 arrives with offset 1002, behind the already-accepted 1005, so it is rejected
// stale_or_duplicate. No reset, no snapshot_request — that machinery lives in job 2's "update"
// branch, which this exchange no longer uses. Used to be named Ex2SequenceGap.
var Ex2WsGapAcceptedStaleRejected = Scenario{
	ExchangeID: 2,
	PairID:     1,
	Sources: []string{
		// 01 rest snapshot
		`{
	"id": "bc839433-16c2-4fc2-a3ec-14b1106ece77",
	"simulation": 1,
	"action": "snapshot",
	"pair": "BTC_USDT",
	"event_time": 1800000000000,
	"asks": [
		["62700.00", "2.21924167"],
		["62701.30", "0.17447383"],
		["62702.60", "0.19067482"],
		["62705.00", "1.05000000"],
		["62710.80", "0.33476925"]
	],
	"bids": [
		["62699.50", "0.50000000"],
		["62698.20", "0.02744953"],
		["62697.10", "0.20630833"],
		["62695.00", "0.90000000"],
		["62690.40", "1.31062803"]
	]
}`,
		// 02 ws snapshot
		`{
	"id": "0a6f5a8e-69c7-4086-9821-f4c0ccbf7bb9",
	"simulation": 1,
	"push": {
		"channel": "orderbook:BTC_USDT",
		"pub": {
			"data": {
				"asks": [
					["62701.30", "0.29045069"],
					["62720.00", "0.40000000"]
				],
				"bids": [
					["62699.50", "0.55175335"]
				],
				"symbol": "BTC_USDT",
				"event": "market_data",
				"event_time": "2027-01-15T08:00:01Z"
			},
			"offset": 1000
		}
	}
}`,
		// 03 ws snapshot ok
		`{
	"id": "f4ae76ba-056d-4554-a349-dc6107d50963",
	"simulation": 1,
	"push": {
		"channel": "orderbook:BTC_USDT",
		"pub": {
			"data": {
				"asks": [
					["62730.00", "0.75000000"]
				],
				"bids": [
					["62685.00", "2.00000000"]
				],
				"symbol": "BTC_USDT",
				"event": "market_data",
				"event_time": "2027-01-15T08:00:02Z"
			},
			"offset": 1001
		}
	}
}`,
		// 04 ws snapshot gap
		`{
	"id": "467c2533-27b0-42b6-a4a7-812d497b489a",
	"simulation": 1,
	"push": {
		"channel": "orderbook:BTC_USDT",
		"pub": {
			"data": {
				"asks": [
					["62740.00", "9.99900000"]
				],
				"bids": [
					["62680.00", "9.99900000"]
				],
				"symbol": "BTC_USDT",
				"event": "market_data",
				"event_time": "2027-01-15T08:00:03Z"
			},
			"offset": 1005
		}
	}
}`,
		// 05 ws snapshot awaiting
		`{
	"id": "0475c8d0-8887-40b4-96d9-6e948f82adda",
	"simulation": 1,
	"push": {
		"channel": "orderbook:BTC_USDT",
		"pub": {
			"data": {
				"asks": [
					["62745.00", "9.99900000"]
				],
				"bids": [
					["62675.00", "9.99900000"]
				],
				"symbol": "BTC_USDT",
				"event": "market_data",
				"event_time": "2027-01-15T08:00:04Z"
			},
			"offset": 1002
		}
	}
}`,
		// 06 rest snapshot resync
		`{
	"id": "583cccf0-e4d3-4915-843c-90c9f9cb19e5",
	"simulation": 1,
	"action": "snapshot",
	"pair": "BTC_USDT",
	"event_time": 1800000005000,
	"asks": [
		["63100.00", "1.80000000"],
		["63101.30", "0.22000000"],
		["63105.00", "0.70000000"],
		["63110.80", "0.40000000"]
	],
	"bids": [
		["63099.50", "0.60000000"],
		["63098.20", "0.11000000"],
		["63095.00", "0.90000000"],
		["63090.40", "1.20000000"]
	]
}`,
		// 07 ws snapshot
		`{
	"id": "2a8fdcab-76cf-40df-a3a5-54de1c982c84",
	"simulation": 1,
	"push": {
		"channel": "orderbook:BTC_USDT",
		"pub": {
			"data": {
				"asks": [
					["63101.30", "0"],
					["63120.00", "0.55000000"]
				],
				"bids": [
					["63099.50", "0.95000000"],
					["63080.00", "1.40000000"]
				],
				"symbol": "BTC_USDT",
				"event": "market_data",
				"event_time": "2027-01-15T08:00:06Z"
			},
			"offset": 2000
		}
	}
}`,
		// 08 ws snapshot ok
		`{
	"id": "b8d2e5ea-81fd-4bb3-bafa-4ca2ce64a431",
	"simulation": 1,
	"push": {
		"channel": "orderbook:BTC_USDT",
		"pub": {
			"data": {
				"asks": [
					["63120.00", "0"],
					["63130.00", "0.30000000"]
				],
				"bids": [
					["63080.00", "0"],
					["63075.00", "1.10000000"]
				],
				"symbol": "BTC_USDT",
				"event": "market_data",
				"event_time": "2027-01-15T08:00:07Z"
			},
			"offset": 2001
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 rest snapshot
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62700", Quantity: "2.21924167"},
				{Price: "62701.3", Quantity: "0.17447383"},
				{Price: "62702.6", Quantity: "0.19067482"},
				{Price: "62705", Quantity: "1.05"},
				{Price: "62710.8", Quantity: "0.33476925"},
			},
			Bids: []events.PriceLevel{
				{Price: "62699.5", Quantity: "0.5"},
				{Price: "62698.2", Quantity: "0.02744953"},
				{Price: "62697.1", Quantity: "0.20630833"},
				{Price: "62695", Quantity: "0.9"},
				{Price: "62690.4", Quantity: "1.31062803"},
			},
		},
		{ // after 02 ws snapshot — replaces wholesale
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "62701.3", Quantity: "0.29045069"},
				{Price: "62720", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62699.5", Quantity: "0.55175335"},
			},
		},
		{ // after 03 ws snapshot ok
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks: []events.PriceLevel{
				{Price: "62730", Quantity: "0.75"},
			},
			Bids: []events.PriceLevel{
				{Price: "62685", Quantity: "2"},
			},
		},
		{ // after 04 ws snapshot — offset skips 1001->1005; accepted, no gap check on a snapshot
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:03Z",
			Asks: []events.PriceLevel{
				{Price: "62740", Quantity: "9.999"},
			},
			Bids: []events.PriceLevel{
				{Price: "62680", Quantity: "9.999"},
			},
		},
		// 05 (offset 1002) is REJECTED stale_or_duplicate — 1002 <= the already-accepted 1005 —
		// so it produces no book output; the state from 04 stands until 06.
		{ // after 06 rest snapshot resync
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:05Z",
			Asks: []events.PriceLevel{
				{Price: "63100", Quantity: "1.8"},
				{Price: "63101.3", Quantity: "0.22"},
				{Price: "63105", Quantity: "0.7"},
				{Price: "63110.8", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "63099.5", Quantity: "0.6"},
				{Price: "63098.2", Quantity: "0.11"},
				{Price: "63095", Quantity: "0.9"},
				{Price: "63090.4", Quantity: "1.2"},
			},
		},
		{ // after 07 ws snapshot
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:06Z",
			Asks: []events.PriceLevel{
				{Price: "63120", Quantity: "0.55"},
			},
			Bids: []events.PriceLevel{
				{Price: "63099.5", Quantity: "0.95"},
				{Price: "63080", Quantity: "1.4"},
			},
		},
		{ // after 08 ws snapshot ok
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:07Z",
			Asks: []events.PriceLevel{
				{Price: "63130", Quantity: "0.3"},
			},
			Bids: []events.PriceLevel{
				{Price: "63075", Quantity: "1.1"},
			},
		},
	},
	WantRejects: []string{"stale_or_duplicate"},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 2, Simulation: 1, Price: "63130", Quantity: "0.3"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 2, Simulation: 1, Price: "63075", Quantity: "1.1"},
		},
	},
}

// Ex2NoiseFrames — 05 is the ex2-only one: a snapshot whose event_time is an ISO string where the
// REST shape needs a number is dropped silently. WS frames (06, 07) are snapshots (2026-09-02)
// that replace the book wholesale rather than merging.
var Ex2NoiseFrames = Scenario{
	ExchangeID: 2,
	PairID:     1,
	Sources: []string{
		// 01 connect ack
		`{
	"id": "917d4004-683f-4e51-94cb-33a7e5e49200",
	"simulation": 1,
	"connect": {
		"client": "b7e1c2f4-3a5d-4e6f-9a0b-1c2d3e4f5a6b",
		"version": "5.0.0"
	}
}`,
		// 02 foreign channel
		`{
	"id": "d0c0949d-42fc-4206-959f-05e85c8fc198",
	"simulation": 1,
	"push": {
		"channel": "trades:BTC_USDT",
		"pub": {
			"data": {
				"symbol": "BTC_USDT",
				"event": "trade",
				"price": "62800.00",
				"quantity": "0.01000000",
				"event_time": "2027-01-15T08:00:00Z"
			},
			"offset": 5551
		}
	}
}`,
		// 03 rest snapshot
		`{
	"id": "f0c6b2a7-5f6f-4712-9fcd-53c935995edc",
	"simulation": 1,
	"action": "snapshot",
	"pair": "BTC_USDT",
	"event_time": 1800000000000,
	"asks": [
		["62800.00", "2.21924167"],
		["62801.30", "0.17447383"],
		["62805.00", "1.05000000"],
		["62810.80", "0.33476925"]
	],
	"bids": [
		["62799.50", "0.50000000"],
		["62798.20", "0.02744953"],
		["62795.00", "0.90000000"],
		["62790.40", "1.31062803"]
	]
}`,
		// 04 malformed book
		`{
	"id": "3c458b26-6572-4066-8f20-b3e651d026a8",
	"simulation": 1,
	"push": {
		"channel": "orderbook:BTC_USDT",
		"pub": {
			"data": {
				"symbol": "BTC_USDT",
				"event": "market_data",
				"event_time": "2027-01-15T08:00:00Z"
			},
			"offset": 6001
		}
	}
}`,
		// 05 rest snapshot string event time
		`{
	"id": "eb815188-b067-4741-91c0-b4c2cc8a8b98",
	"simulation": 1,
	"action": "snapshot",
	"pair": "BTC_USDT",
	"event_time": "2027-01-15T08:00:00Z",
	"asks": [
		["62815.00", "9.99900000"]
	],
	"bids": [
		["60000.00", "5.00000000"]
	]
}`,
		// 06 ws snapshot
		`{
	"id": "3b39cd5a-4aaf-4e7e-880e-c5500246746e",
	"simulation": 1,
	"push": {
		"channel": "orderbook:BTC_USDT",
		"pub": {
			"data": {
				"asks": [
					["62801.30", "0"],
					["62820.00", "0.40000000"]
				],
				"bids": [
					["62799.50", "0.65000000"],
					["62788.00", "1.10000000"]
				],
				"symbol": "BTC_USDT",
				"event": "market_data",
				"event_time": "2027-01-15T08:00:01Z"
			},
			"offset": 1000
		}
	}
}`,
		// 07 ws snapshot
		`{
	"id": "5af3bc38-30b0-404a-9078-7b6d1f8cdb2c",
	"simulation": 1,
	"push": {
		"channel": "orderbook:BTC_USDT",
		"pub": {
			"data": {
				"asks": [
					["62820.00", "0"],
					["62830.00", "0.30000000"]
				],
				"bids": [
					["62788.00", "0"],
					["62785.00", "1.10000000"]
				],
				"symbol": "BTC_USDT",
				"event": "market_data",
				"event_time": "2027-01-15T08:00:02Z"
			},
			"offset": 1001
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 03 rest snapshot
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62800", Quantity: "2.21924167"},
				{Price: "62801.3", Quantity: "0.17447383"},
				{Price: "62805", Quantity: "1.05"},
				{Price: "62810.8", Quantity: "0.33476925"},
			},
			Bids: []events.PriceLevel{
				{Price: "62799.5", Quantity: "0.5"},
				{Price: "62798.2", Quantity: "0.02744953"},
				{Price: "62795", Quantity: "0.9"},
				{Price: "62790.4", Quantity: "1.31062803"},
			},
		},
		{ // after 06 ws snapshot — replaces the REST book wholesale
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "62820", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62799.5", Quantity: "0.65"},
				{Price: "62788", Quantity: "1.1"},
			},
		},
		{ // after 07 ws snapshot
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks: []events.PriceLevel{
				{Price: "62830", Quantity: "0.3"},
			},
			Bids: []events.PriceLevel{
				{Price: "62785", Quantity: "1.1"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 2, Simulation: 1, Price: "62830", Quantity: "0.3"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 2, Simulation: 1, Price: "62785", Quantity: "1.1"},
		},
	},
}

// Ex2StaleRestReplay — proves the epoch-millis REST snapshot and the ISO WS event times land on
// one scale; WS frames (02, 03, 05) are snapshots (2026-09-02) that replace the book wholesale.
var Ex2StaleRestReplay = Scenario{
	ExchangeID: 2,
	PairID:     1,
	Sources: []string{
		// 01 rest snapshot
		`{
	"id": "5f519c67-3362-4280-a687-6a8707f9dfaa",
	"simulation": 1,
	"action": "snapshot",
	"pair": "BTC_USDT",
	"event_time": 1800000000000,
	"asks": [
		["62700.00", "2.21924167"],
		["62701.30", "0.17447383"],
		["62710.80", "0.33476925"]
	],
	"bids": [
		["62699.50", "0.50000000"],
		["62698.20", "0.02744953"],
		["62690.40", "1.31062803"]
	]
}`,
		// 02 ws snapshot
		`{
	"id": "80525779-99cf-4807-b530-b7bfa8cc88ec",
	"simulation": 1,
	"push": {
		"channel": "orderbook:BTC_USDT",
		"pub": {
			"data": {
				"asks": [
					["62701.30", "0.29045069"],
					["62720.00", "0.40000000"]
				],
				"bids": [
					["62699.50", "0.55175335"]
				],
				"symbol": "BTC_USDT",
				"event": "market_data",
				"event_time": "2027-01-15T08:00:01Z"
			},
			"offset": 1000
		}
	}
}`,
		// 03 ws snapshot loud
		`{
	"id": "3d0671d7-68a3-4bbe-9d58-8f5e1b8c7d69",
	"simulation": 1,
	"push": {
		"channel": "orderbook:BTC_USDT",
		"pub": {
			"data": {
				"asks": [
					["62730.00", "0.75000000"]
				],
				"bids": [
					["60000.00", "5.00000000"]
				],
				"symbol": "BTC_USDT",
				"event": "market_data",
				"event_time": "2027-01-15T08:00:02Z"
			},
			"offset": 1001
		}
	}
}`,
		// 04 rest snapshot stale replay
		`{
	"id": "246dd491-be6e-40c0-b821-425ce9277054",
	"simulation": 1,
	"action": "snapshot",
	"pair": "BTC_USDT",
	"event_time": 1800000000000,
	"asks": [
		["62700.00", "2.21924167"],
		["62701.30", "0.17447383"],
		["62710.80", "0.33476925"]
	],
	"bids": [
		["62699.50", "0.50000000"],
		["62698.20", "0.02744953"],
		["62690.40", "1.31062803"]
	]
}`,
		// 05 ws snapshot
		`{
	"id": "71103480-23d2-480a-9959-6ab8d922feab",
	"simulation": 1,
	"push": {
		"channel": "orderbook:BTC_USDT",
		"pub": {
			"data": {
				"asks": [
					["62701.30", "0.10000000"]
				],
				"bids": [
					["62699.50", "0.60000000"]
				],
				"symbol": "BTC_USDT",
				"event": "market_data",
				"event_time": "2027-01-15T08:00:03Z"
			},
			"offset": 1002
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 rest snapshot
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62700", Quantity: "2.21924167"},
				{Price: "62701.3", Quantity: "0.17447383"},
				{Price: "62710.8", Quantity: "0.33476925"},
			},
			Bids: []events.PriceLevel{
				{Price: "62699.5", Quantity: "0.5"},
				{Price: "62698.2", Quantity: "0.02744953"},
				{Price: "62690.4", Quantity: "1.31062803"},
			},
		},
		{ // after 02 ws snapshot — replaces the REST book wholesale
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "62701.3", Quantity: "0.29045069"},
				{Price: "62720", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62699.5", Quantity: "0.55175335"},
			},
		},
		{ // after 03 ws snapshot loud
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks: []events.PriceLevel{
				{Price: "62730", Quantity: "0.75"},
			},
			Bids: []events.PriceLevel{
				{Price: "60000", Quantity: "5"},
			},
		},
		{ // after 05 ws snapshot
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:03Z",
			Asks: []events.PriceLevel{
				{Price: "62701.3", Quantity: "0.1"},
			},
			Bids: []events.PriceLevel{
				{Price: "62699.5", Quantity: "0.6"},
			},
		},
	},
	WantRejects: []string{"out_of_order"},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 2, Simulation: 1, Price: "62701.3", Quantity: "0.1"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 2, Simulation: 1, Price: "62699.5", Quantity: "0.6"},
		},
	},
}

// Ex2PrecisionDust — job 4 on a bitpin feed, the same two rules as ex1: prices colliding at the
// market's 2 places merge with their quantities summed, and a quantity under 8 places truncates
// to zero and deletes. Rebase cannot be asserted on ex2 — every bitpin row in the seed is 0/0,
// so job 3 is the identity here; the rebase cases live on ex1, whose seed has real exponents. The
// WS frame is a snapshot (2026-09-02): it REPLACES the book wholesale, so the 62699.99 bid that
// 02's payload never re-sends is gone, not merely left untouched.
var Ex2PrecisionDust = Scenario{
	ExchangeID: 2,
	PairID:     1,
	Sources: []string{
		// 01 rest snapshot
		`{
	"id": "8e7b3265-3d02-4891-bdf3-353ed58cb13a",
	"simulation": 1,
	"action": "snapshot",
	"pair": "BTC_USDT",
	"event_time": 1800000000000,
	"asks": [
		["62700.117", "0.30000000"],
		["62700.119", "0.20000000"],
		["62701.50", "0.000000004"]
	],
	"bids": [
		["62699.999", "1.00000000"],
		["62698.25", "0.12345678999"]
	]
}`,
		// 02 ws snapshot
		`{
	"id": "7bd7e8aa-c914-4db9-9341-fa7b9a62f3fe",
	"simulation": 1,
	"push": {
		"channel": "orderbook:BTC_USDT",
		"pub": {
			"data": {
				"asks": [
					["62700.115", "0.000000001"],
					["62705.999", "0.60000000"]
				],
				"bids": [
					["62698.251", "0.40000000"],
					["62698.259", "0.10000000"]
				],
				"symbol": "BTC_USDT",
				"event": "market_data",
				"event_time": "2027-01-15T08:00:01Z"
			},
			"offset": 1000
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 rest snapshot — .117 and .119 merged at .11, the dust ask never rested
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62700.11", Quantity: "0.5"},
			},
			Bids: []events.PriceLevel{
				{Price: "62699.99", Quantity: "1"},
				{Price: "62698.25", Quantity: "0.12345678"},
			},
		},
		{ // after 02 ws snapshot — REPLACES wholesale: the dust delete at 62700.11 is a no-op on
			// the freshly-cleared side, and 62699.99 (not resent) is simply gone
			ExchangeID: 2,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "62705.99", Quantity: "0.6"},
			},
			Bids: []events.PriceLevel{
				{Price: "62698.25", Quantity: "0.5"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 2, Simulation: 1, Price: "62705.99", Quantity: "0.6"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 2, Simulation: 1, Price: "62698.25", Quantity: "0.5"},
		},
	},
}
