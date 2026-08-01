// Scenarios from flink/normalizer/manual-test-data for
// ex2/bitpin — ex1's shape, with event_time arriving in two wire types.
// The conventions these follow are in data.go.

package scenario

import "orderbook-e2e/events"

// Ex2RestThenWsResync is 13-ex2-rest-then-ws-resync —
// bitpin inherits ex1's resync; the re-anchor at 04 adopts 9000 without a gap check.
var Ex2RestThenWsResync = Scenario{
	ExchangeID: 2,
	PairID:     1,
	Sources: []string{
		// 01-rest-snapshot.json
		`{
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
		// 02-ws-update.json
		`{
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
		// 03-ws-update.json
		`{
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
		// 04-rest-snapshot.json
		`{
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
		// 05-ws-update.json
		`{
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
		// 06-ws-update.json
		`{
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
		{ // after 01-rest-snapshot.json
			ExchangeID: 2,
			PairID:     1,
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
		{ // after 02-ws-update.json
			ExchangeID: 2,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "62700", Quantity: "2.21924167"},
				{Price: "62701.3", Quantity: "0.29045069"},
				{Price: "62705", Quantity: "1.05"},
				{Price: "62710.8", Quantity: "0.33476925"},
				{Price: "62720", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62699.5", Quantity: "0.55175335"},
				{Price: "62698.2", Quantity: "0.02744953"},
				{Price: "62697.1", Quantity: "0.20630833"},
				{Price: "62695", Quantity: "0.9"},
				{Price: "62690.4", Quantity: "1.31062803"},
				{Price: "62688", Quantity: "1.1"},
			},
		},
		{ // after 03-ws-update.json
			ExchangeID: 2,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks: []events.PriceLevel{
				{Price: "62700", Quantity: "2.21924167"},
				{Price: "62705", Quantity: "1.05"},
				{Price: "62710.8", Quantity: "0.33476925"},
				{Price: "62720", Quantity: "0.4"},
				{Price: "62730", Quantity: "0.75"},
			},
			Bids: []events.PriceLevel{
				{Price: "62699.5", Quantity: "0.55175335"},
				{Price: "62698.2", Quantity: "0.02744953"},
				{Price: "62697.1", Quantity: "0.20630833"},
				{Price: "62695", Quantity: "0.9"},
				{Price: "62690.4", Quantity: "1.31062803"},
				{Price: "62685", Quantity: "2"},
			},
		},
		{ // after 04-rest-snapshot.json
			ExchangeID: 2,
			PairID:     1,
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
		{ // after 05-ws-update.json
			ExchangeID: 2,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:04Z",
			Asks: []events.PriceLevel{
				{Price: "62900", Quantity: "1.8"},
				{Price: "62905", Quantity: "0.7"},
				{Price: "62910.8", Quantity: "0.4"},
				{Price: "62920", Quantity: "0.55"},
			},
			Bids: []events.PriceLevel{
				{Price: "62899.5", Quantity: "0.95"},
				{Price: "62898.2", Quantity: "0.11"},
				{Price: "62895", Quantity: "0.9"},
				{Price: "62890.4", Quantity: "1.2"},
				{Price: "62880", Quantity: "1.4"},
			},
		},
		{ // after 06-ws-update.json
			ExchangeID: 2,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:05Z",
			Asks: []events.PriceLevel{
				{Price: "62900", Quantity: "1.8"},
				{Price: "62905", Quantity: "0.7"},
				{Price: "62910.8", Quantity: "0.4"},
				{Price: "62930", Quantity: "0.3"},
			},
			Bids: []events.PriceLevel{
				{Price: "62899.5", Quantity: "0.95"},
				{Price: "62898.2", Quantity: "0.11"},
				{Price: "62895", Quantity: "0.9"},
				{Price: "62890.4", Quantity: "1.2"},
				{Price: "62875", Quantity: "1.1"},
			},
		},
	},
}

// Ex2UpdateBeforeSnapshot is 14-ex2-update-before-snapshot —
// the shape a partial rollout produces continuously: parser live, REST feed not yet publishing.
var Ex2UpdateBeforeSnapshot = Scenario{
	ExchangeID: 2,
	PairID:     1,
	Sources: []string{
		// 01-ws-update-no-baseline.json
		`{
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
		// 02-rest-snapshot.json
		`{
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
		// 03-ws-update.json
		`{
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
		{ // after 02-rest-snapshot.json
			ExchangeID: 2,
			PairID:     1,
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
		{ // after 03-ws-update.json
			ExchangeID: 2,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks: []events.PriceLevel{
				{Price: "62700", Quantity: "2.21924167"},
				{Price: "62701.3", Quantity: "0.29045069"},
				{Price: "62705", Quantity: "1.05"},
				{Price: "62710.8", Quantity: "0.33476925"},
				{Price: "62720", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62699.5", Quantity: "0.55175335"},
				{Price: "62698.2", Quantity: "0.02744953"},
				{Price: "62697.1", Quantity: "0.20630833"},
				{Price: "62695", Quantity: "0.9"},
				{Price: "62690.4", Quantity: "1.31062803"},
				{Price: "62688", Quantity: "1.1"},
			},
		},
	},
	WantRejects: []string{"no_baseline"},
}

// Ex2SequenceGap is 15-ex2-sequence-gap —
// same gap episode as ex1, under bitpin's two-typed event_time.
var Ex2SequenceGap = Scenario{
	ExchangeID: 2,
	PairID:     1,
	Sources: []string{
		// 01-rest-snapshot.json
		`{
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
		// 02-ws-update.json
		`{
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
		// 03-ws-update-ok.json
		`{
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
		// 04-ws-update-gap.json
		`{
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
		// 05-ws-update-awaiting.json
		`{
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
		// 06-rest-snapshot-resync.json
		`{
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
		// 07-ws-update.json
		`{
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
		// 08-ws-update-ok.json
		`{
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
		{ // after 01-rest-snapshot.json
			ExchangeID: 2,
			PairID:     1,
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
		{ // after 02-ws-update.json
			ExchangeID: 2,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "62700", Quantity: "2.21924167"},
				{Price: "62701.3", Quantity: "0.29045069"},
				{Price: "62702.6", Quantity: "0.19067482"},
				{Price: "62705", Quantity: "1.05"},
				{Price: "62710.8", Quantity: "0.33476925"},
				{Price: "62720", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62699.5", Quantity: "0.55175335"},
				{Price: "62698.2", Quantity: "0.02744953"},
				{Price: "62697.1", Quantity: "0.20630833"},
				{Price: "62695", Quantity: "0.9"},
				{Price: "62690.4", Quantity: "1.31062803"},
			},
		},
		{ // after 03-ws-update-ok.json
			ExchangeID: 2,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks: []events.PriceLevel{
				{Price: "62700", Quantity: "2.21924167"},
				{Price: "62701.3", Quantity: "0.29045069"},
				{Price: "62702.6", Quantity: "0.19067482"},
				{Price: "62705", Quantity: "1.05"},
				{Price: "62710.8", Quantity: "0.33476925"},
				{Price: "62720", Quantity: "0.4"},
				{Price: "62730", Quantity: "0.75"},
			},
			Bids: []events.PriceLevel{
				{Price: "62699.5", Quantity: "0.55175335"},
				{Price: "62698.2", Quantity: "0.02744953"},
				{Price: "62697.1", Quantity: "0.20630833"},
				{Price: "62695", Quantity: "0.9"},
				{Price: "62690.4", Quantity: "1.31062803"},
				{Price: "62685", Quantity: "2"},
			},
		},
		{ // after 04-ws-update-gap.json (reset)
			ExchangeID: 2,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:03Z",
			Asks:       []events.PriceLevel{},
			Bids:       []events.PriceLevel{},
		},
		{ // after 06-rest-snapshot-resync.json
			ExchangeID: 2,
			PairID:     1,
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
		{ // after 07-ws-update.json
			ExchangeID: 2,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:06Z",
			Asks: []events.PriceLevel{
				{Price: "63100", Quantity: "1.8"},
				{Price: "63105", Quantity: "0.7"},
				{Price: "63110.8", Quantity: "0.4"},
				{Price: "63120", Quantity: "0.55"},
			},
			Bids: []events.PriceLevel{
				{Price: "63099.5", Quantity: "0.95"},
				{Price: "63098.2", Quantity: "0.11"},
				{Price: "63095", Quantity: "0.9"},
				{Price: "63090.4", Quantity: "1.2"},
				{Price: "63080", Quantity: "1.4"},
			},
		},
		{ // after 08-ws-update-ok.json
			ExchangeID: 2,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:07Z",
			Asks: []events.PriceLevel{
				{Price: "63100", Quantity: "1.8"},
				{Price: "63105", Quantity: "0.7"},
				{Price: "63110.8", Quantity: "0.4"},
				{Price: "63130", Quantity: "0.3"},
			},
			Bids: []events.PriceLevel{
				{Price: "63099.5", Quantity: "0.95"},
				{Price: "63098.2", Quantity: "0.11"},
				{Price: "63095", Quantity: "0.9"},
				{Price: "63090.4", Quantity: "1.2"},
				{Price: "63075", Quantity: "1.1"},
			},
		},
	},
	WantRejects: []string{"sequence_gap", "awaiting_snapshot"},
}

// Ex2NoiseFrames is 16-ex2-noise-frames —
// 05 is the ex2-only one: a snapshot whose event_time is an ISO string where the REST shape needs a number is dropped silently.
var Ex2NoiseFrames = Scenario{
	ExchangeID: 2,
	PairID:     1,
	Sources: []string{
		// 01-connect-ack.json
		`{
	"connect": {
		"client": "b7e1c2f4-3a5d-4e6f-9a0b-1c2d3e4f5a6b",
		"version": "5.0.0"
	}
}`,
		// 02-foreign-channel.json
		`{
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
		// 03-rest-snapshot.json
		`{
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
		// 04-malformed-book.json
		`{
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
		// 05-rest-snapshot-string-event-time.json
		`{
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
		// 06-ws-update.json
		`{
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
		// 07-ws-update.json
		`{
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
		{ // after 03-rest-snapshot.json
			ExchangeID: 2,
			PairID:     1,
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
		{ // after 06-ws-update.json
			ExchangeID: 2,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "62800", Quantity: "2.21924167"},
				{Price: "62805", Quantity: "1.05"},
				{Price: "62810.8", Quantity: "0.33476925"},
				{Price: "62820", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62799.5", Quantity: "0.65"},
				{Price: "62798.2", Quantity: "0.02744953"},
				{Price: "62795", Quantity: "0.9"},
				{Price: "62790.4", Quantity: "1.31062803"},
				{Price: "62788", Quantity: "1.1"},
			},
		},
		{ // after 07-ws-update.json
			ExchangeID: 2,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks: []events.PriceLevel{
				{Price: "62800", Quantity: "2.21924167"},
				{Price: "62805", Quantity: "1.05"},
				{Price: "62810.8", Quantity: "0.33476925"},
				{Price: "62830", Quantity: "0.3"},
			},
			Bids: []events.PriceLevel{
				{Price: "62799.5", Quantity: "0.65"},
				{Price: "62798.2", Quantity: "0.02744953"},
				{Price: "62795", Quantity: "0.9"},
				{Price: "62790.4", Quantity: "1.31062803"},
				{Price: "62785", Quantity: "1.1"},
			},
		},
	},
}

// Ex2StaleRestReplay is 17-ex2-stale-rest-replay —
// proves the epoch-millis snapshot and the ISO WS deltas land on one scale.
var Ex2StaleRestReplay = Scenario{
	ExchangeID: 2,
	PairID:     1,
	Sources: []string{
		// 01-rest-snapshot.json
		`{
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
		// 02-ws-update.json
		`{
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
		// 03-ws-update-loud.json
		`{
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
		// 04-rest-snapshot-stale-replay.json
		`{
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
		// 05-ws-update.json
		`{
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
		{ // after 01-rest-snapshot.json
			ExchangeID: 2,
			PairID:     1,
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
		{ // after 02-ws-update.json
			ExchangeID: 2,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "62700", Quantity: "2.21924167"},
				{Price: "62701.3", Quantity: "0.29045069"},
				{Price: "62710.8", Quantity: "0.33476925"},
				{Price: "62720", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62699.5", Quantity: "0.55175335"},
				{Price: "62698.2", Quantity: "0.02744953"},
				{Price: "62690.4", Quantity: "1.31062803"},
			},
		},
		{ // after 03-ws-update-loud.json
			ExchangeID: 2,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks: []events.PriceLevel{
				{Price: "62700", Quantity: "2.21924167"},
				{Price: "62701.3", Quantity: "0.29045069"},
				{Price: "62710.8", Quantity: "0.33476925"},
				{Price: "62720", Quantity: "0.4"},
				{Price: "62730", Quantity: "0.75"},
			},
			Bids: []events.PriceLevel{
				{Price: "62699.5", Quantity: "0.55175335"},
				{Price: "62698.2", Quantity: "0.02744953"},
				{Price: "62690.4", Quantity: "1.31062803"},
				{Price: "60000", Quantity: "5"},
			},
		},
		{ // after 05-ws-update.json
			ExchangeID: 2,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:03Z",
			Asks: []events.PriceLevel{
				{Price: "62700", Quantity: "2.21924167"},
				{Price: "62701.3", Quantity: "0.1"},
				{Price: "62710.8", Quantity: "0.33476925"},
				{Price: "62720", Quantity: "0.4"},
				{Price: "62730", Quantity: "0.75"},
			},
			Bids: []events.PriceLevel{
				{Price: "62699.5", Quantity: "0.6"},
				{Price: "62698.2", Quantity: "0.02744953"},
				{Price: "62690.4", Quantity: "1.31062803"},
				{Price: "60000", Quantity: "5"},
			},
		},
	},
	WantRejects: []string{"out_of_order"},
}
