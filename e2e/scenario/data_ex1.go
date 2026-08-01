// Scenarios from flink/normalizer/manual-test-data for
// ex1/nobitex — a REST snapshot with no offset plus Centrifugo WS deltas.
// The conventions these follow are in data.go.

package scenario

import "orderbook-e2e/events"

// Ex1RestThenWsResync is 08-ex1-rest-then-ws-resync —
// a null-sequence REST snapshot arms a resync and the next WS update adopts its offset unconditionally.
var Ex1RestThenWsResync = Scenario{
	ExchangeID: 1,
	PairID:     1,
	Sources: []string{
		// 01-rest-snapshot.json
		`{
	"action": "snapshot",
	"pair": "BTCUSDT",
	"status": "ok",
	"lastUpdate": 1800000000000,
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
		// 02-ws-update.json
		`{
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
				"lastUpdate": 1800000000100
			},
			"offset": 1000
		}
	}
}`,
		// 03-ws-update.json
		`{
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"asks": [
					["62651", "0"],
					["62680", "0.75000000"]
				],
				"bids": [
					["62638", "0"],
					["62635", "2.00000000"]
				],
				"lastTradePrice": "62655",
				"lastUpdate": 1800000000200
			},
			"offset": 1001
		}
	}
}`,
		// 04-rest-snapshot.json
		`{
	"action": "snapshot",
	"pair": "BTCUSDT",
	"status": "ok",
	"lastUpdate": 1800000000300,
	"lastTradePrice": "62850",
	"bids": [
		["62849", "0.60000000"],
		["62848", "0.11000000"],
		["62845", "0.90000000"],
		["62840", "1.20000000"]
	],
	"asks": [
		["62850", "1.80000000"],
		["62851", "0.22000000"],
		["62855", "0.70000000"],
		["62860", "0.40000000"]
	]
}`,
		// 05-ws-update.json
		`{
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"asks": [
					["62851", "0"],
					["62870", "0.55000000"]
				],
				"bids": [
					["62849", "0.95000000"],
					["62830", "1.40000000"]
				],
				"lastTradePrice": "62855",
				"lastUpdate": 1800000000400
			},
			"offset": 9000
		}
	}
}`,
		// 06-ws-update.json
		`{
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"asks": [
					["62870", "0"],
					["62880", "0.30000000"]
				],
				"bids": [
					["62830", "0"],
					["62825", "1.10000000"]
				],
				"lastTradePrice": "62860",
				"lastUpdate": 1800000000500
			},
			"offset": 9001
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01-rest-snapshot.json
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
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
		{ // after 02-ws-update.json
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
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
		{ // after 03-ws-update.json
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62650", Quantity: "2.21924167"},
				{Price: "62655", Quantity: "1.05"},
				{Price: "62660", Quantity: "0.33476925"},
				{Price: "62670", Quantity: "0.4"},
				{Price: "62680", Quantity: "0.75"},
			},
			Bids: []events.PriceLevel{
				{Price: "62649", Quantity: "0.55175335"},
				{Price: "62648", Quantity: "0.02744953"},
				{Price: "62647", Quantity: "0.20630833"},
				{Price: "62645", Quantity: "0.9"},
				{Price: "62640", Quantity: "1.31062803"},
				{Price: "62635", Quantity: "2"},
			},
		},
		{ // after 04-rest-snapshot.json
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62850", Quantity: "1.8"},
				{Price: "62851", Quantity: "0.22"},
				{Price: "62855", Quantity: "0.7"},
				{Price: "62860", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62849", Quantity: "0.6"},
				{Price: "62848", Quantity: "0.11"},
				{Price: "62845", Quantity: "0.9"},
				{Price: "62840", Quantity: "1.2"},
			},
		},
		{ // after 05-ws-update.json
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62850", Quantity: "1.8"},
				{Price: "62855", Quantity: "0.7"},
				{Price: "62860", Quantity: "0.4"},
				{Price: "62870", Quantity: "0.55"},
			},
			Bids: []events.PriceLevel{
				{Price: "62849", Quantity: "0.95"},
				{Price: "62848", Quantity: "0.11"},
				{Price: "62845", Quantity: "0.9"},
				{Price: "62840", Quantity: "1.2"},
				{Price: "62830", Quantity: "1.4"},
			},
		},
		{ // after 06-ws-update.json
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62850", Quantity: "1.8"},
				{Price: "62855", Quantity: "0.7"},
				{Price: "62860", Quantity: "0.4"},
				{Price: "62880", Quantity: "0.3"},
			},
			Bids: []events.PriceLevel{
				{Price: "62849", Quantity: "0.95"},
				{Price: "62848", Quantity: "0.11"},
				{Price: "62845", Quantity: "0.9"},
				{Price: "62840", Quantity: "1.2"},
				{Price: "62825", Quantity: "1.1"},
			},
		},
	},
}

// Ex1UpdateBeforeSnapshot is 09-ex1-update-before-snapshot —
// a WS delta before any REST snapshot has no baseline to apply to.
var Ex1UpdateBeforeSnapshot = Scenario{
	ExchangeID: 1,
	PairID:     1,
	Sources: []string{
		// 01-ws-update-no-baseline.json
		`{
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"asks": [
					["62651", "0.29045069"],
					["62670", "0.40000000"]
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
		// 02-rest-snapshot.json
		`{
	"action": "snapshot",
	"pair": "BTCUSDT",
	"status": "ok",
	"lastUpdate": 1800000000100,
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
		// 03-ws-update.json
		`{
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
				"lastUpdate": 1800000000200
			},
			"offset": 2000
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 02-rest-snapshot.json
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
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
		{ // after 03-ws-update.json
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
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
	},
	WantRejects: []string{"no_baseline"},
}

// Ex1SequenceGap is 10-ex1-sequence-gap —
// Centrifugo offsets step by exactly one, so any skip is a gap; only a REST snapshot can re-arm.
var Ex1SequenceGap = Scenario{
	ExchangeID: 1,
	PairID:     1,
	Sources: []string{
		// 01-rest-snapshot.json
		`{
	"action": "snapshot",
	"pair": "BTCUSDT",
	"status": "ok",
	"lastUpdate": 1800000000000,
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
		// 02-ws-update.json
		`{
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"asks": [
					["62651", "0.29045069"],
					["62670", "0.40000000"]
				],
				"bids": [
					["62649", "0.55175335"]
				],
				"lastTradePrice": "62650",
				"lastUpdate": 1800000000100
			},
			"offset": 1000
		}
	}
}`,
		// 03-ws-update-ok.json
		`{
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"asks": [
					["62680", "0.75000000"]
				],
				"bids": [
					["62635", "2.00000000"]
				],
				"lastTradePrice": "62655",
				"lastUpdate": 1800000000200
			},
			"offset": 1001
		}
	}
}`,
		// 04-ws-update-gap.json
		`{
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"asks": [
					["62690", "9.99900000"]
				],
				"bids": [
					["62630", "9.99900000"]
				],
				"lastTradePrice": "62660",
				"lastUpdate": 1800000000300
			},
			"offset": 1005
		}
	}
}`,
		// 05-ws-update-awaiting.json
		`{
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"asks": [
					["62695", "9.99900000"]
				],
				"bids": [
					["62625", "9.99900000"]
				],
				"lastTradePrice": "62665",
				"lastUpdate": 1800000000400
			},
			"offset": 1002
		}
	}
}`,
		// 06-rest-snapshot-resync.json
		`{
	"action": "snapshot",
	"pair": "BTCUSDT",
	"status": "ok",
	"lastUpdate": 1800000000500,
	"lastTradePrice": "63050",
	"bids": [
		["63049", "0.60000000"],
		["63048", "0.11000000"],
		["63045", "0.90000000"],
		["63040", "1.20000000"]
	],
	"asks": [
		["63050", "1.80000000"],
		["63051", "0.22000000"],
		["63055", "0.70000000"],
		["63060", "0.40000000"]
	]
}`,
		// 07-ws-update.json
		`{
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"asks": [
					["63051", "0"],
					["63070", "0.55000000"]
				],
				"bids": [
					["63049", "0.95000000"],
					["63030", "1.40000000"]
				],
				"lastTradePrice": "63055",
				"lastUpdate": 1800000000600
			},
			"offset": 2000
		}
	}
}`,
		// 08-ws-update-ok.json
		`{
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"asks": [
					["63070", "0"],
					["63080", "0.30000000"]
				],
				"bids": [
					["63030", "0"],
					["63025", "1.10000000"]
				],
				"lastTradePrice": "63060",
				"lastUpdate": 1800000000700
			},
			"offset": 2001
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01-rest-snapshot.json
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
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
		{ // after 02-ws-update.json
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62650", Quantity: "2.21924167"},
				{Price: "62651", Quantity: "0.29045069"},
				{Price: "62652", Quantity: "0.19067482"},
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
			},
		},
		{ // after 03-ws-update-ok.json
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62650", Quantity: "2.21924167"},
				{Price: "62651", Quantity: "0.29045069"},
				{Price: "62652", Quantity: "0.19067482"},
				{Price: "62655", Quantity: "1.05"},
				{Price: "62660", Quantity: "0.33476925"},
				{Price: "62670", Quantity: "0.4"},
				{Price: "62680", Quantity: "0.75"},
			},
			Bids: []events.PriceLevel{
				{Price: "62649", Quantity: "0.55175335"},
				{Price: "62648", Quantity: "0.02744953"},
				{Price: "62647", Quantity: "0.20630833"},
				{Price: "62645", Quantity: "0.9"},
				{Price: "62640", Quantity: "1.31062803"},
				{Price: "62635", Quantity: "2"},
			},
		},
		{ // after 04-ws-update-gap.json (reset)
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{},
			Bids:       []events.PriceLevel{},
		},
		{ // after 06-rest-snapshot-resync.json
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "63050", Quantity: "1.8"},
				{Price: "63051", Quantity: "0.22"},
				{Price: "63055", Quantity: "0.7"},
				{Price: "63060", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "63049", Quantity: "0.6"},
				{Price: "63048", Quantity: "0.11"},
				{Price: "63045", Quantity: "0.9"},
				{Price: "63040", Quantity: "1.2"},
			},
		},
		{ // after 07-ws-update.json
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "63050", Quantity: "1.8"},
				{Price: "63055", Quantity: "0.7"},
				{Price: "63060", Quantity: "0.4"},
				{Price: "63070", Quantity: "0.55"},
			},
			Bids: []events.PriceLevel{
				{Price: "63049", Quantity: "0.95"},
				{Price: "63048", Quantity: "0.11"},
				{Price: "63045", Quantity: "0.9"},
				{Price: "63040", Quantity: "1.2"},
				{Price: "63030", Quantity: "1.4"},
			},
		},
		{ // after 08-ws-update-ok.json
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "63050", Quantity: "1.8"},
				{Price: "63055", Quantity: "0.7"},
				{Price: "63060", Quantity: "0.4"},
				{Price: "63080", Quantity: "0.3"},
			},
			Bids: []events.PriceLevel{
				{Price: "63049", Quantity: "0.95"},
				{Price: "63048", Quantity: "0.11"},
				{Price: "63045", Quantity: "0.9"},
				{Price: "63040", Quantity: "1.2"},
				{Price: "63025", Quantity: "1.1"},
			},
		},
	},
	WantRejects: []string{"sequence_gap", "awaiting_snapshot"},
}

// Ex1NoiseFrames is 11-ex1-noise-frames —
// Centrifugo noise is dropped without consuming an offset or arming a resync.
var Ex1NoiseFrames = Scenario{
	ExchangeID: 1,
	PairID:     1,
	Sources: []string{
		// 01-connect-ack.json
		`{
	"connect": {
		"client": "a4d3ae55-9f2c-4c31-8f0e-1b2c3d4e5f60",
		"version": "5.0.0"
	}
}`,
		// 02-foreign-channel.json
		`{
	"push": {
		"channel": "public:trades-BTCUSDT",
		"pub": {
			"data": {
				"price": "62650",
				"quantity": "0.01",
				"lastUpdate": 1800000000000
			},
			"offset": 5551
		}
	}
}`,
		// 03-rest-snapshot.json
		`{
	"action": "snapshot",
	"pair": "BTCUSDT",
	"status": "ok",
	"lastUpdate": 1800000000000,
	"lastTradePrice": "62950",
	"bids": [
		["62949", "0.50000000"],
		["62948", "0.02744953"],
		["62945", "0.90000000"],
		["62940", "1.31062803"]
	],
	"asks": [
		["62950", "2.21924167"],
		["62951", "0.17447383"],
		["62955", "1.05000000"],
		["62960", "0.33476925"]
	]
}`,
		// 04-malformed-book.json
		`{
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"lastTradePrice": "62950",
				"lastUpdate": 1800000000050
			},
			"offset": 6001
		}
	}
}`,
		// 05-ws-update.json
		`{
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"asks": [
					["62951", "0"],
					["62970", "0.40000000"]
				],
				"bids": [
					["62949", "0.65000000"],
					["62938", "1.10000000"]
				],
				"lastTradePrice": "62955",
				"lastUpdate": 1800000000100
			},
			"offset": 1000
		}
	}
}`,
		// 06-ws-update.json
		`{
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"asks": [
					["62970", "0"],
					["62980", "0.30000000"]
				],
				"bids": [
					["62938", "0"],
					["62935", "1.10000000"]
				],
				"lastTradePrice": "62960",
				"lastUpdate": 1800000000200
			},
			"offset": 1001
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 03-rest-snapshot.json
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62950", Quantity: "2.21924167"},
				{Price: "62951", Quantity: "0.17447383"},
				{Price: "62955", Quantity: "1.05"},
				{Price: "62960", Quantity: "0.33476925"},
			},
			Bids: []events.PriceLevel{
				{Price: "62949", Quantity: "0.5"},
				{Price: "62948", Quantity: "0.02744953"},
				{Price: "62945", Quantity: "0.9"},
				{Price: "62940", Quantity: "1.31062803"},
			},
		},
		{ // after 05-ws-update.json
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62950", Quantity: "2.21924167"},
				{Price: "62955", Quantity: "1.05"},
				{Price: "62960", Quantity: "0.33476925"},
				{Price: "62970", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62949", Quantity: "0.65"},
				{Price: "62948", Quantity: "0.02744953"},
				{Price: "62945", Quantity: "0.9"},
				{Price: "62940", Quantity: "1.31062803"},
				{Price: "62938", Quantity: "1.1"},
			},
		},
		{ // after 06-ws-update.json
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62950", Quantity: "2.21924167"},
				{Price: "62955", Quantity: "1.05"},
				{Price: "62960", Quantity: "0.33476925"},
				{Price: "62980", Quantity: "0.3"},
			},
			Bids: []events.PriceLevel{
				{Price: "62949", Quantity: "0.65"},
				{Price: "62948", Quantity: "0.02744953"},
				{Price: "62945", Quantity: "0.9"},
				{Price: "62940", Quantity: "1.31062803"},
				{Price: "62935", Quantity: "1.1"},
			},
		},
	},
}

// Ex1StaleRestReplay is 12-ex1-stale-rest-replay —
// a REST snapshot carries no offset, so a replayed old one is caught by event time alone.
var Ex1StaleRestReplay = Scenario{
	ExchangeID: 1,
	PairID:     1,
	Sources: []string{
		// 01-rest-snapshot.json
		`{
	"action": "snapshot",
	"pair": "BTCUSDT",
	"status": "ok",
	"lastUpdate": 1800000000000,
	"lastTradePrice": "62650",
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
}`,
		// 02-ws-update.json
		`{
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"asks": [
					["62651", "0.29045069"],
					["62670", "0.40000000"]
				],
				"bids": [
					["62649", "0.55175335"]
				],
				"lastTradePrice": "62650",
				"lastUpdate": 1800000000100
			},
			"offset": 1000
		}
	}
}`,
		// 03-ws-update-loud.json
		`{
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"asks": [
					["62680", "0.75000000"]
				],
				"bids": [
					["60000", "5.00000000"]
				],
				"lastTradePrice": "62650",
				"lastUpdate": 1800000000200
			},
			"offset": 1001
		}
	}
}`,
		// 04-rest-snapshot-stale-replay.json
		`{
	"action": "snapshot",
	"pair": "BTCUSDT",
	"status": "ok",
	"lastUpdate": 1800000000000,
	"lastTradePrice": "62650",
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
}`,
		// 05-ws-update.json
		`{
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"asks": [
					["62651", "0.10000000"]
				],
				"bids": [
					["62649", "0.60000000"]
				],
				"lastTradePrice": "62650",
				"lastUpdate": 1800000000300
			},
			"offset": 1002
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01-rest-snapshot.json
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
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
		{ // after 02-ws-update.json
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
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
		{ // after 03-ws-update-loud.json
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62650", Quantity: "2.21924167"},
				{Price: "62651", Quantity: "0.29045069"},
				{Price: "62660", Quantity: "0.33476925"},
				{Price: "62670", Quantity: "0.4"},
				{Price: "62680", Quantity: "0.75"},
			},
			Bids: []events.PriceLevel{
				{Price: "62649", Quantity: "0.55175335"},
				{Price: "62648", Quantity: "0.02744953"},
				{Price: "62640", Quantity: "1.31062803"},
				{Price: "60000", Quantity: "5"},
			},
		},
		{ // after 05-ws-update.json
			ExchangeID: 1,
			PairID:     1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62650", Quantity: "2.21924167"},
				{Price: "62651", Quantity: "0.1"},
				{Price: "62660", Quantity: "0.33476925"},
				{Price: "62670", Quantity: "0.4"},
				{Price: "62680", Quantity: "0.75"},
			},
			Bids: []events.PriceLevel{
				{Price: "62649", Quantity: "0.6"},
				{Price: "62648", Quantity: "0.02744953"},
				{Price: "62640", Quantity: "1.31062803"},
				{Price: "60000", Quantity: "5"},
			},
		},
	},
	WantRejects: []string{"out_of_order"},
}
