// Scenarios for ex1/nobitex — a REST snapshot with no offset plus Centrifugo WS deltas.
// The conventions these follow are in data.go.

package scenario

import "orderbook-e2e/events"

// Ex1RestThenWsResync — a null-sequence REST snapshot arms a resync and the next WS update adopts its offset unconditionally.
var Ex1RestThenWsResync = Scenario{
	ExchangeID: 1,
	PairID:     1,
	Sources: []string{
		// 01 rest snapshot
		`{
	"id": "f615c6f8-da26-454c-8f10-1fb059c33808",
	"simulation": 1,
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
		// 02 ws update
		`{
	"id": "c37bfc4a-1c2b-402f-8cfe-503c45884d8b",
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
				"lastUpdate": 1800000000100
			},
			"offset": 1000
		}
	}
}`,
		// 03 ws update
		`{
	"id": "2bd58608-6a1b-47a2-aa5d-70b9de6afcb1",
	"simulation": 1,
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
		// 04 rest snapshot
		`{
	"id": "cbdbb237-a629-4622-b7cb-7025d7a2f47f",
	"simulation": 1,
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
		// 05 ws update
		`{
	"id": "97e7d6c4-bb06-4309-9986-7d5718a52c3d",
	"simulation": 1,
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
		// 06 ws update
		`{
	"id": "1ca3217f-7f2a-45d3-ad98-e75f9021bfdb",
	"simulation": 1,
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
		{ // after 01 rest snapshot
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
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
		{ // after 02 ws update
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
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
		{ // after 03 ws update
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
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
		{ // after 04 rest snapshot
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
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
		{ // after 05 ws update
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
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
		{ // after 06 ws update
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
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
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 1, Simulation: 1, Price: "62850", Quantity: "1.8"},
			{ExchangeID: 1, Simulation: 1, Price: "62855", Quantity: "0.7"},
			{ExchangeID: 1, Simulation: 1, Price: "62860", Quantity: "0.4"},
			{ExchangeID: 1, Simulation: 1, Price: "62880", Quantity: "0.3"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 1, Simulation: 1, Price: "62849", Quantity: "0.95"},
			{ExchangeID: 1, Simulation: 1, Price: "62848", Quantity: "0.11"},
			{ExchangeID: 1, Simulation: 1, Price: "62845", Quantity: "0.9"},
			{ExchangeID: 1, Simulation: 1, Price: "62840", Quantity: "1.2"},
			{ExchangeID: 1, Simulation: 1, Price: "62825", Quantity: "1.1"},
		},
	},
}

// Ex1UpdateBeforeSnapshot — a WS delta before any REST snapshot has no baseline to apply to.
var Ex1UpdateBeforeSnapshot = Scenario{
	ExchangeID: 1,
	PairID:     1,
	Sources: []string{
		// 01 ws update no baseline
		`{
	"id": "41d21fe3-dbd2-4516-b6fe-7b7fe0888c13",
	"simulation": 1,
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
		// 02 rest snapshot
		`{
	"id": "e3c2319d-6312-4ac1-9ff6-a2798b2aab75",
	"simulation": 1,
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
		// 03 ws update
		`{
	"id": "10fc304a-a135-4f0b-b783-2a92552e6e32",
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
				"lastUpdate": 1800000000200
			},
			"offset": 2000
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 02 rest snapshot
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
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
		{ // after 03 ws update
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
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
	// The cold delta is also what makes job 2 ask NiFi for a snapshot: it has no
	// baseline, and only a snapshot can give it one.
	WantControlCommands: []events.ControlCommand{
		{Action: "snapshot_request", ExchangeID: 1, PairID: 1},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 1, Simulation: 1, Price: "62650", Quantity: "2.21924167"},
			{ExchangeID: 1, Simulation: 1, Price: "62651", Quantity: "0.29045069"},
			{ExchangeID: 1, Simulation: 1, Price: "62655", Quantity: "1.05"},
			{ExchangeID: 1, Simulation: 1, Price: "62660", Quantity: "0.33476925"},
			{ExchangeID: 1, Simulation: 1, Price: "62670", Quantity: "0.4"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 1, Simulation: 1, Price: "62649", Quantity: "0.55175335"},
			{ExchangeID: 1, Simulation: 1, Price: "62648", Quantity: "0.02744953"},
			{ExchangeID: 1, Simulation: 1, Price: "62647", Quantity: "0.20630833"},
			{ExchangeID: 1, Simulation: 1, Price: "62645", Quantity: "0.9"},
			{ExchangeID: 1, Simulation: 1, Price: "62640", Quantity: "1.31062803"},
			{ExchangeID: 1, Simulation: 1, Price: "62638", Quantity: "1.1"},
		},
	},
}

// Ex1SequenceGap — Centrifugo offsets step by exactly one, so any skip is a gap; only a REST snapshot can re-arm.
var Ex1SequenceGap = Scenario{
	ExchangeID: 1,
	PairID:     1,
	Sources: []string{
		// 01 rest snapshot
		`{
	"id": "966de6b0-e10b-4df8-8929-d2ab6d8fa7a8",
	"simulation": 1,
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
		// 02 ws update
		`{
	"id": "8fc6cd1d-252d-49aa-ba8a-0d8cd3e611b3",
	"simulation": 1,
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
		// 03 ws update ok
		`{
	"id": "8e59c061-4a89-47ef-898d-e1dcb1d3481a",
	"simulation": 1,
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
		// 04 ws update gap
		`{
	"id": "4cd041a7-1125-4734-b7f7-fec7869897c0",
	"simulation": 1,
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
		// 05 ws update awaiting
		`{
	"id": "12840d39-d731-48e7-83c3-acae33029ffe",
	"simulation": 1,
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
		// 06 rest snapshot resync
		`{
	"id": "ebf1d333-48ff-4294-b3f6-0681f6b84347",
	"simulation": 1,
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
		// 07 ws update
		`{
	"id": "48172937-bec2-42c6-8026-52b4770dbd79",
	"simulation": 1,
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
		// 08 ws update ok
		`{
	"id": "9e0a8ea9-222b-4b28-87ac-f343c06fe7d6",
	"simulation": 1,
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
		{ // after 01 rest snapshot
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
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
		{ // after 02 ws update
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
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
		{ // after 03 ws update ok
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
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
		{ // after 04 ws update gap (reset)
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks:       []events.PriceLevel{},
			Bids:       []events.PriceLevel{},
		},
		{ // after 06 rest snapshot resync
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
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
		{ // after 07 ws update
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
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
		{ // after 08 ws update ok
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
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
	// One command for the episode, not one per rejected event: the second update
	// rejects on the same unresolved gap, and job 2 does not re-ask.
	WantControlCommands: []events.ControlCommand{
		{Action: "snapshot_request", ExchangeID: 1, PairID: 1},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 1, Simulation: 1, Price: "63050", Quantity: "1.8"},
			{ExchangeID: 1, Simulation: 1, Price: "63055", Quantity: "0.7"},
			{ExchangeID: 1, Simulation: 1, Price: "63060", Quantity: "0.4"},
			{ExchangeID: 1, Simulation: 1, Price: "63080", Quantity: "0.3"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 1, Simulation: 1, Price: "63049", Quantity: "0.95"},
			{ExchangeID: 1, Simulation: 1, Price: "63048", Quantity: "0.11"},
			{ExchangeID: 1, Simulation: 1, Price: "63045", Quantity: "0.9"},
			{ExchangeID: 1, Simulation: 1, Price: "63040", Quantity: "1.2"},
			{ExchangeID: 1, Simulation: 1, Price: "63025", Quantity: "1.1"},
		},
	},
}

// Ex1NoiseFrames — Centrifugo noise is dropped without consuming an offset or arming a resync.
var Ex1NoiseFrames = Scenario{
	ExchangeID: 1,
	PairID:     1,
	Sources: []string{
		// 01 connect ack
		`{
	"id": "991c3ddc-436e-4530-8417-4aa87c2abf87",
	"simulation": 1,
	"connect": {
		"client": "a4d3ae55-9f2c-4c31-8f0e-1b2c3d4e5f60",
		"version": "5.0.0"
	}
}`,
		// 02 foreign channel
		`{
	"id": "0cf96ec4-33a2-44e7-914f-81dfa41761c3",
	"simulation": 1,
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
		// 03 rest snapshot
		`{
	"id": "f07db891-dd2c-4197-9870-6cdafc7ebbf4",
	"simulation": 1,
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
		// 04 malformed book
		`{
	"id": "e872d13c-3b6f-4f38-8078-e0228a62e2c1",
	"simulation": 1,
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
		// 05 ws update
		`{
	"id": "10c02102-cc81-4d00-be4d-65a8862a48ad",
	"simulation": 1,
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
		// 06 ws update
		`{
	"id": "a357fea8-e985-4b91-b41b-cafa9f5db80e",
	"simulation": 1,
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
		{ // after 03 rest snapshot
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
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
		{ // after 05 ws update
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
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
		{ // after 06 ws update
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
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
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 1, Simulation: 1, Price: "62950", Quantity: "2.21924167"},
			{ExchangeID: 1, Simulation: 1, Price: "62955", Quantity: "1.05"},
			{ExchangeID: 1, Simulation: 1, Price: "62960", Quantity: "0.33476925"},
			{ExchangeID: 1, Simulation: 1, Price: "62980", Quantity: "0.3"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 1, Simulation: 1, Price: "62949", Quantity: "0.65"},
			{ExchangeID: 1, Simulation: 1, Price: "62948", Quantity: "0.02744953"},
			{ExchangeID: 1, Simulation: 1, Price: "62945", Quantity: "0.9"},
			{ExchangeID: 1, Simulation: 1, Price: "62940", Quantity: "1.31062803"},
			{ExchangeID: 1, Simulation: 1, Price: "62935", Quantity: "1.1"},
		},
	},
}

// Ex1StaleRestReplay — a REST snapshot carries no offset, so a replayed old one is caught by event time alone.
var Ex1StaleRestReplay = Scenario{
	ExchangeID: 1,
	PairID:     1,
	Sources: []string{
		// 01 rest snapshot
		`{
	"id": "a1964397-ee11-4aba-91ef-d73273b7fcec",
	"simulation": 1,
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
		// 02 ws update
		`{
	"id": "510f6ac4-797c-495b-a883-86b1fa0b978a",
	"simulation": 1,
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
		// 03 ws update loud
		`{
	"id": "6305c64d-4919-4539-b6de-3028dc727b1a",
	"simulation": 1,
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
		// 04 rest snapshot stale replay
		`{
	"id": "1f918180-0f53-457a-ab58-c7a65afb139b",
	"simulation": 1,
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
		// 05 ws update
		`{
	"id": "b0e39084-ed3d-40ad-a2d0-d83e8162ecf9",
	"simulation": 1,
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
		{ // after 01 rest snapshot
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
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
		{ // after 02 ws update
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
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
		{ // after 03 ws update loud
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
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
		{ // after 05 ws update
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
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
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 1, Simulation: 1, Price: "62650", Quantity: "2.21924167"},
			{ExchangeID: 1, Simulation: 1, Price: "62651", Quantity: "0.1"},
			{ExchangeID: 1, Simulation: 1, Price: "62660", Quantity: "0.33476925"},
			{ExchangeID: 1, Simulation: 1, Price: "62670", Quantity: "0.4"},
			{ExchangeID: 1, Simulation: 1, Price: "62680", Quantity: "0.75"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 1, Simulation: 1, Price: "62649", Quantity: "0.6"},
			{ExchangeID: 1, Simulation: 1, Price: "62648", Quantity: "0.02744953"},
			{ExchangeID: 1, Simulation: 1, Price: "62640", Quantity: "1.31062803"},
			{ExchangeID: 1, Simulation: 1, Price: "60000", Quantity: "5"},
		},
	},
}

// Ex1PrecisionDust — job 4 on a nobitex feed: prices that collide once truncated to the market's
// 2 places merge into one level with their quantities summed, and a quantity below the market's 8
// places truncates to zero, which job 5 reads as a delete. Pair 1 rebases by 10^0, so the numbers
// that move here moved in job 4 and nowhere else.
var Ex1PrecisionDust = Scenario{
	ExchangeID: 1,
	PairID:     1,
	Sources: []string{
		// 01 rest snapshot
		`{
	"id": "e766f82d-2cfa-435d-9096-ce1679cbdbcc",
	"simulation": 1,
	"action": "snapshot",
	"pair": "BTCUSDT",
	"status": "ok",
	"lastUpdate": 1800000000000,
	"lastTradePrice": "62650",
	"bids": [
		["62650.123", "0.40000000"],
		["62650.129", "0.25000000"],
		["62649.5", "1.00000000"]
	],
	"asks": [
		["62651.006", "0.30000000"],
		["62652", "0.000000005"]
	]
}`,
		// 02 ws update
		`{
	"id": "41ed1b1e-dfff-4ad2-89fa-6d0d41b449e0",
	"simulation": 1,
	"push": {
		"channel": "public:orderbook-BTCUSDT",
		"pub": {
			"data": {
				"asks": [
					["62651.004", "0.000000009"],
					["62653.999", "0.75000000"]
				],
				"bids": [
					["62650.121", "0.10000000"],
					["62650.128", "0.20000000"]
				],
				"lastTradePrice": "62652",
				"lastUpdate": 1800000000100
			},
			"offset": 5000
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 rest snapshot — .123 and .129 merged at .12 (0.4+0.25), the dust ask never rested
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62651", Quantity: "0.3"},
			},
			Bids: []events.PriceLevel{
				{Price: "62650.12", Quantity: "0.65"},
				{Price: "62649.5", Quantity: "1"},
			},
		},
		{ // after 02 ws update — dust deleted the 62651 ask that was already resting there
			ExchangeID: 1,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
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
		Asks: []events.AggregatedLevel{{ExchangeID: 1, Simulation: 1, Price: "62653.99", Quantity: "0.75"}},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 1, Simulation: 1, Price: "62650.12", Quantity: "0.3"},
			{ExchangeID: 1, Simulation: 1, Price: "62649.5", Quantity: "1"},
		},
	},
}

// Ex1RebaseToman — job 3's IRR→Toman case. Nobitex quotes IRT markets in rials; the platform
// stores tomans, so `exchange_markets(1,'1K_SHIBIRT')` carries price_amount_rebase -1 and the
// price is shifted one place down before anything else sees it. Volume is untouched (rebase 0).
// The prices are chosen so the shift lands a third decimal on the market's 2 places, which
// job 4 then truncates DOWN — proving rebase runs first and truncation second.
var Ex1RebaseToman = Scenario{
	ExchangeID: 1,
	PairID:     52,
	Sources: []string{
		// 01 rest snapshot, prices in rials
		`{
	"id": "130ded2b-d78b-41e3-b587-c6e2d2c3c079",
	"simulation": 1,
	"action": "snapshot",
	"pair": "1K_SHIBIRT",
	"status": "ok",
	"lastUpdate": 1800000000000,
	"lastTradePrice": "8525",
	"bids": [
		["8523.45", "1500.00000000"],
		["8520", "2000.00000000"],
		["8510.9", "3000.00000000"]
	],
	"asks": [
		["8530.55", "1200.00000000"],
		["8541", "800.00000000"]
	]
}`,
		// 02 ws update
		`{
	"id": "327631c0-1f27-4ca6-a5f0-ac1d5fbdb952",
	"simulation": 1,
	"push": {
		"channel": "public:orderbook-1K_SHIBIRT",
		"pub": {
			"data": {
				"asks": [
					["8530.55", "0"],
					["8535.99", "500.00000000"]
				],
				"bids": [
					["8520", "2500.00000000"]
				],
				"lastTradePrice": "8530",
				"lastUpdate": 1800000000100
			},
			"offset": 4000
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 rest snapshot — 8523.45 rials is 852.345 tomans, truncated DOWN to 852.34
			ExchangeID: 1,
			PairID:     52,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "853.05", Quantity: "1200"},
				{Price: "854.1", Quantity: "800"},
			},
			Bids: []events.PriceLevel{
				{Price: "852.34", Quantity: "1500"},
				{Price: "852", Quantity: "2000"},
				{Price: "851.09", Quantity: "3000"},
			},
		},
		{ // after 02 ws update — the delete is matched on the rebased price, not the wire one
			ExchangeID: 1,
			PairID:     52,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "853.59", Quantity: "500"},
				{Price: "854.1", Quantity: "800"},
			},
			Bids: []events.PriceLevel{
				{Price: "852.34", Quantity: "1500"},
				{Price: "852", Quantity: "2500"},
				{Price: "851.09", Quantity: "3000"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 1, Simulation: 1, Price: "853.59", Quantity: "500"},
			{ExchangeID: 1, Simulation: 1, Price: "854.1", Quantity: "800"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 1, Simulation: 1, Price: "852.34", Quantity: "1500"},
			{ExchangeID: 1, Simulation: 1, Price: "852", Quantity: "2500"},
			{ExchangeID: 1, Simulation: 1, Price: "851.09", Quantity: "3000"},
		},
	},
}

// Ex1RebaseScaledUnit — job 3's scaled-unit case, and the only pair in the suite where BOTH
// rebase exponents are non-zero. Nobitex quotes PEPE per 1,000,000 units, so
// `exchange_markets(1,'1M_PEPEUSDT')` is price -6 / volume +6: the price divides by a million
// and the quantity multiplies by one, landing on per-1-PEPE numbers. Pair 17 is also the only
// pair seeded at precision 10/10, so the truncation happens ten places out instead of two.
//
// The last ask of 01 is the ordering proof: 10^-17 of a 1M unit is 10^-11 whole PEPE, which
// survives the +6 rebase and only then dies at the market's 10 places. Rebase first, truncate
// second — the other order would have kept it.
var Ex1RebaseScaledUnit = Scenario{
	ExchangeID: 1,
	PairID:     17,
	Sources: []string{
		// 01 rest snapshot, priced per 1M PEPE
		`{
	"id": "574de2d6-8339-4362-baa0-640b2b509bc6",
	"simulation": 1,
	"action": "snapshot",
	"pair": "1M_PEPEUSDT",
	"status": "ok",
	"lastUpdate": 1800000000000,
	"lastTradePrice": "12.35",
	"bids": [
		["12.3456789", "0.00000005"],
		["12.3", "1.50000000"]
	],
	"asks": [
		["12.4", "2.25000000"],
		["12.45678901234", "0.00100000"],
		["12.6", "0.00000000000000001"]
	]
}`,
		// 02 ws update
		`{
	"id": "b2e3a6b2-6b0a-4e0d-b8ad-7b8d9d6b162e",
	"simulation": 1,
	"push": {
		"channel": "public:orderbook-1M_PEPEUSDT",
		"pub": {
			"data": {
				"asks": [
					["12.4", "0"],
					["12.5", "0.50000000"]
				],
				"bids": [
					["12.3456789", "0.00000001"]
				],
				"lastTradePrice": "12.45",
				"lastUpdate": 1800000000100
			},
			"offset": 3000
		}
	}
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 rest snapshot — the 12.6 ask rebased to 10^-11 PEPE and truncated away
			ExchangeID: 1,
			PairID:     17,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "0.0000124", Quantity: "2250000"},
				{Price: "0.0000124567", Quantity: "1000"},
			},
			Bids: []events.PriceLevel{
				{Price: "0.0000123456", Quantity: "0.05"},
				{Price: "0.0000123", Quantity: "1500000"},
			},
		},
		{ // after 02 ws update
			ExchangeID: 1,
			PairID:     17,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "0.0000124567", Quantity: "1000"},
				{Price: "0.0000125", Quantity: "500000"},
			},
			Bids: []events.PriceLevel{
				{Price: "0.0000123456", Quantity: "0.01"},
				{Price: "0.0000123", Quantity: "1500000"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 1, Simulation: 1, Price: "0.0000124567", Quantity: "1000"},
			{ExchangeID: 1, Simulation: 1, Price: "0.0000125", Quantity: "500000"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 1, Simulation: 1, Price: "0.0000123456", Quantity: "0.01"},
			{ExchangeID: 1, Simulation: 1, Price: "0.0000123", Quantity: "1500000"},
		},
	},
}
