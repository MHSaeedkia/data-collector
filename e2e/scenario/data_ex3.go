// Scenarios from flink/normalizer/manual-test-data for
// ex3/wallex — one side per message, no ordering field, no timestamp.
// The conventions these follow are in data.go.

package scenario

import "orderbook-e2e/events"

// Ex3WallexHalfBook is 07-ex3-wallex-half-book —
// wallex sends one side per message; a null side must never wipe the other.
var Ex3WallexHalfBook = Scenario{
	ExchangeID:      3,
	PairID:          1,
	IgnoreEventTime: true,
	Sources: []string{
		// 01-buy-depth.json
		`[
	"BTCUSDT@buyDepth",
	[
		{ "price": 62942.5, "quantity": 0.8, "sum": 50354.0 },
		{ "price": 62937.5, "quantity": 1.6, "sum": 100700.0 },
		{ "price": 62932.5, "quantity": 2.9, "sum": 182504.25 }
	]
]`,
		// 02-sell-depth.json
		`[
	"BTCUSDT@sellDepth",
	[
		{ "price": 62952.5, "quantity": 0.7, "sum": 44066.75 },
		{ "price": 62957.5, "quantity": 1.4, "sum": 88140.5 },
		{ "price": 62962.5, "quantity": 2.2, "sum": 138517.5 }
	]
]`,
		// 03-buy-depth-refresh.json
		`[
	"BTCUSDT@buyDepth",
	[
		{ "price": 62942.5, "quantity": 0.5, "sum": 31471.25 },
		{ "price": 62927.5, "quantity": 3.5, "sum": 220246.25 }
	]
]`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01-buy-depth.json
			ExchangeID: 3,
			PairID:     1,
			Asks:       []events.PriceLevel{},
			Bids: []events.PriceLevel{
				{Price: "62942.5", Quantity: "0.8"},
				{Price: "62937.5", Quantity: "1.6"},
				{Price: "62932.5", Quantity: "2.9"},
			},
		},
		{ // after 02-sell-depth.json
			ExchangeID: 3,
			PairID:     1,
			Asks: []events.PriceLevel{
				{Price: "62952.5", Quantity: "0.7"},
				{Price: "62957.5", Quantity: "1.4"},
				{Price: "62962.5", Quantity: "2.2"},
			},
			Bids: []events.PriceLevel{
				{Price: "62942.5", Quantity: "0.8"},
				{Price: "62937.5", Quantity: "1.6"},
				{Price: "62932.5", Quantity: "2.9"},
			},
		},
		{ // after 03-buy-depth-refresh.json
			ExchangeID: 3,
			PairID:     1,
			Asks: []events.PriceLevel{
				{Price: "62952.5", Quantity: "0.7"},
				{Price: "62957.5", Quantity: "1.4"},
				{Price: "62962.5", Quantity: "2.2"},
			},
			Bids: []events.PriceLevel{
				{Price: "62942.5", Quantity: "0.5"},
				{Price: "62927.5", Quantity: "3.5"},
			},
		},
	},
}
