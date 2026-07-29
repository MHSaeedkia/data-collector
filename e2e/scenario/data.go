package scenario

import "orderbook-e2e/events"

// NobitexSnapshot feeds ex1/p1 a single nobitex BTCUSDT snapshot frame and
// expects one order book out and nothing dead-lettered.
var NobitexSnapshot = Scenario{
	ExchangeID: 1,
	PairID:     1,
	Sources: []string{`{
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
}
`},
	// What the source becomes by the time it reaches the book builder:
	// event_time is nobitex's `lastUpdate`, ex1/BTCUSDT rebases by 10^0 on both
	// sides (identity), pair 1 truncates price to 2 and quantity to 8 decimals,
	// and every value is canonicalized (trailing zeros stripped). Asks sort
	// ascending, bids descending — the source already arrives that way.
	WantSnapshots: []events.OrderbookSnapshot{
		{
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
	},
}
