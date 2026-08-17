// Scenarios for ex8/okx — a single-shape feed: every frame is a book frame carrying its
// own sequence in `ts`.
// The conventions these follow are in data.go.

package scenario

import "orderbook-e2e/events"

// Ex8UpdateBeforeSnapshot — an update with no baseline is rejected; the snapshot after it becomes one.
var Ex8UpdateBeforeSnapshot = Scenario{
	ExchangeID: 8,
	PairID:     1,
	Sources: []string{
		// 01 update no baseline
		`{
	"id": "ca0f8d01-294f-42fa-853a-7e7289f636c6",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "update",
	"data": [
		{
			"asks": [
				["62771", "0.29045069"],
				["62775", "1.05000000"]
			],
			"bids": [["62769", "0.55175335"]],
			"ts": "1800000000300"
		}
	]
}`,
		// 02 snapshot
		`{
	"id": "60a0a661-a7db-482c-96f7-d670cdfa5bd3",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "snapshot",
	"data": [
		{
			"asks": [
				["62770", "2.21924167"],
				["62771", "0.17447383"],
				["62772", "0.19067482"],
				["62775", "1.05000000"],
				["62780", "0.33476925"]
			],
			"bids": [
				["62769", "0.50795335"],
				["62768", "0.02744953"],
				["62767", "0.20630833"],
				["62765", "0.90000000"],
				["62760", "1.31062803"]
			],
			"ts": "1800000000600"
		}
	]
}`,
		// 03 update
		`{
	"id": "b87c615c-dc81-428d-8227-a00fa614e7c6",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "update",
	"data": [
		{
			"asks": [
				["62771", "0.29045069"],
				["62772", "0"],
				["62790", "0.40000000"]
			],
			"bids": [
				["62769", "0.55175335"],
				["62758", "1.10000000"]
			],
			"ts": "1800000000900"
		}
	]
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 02 snapshot
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62770", Quantity: "2.21924167"},
				{Price: "62771", Quantity: "0.17447383"},
				{Price: "62772", Quantity: "0.19067482"},
				{Price: "62775", Quantity: "1.05"},
				{Price: "62780", Quantity: "0.33476925"},
			},
			Bids: []events.PriceLevel{
				{Price: "62769", Quantity: "0.50795335"},
				{Price: "62768", Quantity: "0.02744953"},
				{Price: "62767", Quantity: "0.20630833"},
				{Price: "62765", Quantity: "0.9"},
				{Price: "62760", Quantity: "1.31062803"},
			},
		},
		{ // after 03 update
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62770", Quantity: "2.21924167"},
				{Price: "62771", Quantity: "0.29045069"},
				{Price: "62775", Quantity: "1.05"},
				{Price: "62780", Quantity: "0.33476925"},
				{Price: "62790", Quantity: "0.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62769", Quantity: "0.55175335"},
				{Price: "62768", Quantity: "0.02744953"},
				{Price: "62767", Quantity: "0.20630833"},
				{Price: "62765", Quantity: "0.9"},
				{Price: "62760", Quantity: "1.31062803"},
				{Price: "62758", Quantity: "1.1"},
			},
		},
	},
	WantRejects: []string{"no_baseline"},
	// The cold delta is also what makes job 2 ask NiFi for a snapshot: it has no
	// baseline, and only a snapshot can give it one.
	WantControlCommands: []events.ControlCommand{
		{Action: "snapshot_request", ExchangeID: 8, PairID: 1},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 8, Simulation: 1, Price: "62770", Quantity: "2.21924167"},
			{ExchangeID: 8, Simulation: 1, Price: "62771", Quantity: "0.29045069"},
			{ExchangeID: 8, Simulation: 1, Price: "62775", Quantity: "1.05"},
			{ExchangeID: 8, Simulation: 1, Price: "62780", Quantity: "0.33476925"},
			{ExchangeID: 8, Simulation: 1, Price: "62790", Quantity: "0.4"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 8, Simulation: 1, Price: "62769", Quantity: "0.55175335"},
			{ExchangeID: 8, Simulation: 1, Price: "62768", Quantity: "0.02744953"},
			{ExchangeID: 8, Simulation: 1, Price: "62767", Quantity: "0.20630833"},
			{ExchangeID: 8, Simulation: 1, Price: "62765", Quantity: "0.9"},
			{ExchangeID: 8, Simulation: 1, Price: "62760", Quantity: "1.31062803"},
			{ExchangeID: 8, Simulation: 1, Price: "62758", Quantity: "1.1"},
		},
	},
}

// Ex8HappyPath — snapshot then three consecutive updates; 04 omits bids entirely, so the bid side must survive untouched.
var Ex8HappyPath = Scenario{
	ExchangeID: 8,
	PairID:     1,
	Sources: []string{
		// 01 snapshot
		`{
	"id": "73c46589-6b55-4335-a044-eca17b9991ef",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "snapshot",
	"data": [
		{
			"asks": [
				["62800", "1.50000000"],
				["62801", "0.85000000"],
				["62802", "2.20000000"],
				["62805", "0.40000000"],
				["62810", "3.10000000"]
			],
			"bids": [
				["62799", "1.20000000"],
				["62798", "0.60000000"],
				["62795", "2.40000000"],
				["62790", "0.95000000"],
				["62785", "4.00000000"]
			],
			"ts": "1800000000300"
		}
	]
}`,
		// 02 update modify add delete
		`{
	"id": "23d6e762-3672-463e-a115-22c3b6c14bcd",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "update",
	"data": [
		{
			"asks": [
				["62800", "1.00000000"],
				["62802", "0"],
				["62807", "0.70000000"]
			],
			"bids": [["62799", "1.45000000"]],
			"ts": "1800000000600"
		}
	]
}`,
		// 03 update delete bid
		`{
	"id": "37a564f5-221a-495d-baf1-bd74671b193c",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "update",
	"data": [
		{
			"asks": [["62803", "0.33000000"]],
			"bids": [
				["62798", "0"],
				["62780", "5.50000000"]
			],
			"ts": "1800000000900"
		}
	]
}`,
		// 04 update asks only
		`{
	"id": "936124ea-e1a8-4b0f-a8d6-c4fc7a0ddc37",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "update",
	"data": [
		{
			"asks": [
				["62800", "0.90000000"],
				["62810", "0"]
			],
			"ts": "1800000001200"
		}
	]
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 snapshot
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62800", Quantity: "1.5"},
				{Price: "62801", Quantity: "0.85"},
				{Price: "62802", Quantity: "2.2"},
				{Price: "62805", Quantity: "0.4"},
				{Price: "62810", Quantity: "3.1"},
			},
			Bids: []events.PriceLevel{
				{Price: "62799", Quantity: "1.2"},
				{Price: "62798", Quantity: "0.6"},
				{Price: "62795", Quantity: "2.4"},
				{Price: "62790", Quantity: "0.95"},
				{Price: "62785", Quantity: "4"},
			},
		},
		{ // after 02 update modify add delete
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62800", Quantity: "1"},
				{Price: "62801", Quantity: "0.85"},
				{Price: "62805", Quantity: "0.4"},
				{Price: "62807", Quantity: "0.7"},
				{Price: "62810", Quantity: "3.1"},
			},
			Bids: []events.PriceLevel{
				{Price: "62799", Quantity: "1.45"},
				{Price: "62798", Quantity: "0.6"},
				{Price: "62795", Quantity: "2.4"},
				{Price: "62790", Quantity: "0.95"},
				{Price: "62785", Quantity: "4"},
			},
		},
		{ // after 03 update delete bid
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62800", Quantity: "1"},
				{Price: "62801", Quantity: "0.85"},
				{Price: "62803", Quantity: "0.33"},
				{Price: "62805", Quantity: "0.4"},
				{Price: "62807", Quantity: "0.7"},
				{Price: "62810", Quantity: "3.1"},
			},
			Bids: []events.PriceLevel{
				{Price: "62799", Quantity: "1.45"},
				{Price: "62795", Quantity: "2.4"},
				{Price: "62790", Quantity: "0.95"},
				{Price: "62785", Quantity: "4"},
				{Price: "62780", Quantity: "5.5"},
			},
		},
		{ // after 04 update asks only
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks: []events.PriceLevel{
				{Price: "62800", Quantity: "0.9"},
				{Price: "62801", Quantity: "0.85"},
				{Price: "62803", Quantity: "0.33"},
				{Price: "62805", Quantity: "0.4"},
				{Price: "62807", Quantity: "0.7"},
			},
			Bids: []events.PriceLevel{
				{Price: "62799", Quantity: "1.45"},
				{Price: "62795", Quantity: "2.4"},
				{Price: "62790", Quantity: "0.95"},
				{Price: "62785", Quantity: "4"},
				{Price: "62780", Quantity: "5.5"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 8, Simulation: 1, Price: "62800", Quantity: "0.9"},
			{ExchangeID: 8, Simulation: 1, Price: "62801", Quantity: "0.85"},
			{ExchangeID: 8, Simulation: 1, Price: "62803", Quantity: "0.33"},
			{ExchangeID: 8, Simulation: 1, Price: "62805", Quantity: "0.4"},
			{ExchangeID: 8, Simulation: 1, Price: "62807", Quantity: "0.7"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 8, Simulation: 1, Price: "62799", Quantity: "1.45"},
			{ExchangeID: 8, Simulation: 1, Price: "62795", Quantity: "2.4"},
			{ExchangeID: 8, Simulation: 1, Price: "62790", Quantity: "0.95"},
			{ExchangeID: 8, Simulation: 1, Price: "62785", Quantity: "4"},
			{ExchangeID: 8, Simulation: 1, Price: "62780", Quantity: "5.5"},
		},
	},
}

// Ex8SequenceGap — a gap empties the book and arms awaitingSnapshot, so even a contiguous update is refused until a snapshot resyncs.
var Ex8SequenceGap = Scenario{
	ExchangeID: 8,
	PairID:     1,
	Sources: []string{
		// 01 snapshot
		`{
	"id": "756f528b-e41c-4d6a-8380-6165b65e4a1e",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "snapshot",
	"data": [
		{
			"asks": [
				["62800", "1.50000000"],
				["62801", "0.85000000"],
				["62802", "2.20000000"],
				["62805", "0.40000000"],
				["62810", "3.10000000"]
			],
			"bids": [
				["62799", "1.20000000"],
				["62798", "0.60000000"],
				["62795", "2.40000000"],
				["62790", "0.95000000"],
				["62785", "4.00000000"]
			],
			"ts": "1800000000300"
		}
	]
}`,
		// 02 update ok
		`{
	"id": "9aefe424-fc12-4b8d-a311-1fe768ff7cbe",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "update",
	"data": [
		{
			"asks": [["62801", "0.95000000"]],
			"bids": [["62795", "2.60000000"]],
			"ts": "1800000000600"
		}
	]
}`,
		// 03 update gap
		`{
	"id": "9c1d91ad-24da-4a88-8687-27c50aa53ba9",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "update",
	"data": [
		{
			"asks": [["62805", "0"]],
			"bids": [["62790", "1.10000000"]],
			"ts": "1800000001500"
		}
	]
}`,
		// 04 update awaiting snapshot
		`{
	"id": "1dd810e4-7581-4173-aa1e-c19c3ee3d610",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "update",
	"data": [
		{
			"asks": [["62807", "0.50000000"]],
			"bids": [["62785", "3.80000000"]],
			"ts": "1800000001800"
		}
	]
}`,
		// 05 snapshot resync
		`{
	"id": "9e962142-8cb6-4326-bfc8-cc2986996abb",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "snapshot",
	"data": [
		{
			"asks": [
				["63000", "2.00000000"],
				["63010", "1.10000000"],
				["63020", "0.75000000"]
			],
			"bids": [
				["62990", "1.80000000"],
				["62980", "2.50000000"],
				["62970", "3.30000000"]
			],
			"ts": "1800000002100"
		}
	]
}`,
		// 06 update ok
		`{
	"id": "92591a51-c76e-4e49-90cf-a742291a126a",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "update",
	"data": [
		{
			"asks": [["63010", "0.90000000"]],
			"bids": [["62990", "2.00000000"]],
			"ts": "1800000002400"
		}
	]
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 snapshot
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62800", Quantity: "1.5"},
				{Price: "62801", Quantity: "0.85"},
				{Price: "62802", Quantity: "2.2"},
				{Price: "62805", Quantity: "0.4"},
				{Price: "62810", Quantity: "3.1"},
			},
			Bids: []events.PriceLevel{
				{Price: "62799", Quantity: "1.2"},
				{Price: "62798", Quantity: "0.6"},
				{Price: "62795", Quantity: "2.4"},
				{Price: "62790", Quantity: "0.95"},
				{Price: "62785", Quantity: "4"},
			},
		},
		{ // after 02 update ok
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62800", Quantity: "1.5"},
				{Price: "62801", Quantity: "0.95"},
				{Price: "62802", Quantity: "2.2"},
				{Price: "62805", Quantity: "0.4"},
				{Price: "62810", Quantity: "3.1"},
			},
			Bids: []events.PriceLevel{
				{Price: "62799", Quantity: "1.2"},
				{Price: "62798", Quantity: "0.6"},
				{Price: "62795", Quantity: "2.6"},
				{Price: "62790", Quantity: "0.95"},
				{Price: "62785", Quantity: "4"},
			},
		},
		{ // after 03 update gap (reset)
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks:       []events.PriceLevel{},
			Bids:       []events.PriceLevel{},
		},
		{ // after 05 snapshot resync
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks: []events.PriceLevel{
				{Price: "63000", Quantity: "2"},
				{Price: "63010", Quantity: "1.1"},
				{Price: "63020", Quantity: "0.75"},
			},
			Bids: []events.PriceLevel{
				{Price: "62990", Quantity: "1.8"},
				{Price: "62980", Quantity: "2.5"},
				{Price: "62970", Quantity: "3.3"},
			},
		},
		{ // after 06 update ok
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks: []events.PriceLevel{
				{Price: "63000", Quantity: "2"},
				{Price: "63010", Quantity: "0.9"},
				{Price: "63020", Quantity: "0.75"},
			},
			Bids: []events.PriceLevel{
				{Price: "62990", Quantity: "2"},
				{Price: "62980", Quantity: "2.5"},
				{Price: "62970", Quantity: "3.3"},
			},
		},
	},
	WantRejects: []string{"sequence_gap", "awaiting_snapshot"},
	// One command for the episode, not one per rejected event: the second update
	// rejects on the same unresolved gap, and job 2 does not re-ask.
	WantControlCommands: []events.ControlCommand{
		{Action: "snapshot_request", ExchangeID: 8, PairID: 1},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 8, Simulation: 1, Price: "63000", Quantity: "2"},
			{ExchangeID: 8, Simulation: 1, Price: "63010", Quantity: "0.9"},
			{ExchangeID: 8, Simulation: 1, Price: "63020", Quantity: "0.75"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 8, Simulation: 1, Price: "62990", Quantity: "2"},
			{ExchangeID: 8, Simulation: 1, Price: "62980", Quantity: "2.5"},
			{ExchangeID: 8, Simulation: 1, Price: "62970", Quantity: "3.3"},
		},
	},
}

// Ex8StaleDuplicate — a replayed update and a backwards snapshot are both stale; neither advances lastSeq, so 05 still lands.
var Ex8StaleDuplicate = Scenario{
	ExchangeID: 8,
	PairID:     1,
	Sources: []string{
		// 01 snapshot
		`{
	"id": "ae06523b-8215-4b8d-a106-5df7ff57c5d7",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "snapshot",
	"data": [
		{
			"asks": [
				["63000", "2.00000000"],
				["63010", "1.10000000"],
				["63020", "0.75000000"]
			],
			"bids": [
				["62990", "1.80000000"],
				["62980", "2.50000000"],
				["62970", "3.30000000"]
			],
			"ts": "1800000000300"
		}
	]
}`,
		// 02 update ok
		`{
	"id": "c9ad6f89-d645-407a-9bc3-579ca11e1a17",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "update",
	"data": [
		{
			"asks": [["63000", "1.75000000"]],
			"bids": [["62980", "2.70000000"]],
			"ts": "1800000000600"
		}
	]
}`,
		// 03 update replay duplicate
		`{
	"id": "4be9bf5b-b95a-4c9f-a9ae-32ba1da73240",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "update",
	"data": [
		{
			"asks": [["63000", "1.75000000"]],
			"bids": [["62980", "2.70000000"]],
			"ts": "1800000000600"
		}
	]
}`,
		// 04 snapshot out of order
		`{
	"id": "b0bb7997-4b6f-4660-adcc-681a01cc0fab",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "snapshot",
	"data": [
		{
			"asks": [["69999", "9.99999999"]],
			"bids": [["10000", "9.99999999"]],
			"ts": "1800000000150"
		}
	]
}`,
		// 05 snapshot ok
		`{
	"id": "929746b5-0fe9-4096-987b-fb9f89b207fd",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "snapshot",
	"data": [
		{
			"asks": [
				["63005", "1.25000000"],
				["63015", "2.40000000"],
				["63025", "0.60000000"]
			],
			"bids": [
				["62995", "1.90000000"],
				["62985", "2.20000000"],
				["62975", "4.10000000"]
			],
			"ts": "1800000000900"
		}
	]
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 snapshot
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "63000", Quantity: "2"},
				{Price: "63010", Quantity: "1.1"},
				{Price: "63020", Quantity: "0.75"},
			},
			Bids: []events.PriceLevel{
				{Price: "62990", Quantity: "1.8"},
				{Price: "62980", Quantity: "2.5"},
				{Price: "62970", Quantity: "3.3"},
			},
		},
		{ // after 02 update ok
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "63000", Quantity: "1.75"},
				{Price: "63010", Quantity: "1.1"},
				{Price: "63020", Quantity: "0.75"},
			},
			Bids: []events.PriceLevel{
				{Price: "62990", Quantity: "1.8"},
				{Price: "62980", Quantity: "2.7"},
				{Price: "62970", Quantity: "3.3"},
			},
		},
		{ // after 05 snapshot ok
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "63005", Quantity: "1.25"},
				{Price: "63015", Quantity: "2.4"},
				{Price: "63025", Quantity: "0.6"},
			},
			Bids: []events.PriceLevel{
				{Price: "62995", Quantity: "1.9"},
				{Price: "62985", Quantity: "2.2"},
				{Price: "62975", Quantity: "4.1"},
			},
		},
	},
	WantRejects: []string{"stale_or_duplicate", "stale_or_duplicate"},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 8, Simulation: 1, Price: "63005", Quantity: "1.25"},
			{ExchangeID: 8, Simulation: 1, Price: "63015", Quantity: "2.4"},
			{ExchangeID: 8, Simulation: 1, Price: "63025", Quantity: "0.6"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 8, Simulation: 1, Price: "62995", Quantity: "1.9"},
			{ExchangeID: 8, Simulation: 1, Price: "62985", Quantity: "2.2"},
			{ExchangeID: 8, Simulation: 1, Price: "62975", Quantity: "4.1"},
		},
	},
}

// Ex8PrecisionDust — truncation, colliding prices merged by summing, and dust quantities that truncate to a delete.
var Ex8PrecisionDust = Scenario{
	ExchangeID: 8,
	PairID:     1,
	Sources: []string{
		// 01 snapshot
		`{
	"id": "af83e9e8-c221-428b-aff5-0987e895280e",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "snapshot",
	"data": [
		{
			"asks": [
				["62900.1234", "3.1234567891"],
				["62900.1289", "2.0000000099"],
				["62901.55", "1.50000000"],
				["62902.999", "4.00000000"],
				["62905.10", "0.0000000090"],
				["62906.1234", "0.0000000060"],
				["62906.1299", "0.0000000060"]
			],
			"bids": [
				["62899.0567", "5.9876543219"],
				["62899.0512", "1.00000000"],
				["62898.9999", "7.25000000"],
				["62897.50", "2.10000000"],
				["62895.4444", "9.00000000"]
			],
			"ts": "1800000000300"
		}
	]
}`,
		// 02 update dust delete
		`{
	"id": "df2d7c59-c8ce-4eb8-bbf2-8272fcfa680c",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "update",
	"data": [
		{
			"asks": [
				["62901.55", "0.0000000041"],
				["62910.25", "6.00000000"],
				["62910.2599", "1.50000000"]
			],
			"bids": [["62897.50", "0.0000000099"]],
			"ts": "1800000000600"
		}
	]
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01 snapshot
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62900.12", Quantity: "5.12345679"},
				{Price: "62901.55", Quantity: "1.5"},
				{Price: "62902.99", Quantity: "4"},
				{Price: "62906.12", Quantity: "0.00000001"},
			},
			Bids: []events.PriceLevel{
				{Price: "62899.05", Quantity: "6.98765432"},
				{Price: "62898.99", Quantity: "7.25"},
				{Price: "62897.5", Quantity: "2.1"},
				{Price: "62895.44", Quantity: "9"},
			},
		},
		{ // after 02 update dust delete
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62900.12", Quantity: "5.12345679"},
				{Price: "62902.99", Quantity: "4"},
				{Price: "62906.12", Quantity: "0.00000001"},
				{Price: "62910.25", Quantity: "7.5"},
			},
			Bids: []events.PriceLevel{
				{Price: "62899.05", Quantity: "6.98765432"},
				{Price: "62898.99", Quantity: "7.25"},
				{Price: "62895.44", Quantity: "9"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 8, Simulation: 1, Price: "62900.12", Quantity: "5.12345679"},
			{ExchangeID: 8, Simulation: 1, Price: "62902.99", Quantity: "4"},
			{ExchangeID: 8, Simulation: 1, Price: "62906.12", Quantity: "0.00000001"},
			{ExchangeID: 8, Simulation: 1, Price: "62910.25", Quantity: "7.5"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 8, Simulation: 1, Price: "62899.05", Quantity: "6.98765432"},
			{ExchangeID: 8, Simulation: 1, Price: "62898.99", Quantity: "7.25"},
			{ExchangeID: 8, Simulation: 1, Price: "62895.44", Quantity: "9"},
		},
	},
}

// Ex8NoiseFrames — non-book frames are dropped, not dead-lettered, and do not disturb sequence tracking.
var Ex8NoiseFrames = Scenario{
	ExchangeID: 8,
	PairID:     1,
	Sources: []string{
		// 01 subscribe ack
		`{
	"id": "a501c5d0-189c-4c37-9421-b0fd9e52d3cd",
	"simulation": 1,
	"event": "subscribe",
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"connId": "a4d3ae55"
}`,
		// 02 error frame
		`{
	"id": "644f3769-c731-47d4-a32f-67540946eeb8",
	"simulation": 1,
	"event": "error",
	"code": "60012",
	"msg": "Invalid request: unknown channel",
	"connId": "a4d3ae55"
}`,
		// 03 snapshot
		`{
	"id": "62446b18-a0eb-4024-861b-75a97faa89a8",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "snapshot",
	"data": [
		{
			"asks": [
				["62950", "1.20000000"],
				["62955", "0.88000000"],
				["62960", "2.40000000"]
			],
			"bids": [
				["62945", "1.50000000"],
				["62940", "3.10000000"],
				["62935", "0.90000000"]
			],
			"ts": "1800000000300"
		}
	]
}`,
		// 04 unknown action
		`{
	"id": "76c8fa22-92ef-4b5e-a34e-ad8f56f54535",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "partial",
	"data": [
		{
			"asks": [["62950", "9999.00000000"]],
			"bids": [["62945", "9999.00000000"]],
			"ts": "1800000000450"
		}
	]
}`,
		// 05 other channel
		`{
	"id": "82458716-d39f-4bee-9d41-c399d6ae2d5f",
	"simulation": 1,
	"arg": { "channel": "trades", "instId": "BTC-USDT" },
	"data": [
		{
			"instId": "BTC-USDT",
			"tradeId": "778291",
			"px": "62950",
			"sz": "0.01",
			"side": "buy",
			"ts": "1800000000500"
		}
	]
}`,
		// 06 update
		`{
	"id": "72857752-b690-4f18-b050-9f12d2174bd3",
	"simulation": 1,
	"arg": {
		"channel": "books-grouped",
		"instId": "BTC-USDT",
		"grouping": "1"
	},
	"action": "update",
	"data": [
		{
			"asks": [
				["62955", "0"],
				["62970", "0.60000000"]
			],
			"bids": [["62945", "1.65000000"]],
			"ts": "1800000000600"
		}
	]
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 03 snapshot
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62950", Quantity: "1.2"},
				{Price: "62955", Quantity: "0.88"},
				{Price: "62960", Quantity: "2.4"},
			},
			Bids: []events.PriceLevel{
				{Price: "62945", Quantity: "1.5"},
				{Price: "62940", Quantity: "3.1"},
				{Price: "62935", Quantity: "0.9"},
			},
		},
		{ // after 06 update
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62950", Quantity: "1.2"},
				{Price: "62960", Quantity: "2.4"},
				{Price: "62970", Quantity: "0.6"},
			},
			Bids: []events.PriceLevel{
				{Price: "62945", Quantity: "1.65"},
				{Price: "62940", Quantity: "3.1"},
				{Price: "62935", Quantity: "0.9"},
			},
		},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 8, Simulation: 1, Price: "62950", Quantity: "1.2"},
			{ExchangeID: 8, Simulation: 1, Price: "62960", Quantity: "2.4"},
			{ExchangeID: 8, Simulation: 1, Price: "62970", Quantity: "0.6"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 8, Simulation: 1, Price: "62945", Quantity: "1.65"},
			{ExchangeID: 8, Simulation: 1, Price: "62940", Quantity: "3.1"},
			{ExchangeID: 8, Simulation: 1, Price: "62935", Quantity: "0.9"},
		},
	},
}
