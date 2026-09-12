// Scenarios for ex8/okx on the `books` channel (`wss://ws.okx.com:8443/ws/v5/public`), which
// replaced `books-grouped` on 2026-09-05. Every WS frame carries a real chained counter: `seqId`
// with `prevSeqId` naming its predecessor, so the parser stamps a DYNAMIC jump of
// `seqId - prevSeqId` and job 2's `seq == lastSeq + jump` reduces to `prevSeqId == lastSeq`.
// `ts` is the event time only — it no longer sequences anything, which is why the ts values below
// still step by 300 while the seqId steps do not.
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
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "update",
	"data": [
		{
			"asks": [
				["62771", "0.29045069", "0", "1"],
				["62775", "1.05000000", "0", "1"]
			],
			"bids": [["62769", "0.55175335", "0", "1"]],
			"ts": "1800000000300",
			"checksum": 0,
			"seqId": 4429784547,
			"prevSeqId": 4429784540
		}
	]
}`,
		// 02 snapshot
		`{
	"id": "60a0a661-a7db-482c-96f7-d670cdfa5bd3",
	"simulation": 1,
	"arg": {
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "snapshot",
	"data": [
		{
			"asks": [
				["62770", "2.21924167", "0", "1"],
				["62771", "0.17447383", "0", "1"],
				["62772", "0.19067482", "0", "1"],
				["62775", "1.05000000", "0", "1"],
				["62780", "0.33476925", "0", "1"]
			],
			"bids": [
				["62769", "0.50795335", "0", "1"],
				["62768", "0.02744953", "0", "1"],
				["62767", "0.20630833", "0", "1"],
				["62765", "0.90000000", "0", "1"],
				["62760", "1.31062803", "0", "1"]
			],
			"ts": "1800000000600",
			"checksum": 0,
			"seqId": 4429784550,
			"prevSeqId": -1
		}
	]
}`,
		// 03 update
		`{
	"id": "b87c615c-dc81-428d-8227-a00fa614e7c6",
	"simulation": 1,
	"arg": {
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "update",
	"data": [
		{
			"asks": [
				["62771", "0.29045069", "0", "1"],
				["62772", "0", "0", "0"],
				["62790", "0.40000000", "0", "1"]
			],
			"bids": [
				["62769", "0.55175335", "0", "1"],
				["62758", "1.10000000", "0", "1"]
			],
			"ts": "1800000000900",
			"checksum": 0,
			"seqId": 4429784557,
			"prevSeqId": 4429784550
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
		{Action: "snapshot_request", Reason: "no_baseline", ExchangeID: 8, PairID: 1, Simulation: 1},
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
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "snapshot",
	"data": [
		{
			"asks": [
				["62800", "1.50000000", "0", "1"],
				["62801", "0.85000000", "0", "1"],
				["62802", "2.20000000", "0", "1"],
				["62805", "0.40000000", "0", "1"],
				["62810", "3.10000000", "0", "1"]
			],
			"bids": [
				["62799", "1.20000000", "0", "1"],
				["62798", "0.60000000", "0", "1"],
				["62795", "2.40000000", "0", "1"],
				["62790", "0.95000000", "0", "1"],
				["62785", "4.00000000", "0", "1"]
			],
			"ts": "1800000000300",
			"checksum": 0,
			"seqId": 4429784547,
			"prevSeqId": -1
		}
	]
}`,
		// 02 update modify add delete
		`{
	"id": "23d6e762-3672-463e-a115-22c3b6c14bcd",
	"simulation": 1,
	"arg": {
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "update",
	"data": [
		{
			"asks": [
				["62800", "1.00000000", "0", "1"],
				["62802", "0", "0", "0"],
				["62807", "0.70000000", "0", "1"]
			],
			"bids": [["62799", "1.45000000", "0", "1"]],
			"ts": "1800000000600",
			"checksum": 0,
			"seqId": 4429784551,
			"prevSeqId": 4429784547
		}
	]
}`,
		// 03 update delete bid
		`{
	"id": "37a564f5-221a-495d-baf1-bd74671b193c",
	"simulation": 1,
	"arg": {
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "update",
	"data": [
		{
			"asks": [["62803", "0.33000000", "0", "1"]],
			"bids": [
				["62798", "0", "0", "0"],
				["62780", "5.50000000", "0", "1"]
			],
			"ts": "1800000000900",
			"checksum": 0,
			"seqId": 4429784558,
			"prevSeqId": 4429784551
		}
	]
}`,
		// 04 update asks only
		`{
	"id": "936124ea-e1a8-4b0f-a8d6-c4fc7a0ddc37",
	"simulation": 1,
	"arg": {
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "update",
	"data": [
		{
			"asks": [
				["62800", "0.90000000", "0", "1"],
				["62810", "0", "0", "0"]
			],
			"ts": "1800000001200",
			"checksum": 0,
			"seqId": 4429784560,
			"prevSeqId": 4429784558
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
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "snapshot",
	"data": [
		{
			"asks": [
				["62800", "1.50000000", "0", "1"],
				["62801", "0.85000000", "0", "1"],
				["62802", "2.20000000", "0", "1"],
				["62805", "0.40000000", "0", "1"],
				["62810", "3.10000000", "0", "1"]
			],
			"bids": [
				["62799", "1.20000000", "0", "1"],
				["62798", "0.60000000", "0", "1"],
				["62795", "2.40000000", "0", "1"],
				["62790", "0.95000000", "0", "1"],
				["62785", "4.00000000", "0", "1"]
			],
			"ts": "1800000000300",
			"checksum": 0,
			"seqId": 4429784547,
			"prevSeqId": -1
		}
	]
}`,
		// 02 update ok
		`{
	"id": "9aefe424-fc12-4b8d-a311-1fe768ff7cbe",
	"simulation": 1,
	"arg": {
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "update",
	"data": [
		{
			"asks": [["62801", "0.95000000", "0", "1"]],
			"bids": [["62795", "2.60000000", "0", "1"]],
			"ts": "1800000000600",
			"checksum": 0,
			"seqId": 4429784551,
			"prevSeqId": 4429784547
		}
	]
}`,
		// 03 update gap
		`{
	"id": "9c1d91ad-24da-4a88-8687-27c50aa53ba9",
	"simulation": 1,
	"arg": {
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "update",
	"data": [
		{
			"asks": [["62805", "0", "0", "0"]],
			"bids": [["62790", "1.10000000", "0", "1"]],
			"ts": "1800000001500",
			"checksum": 0,
			"seqId": 4429784563,
			"prevSeqId": 4429784559
		}
	]
}`,
		// 04 update awaiting snapshot
		`{
	"id": "1dd810e4-7581-4173-aa1e-c19c3ee3d610",
	"simulation": 1,
	"arg": {
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "update",
	"data": [
		{
			"asks": [["62807", "0.50000000", "0", "1"]],
			"bids": [["62785", "3.80000000", "0", "1"]],
			"ts": "1800000001800",
			"checksum": 0,
			"seqId": 4429784570,
			"prevSeqId": 4429784563
		}
	]
}`,
		// 05 snapshot resync
		`{
	"id": "9e962142-8cb6-4326-bfc8-cc2986996abb",
	"simulation": 1,
	"arg": {
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "snapshot",
	"data": [
		{
			"asks": [
				["63000", "2.00000000", "0", "1"],
				["63010", "1.10000000", "0", "1"],
				["63020", "0.75000000", "0", "1"]
			],
			"bids": [
				["62990", "1.80000000", "0", "1"],
				["62980", "2.50000000", "0", "1"],
				["62970", "3.30000000", "0", "1"]
			],
			"ts": "1800000002100",
			"checksum": 0,
			"seqId": 4429784580,
			"prevSeqId": -1
		}
	]
}`,
		// 06 update ok
		`{
	"id": "92591a51-c76e-4e49-90cf-a742291a126a",
	"simulation": 1,
	"arg": {
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "update",
	"data": [
		{
			"asks": [["63010", "0.90000000", "0", "1"]],
			"bids": [["62990", "2.00000000", "0", "1"]],
			"ts": "1800000002400",
			"checksum": 0,
			"seqId": 4429784585,
			"prevSeqId": 4429784580
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
		{Action: "snapshot_request", Reason: "sequence_gap", ExchangeID: 8, PairID: 1, Simulation: 1},
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
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "snapshot",
	"data": [
		{
			"asks": [
				["63000", "2.00000000", "0", "1"],
				["63010", "1.10000000", "0", "1"],
				["63020", "0.75000000", "0", "1"]
			],
			"bids": [
				["62990", "1.80000000", "0", "1"],
				["62980", "2.50000000", "0", "1"],
				["62970", "3.30000000", "0", "1"]
			],
			"ts": "1800000000300",
			"checksum": 0,
			"seqId": 4429784547,
			"prevSeqId": -1
		}
	]
}`,
		// 02 update ok
		`{
	"id": "c9ad6f89-d645-407a-9bc3-579ca11e1a17",
	"simulation": 1,
	"arg": {
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "update",
	"data": [
		{
			"asks": [["63000", "1.75000000", "0", "1"]],
			"bids": [["62980", "2.70000000", "0", "1"]],
			"ts": "1800000000600",
			"checksum": 0,
			"seqId": 4429784551,
			"prevSeqId": 4429784547
		}
	]
}`,
		// 03 update replay duplicate
		`{
	"id": "4be9bf5b-b95a-4c9f-a9ae-32ba1da73240",
	"simulation": 1,
	"arg": {
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "update",
	"data": [
		{
			"asks": [["63000", "1.75000000", "0", "1"]],
			"bids": [["62980", "2.70000000", "0", "1"]],
			"ts": "1800000000600",
			"checksum": 0,
			"seqId": 4429784551,
			"prevSeqId": 4429784547
		}
	]
}`,
		// 04 snapshot out of order
		`{
	"id": "b0bb7997-4b6f-4660-adcc-681a01cc0fab",
	"simulation": 1,
	"arg": {
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "snapshot",
	"data": [
		{
			"asks": [["69999", "9.99999999", "0", "1"]],
			"bids": [["10000", "9.99999999", "0", "1"]],
			"ts": "1800000000150",
			"checksum": 0,
			"seqId": 4429784549,
			"prevSeqId": -1
		}
	]
}`,
		// 05 snapshot ok
		`{
	"id": "929746b5-0fe9-4096-987b-fb9f89b207fd",
	"simulation": 1,
	"arg": {
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "snapshot",
	"data": [
		{
			"asks": [
				["63005", "1.25000000", "0", "1"],
				["63015", "2.40000000", "0", "1"],
				["63025", "0.60000000", "0", "1"]
			],
			"bids": [
				["62995", "1.90000000", "0", "1"],
				["62985", "2.20000000", "0", "1"],
				["62975", "4.10000000", "0", "1"]
			],
			"ts": "1800000000900",
			"checksum": 0,
			"seqId": 4429784560,
			"prevSeqId": -1
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
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "snapshot",
	"data": [
		{
			"asks": [
				["62900.1234", "3.1234567891", "0", "1"],
				["62900.1289", "2.0000000099", "0", "1"],
				["62901.55", "1.50000000", "0", "1"],
				["62902.999", "4.00000000", "0", "1"],
				["62905.10", "0.0000000090", "0", "1"],
				["62906.1234", "0.0000000060", "0", "1"],
				["62906.1299", "0.0000000060", "0", "1"]
			],
			"bids": [
				["62899.0567", "5.9876543219", "0", "1"],
				["62899.0512", "1.00000000", "0", "1"],
				["62898.9999", "7.25000000", "0", "1"],
				["62897.50", "2.10000000", "0", "1"],
				["62895.4444", "9.00000000", "0", "1"]
			],
			"ts": "1800000000300",
			"checksum": 0,
			"seqId": 4429784547,
			"prevSeqId": -1
		}
	]
}`,
		// 02 update dust delete
		`{
	"id": "df2d7c59-c8ce-4eb8-bbf2-8272fcfa680c",
	"simulation": 1,
	"arg": {
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "update",
	"data": [
		{
			"asks": [
				["62901.55", "0.0000000041", "0", "1"],
				["62910.25", "6.00000000", "0", "1"],
				["62910.2599", "1.50000000", "0", "1"]
			],
			"bids": [["62897.50", "0.0000000099", "0", "1"]],
			"ts": "1800000000600",
			"checksum": 0,
			"seqId": 4429784551,
			"prevSeqId": 4429784547
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
		"channel": "books",
		"instId": "BTC-USDT"
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
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "snapshot",
	"data": [
		{
			"asks": [
				["62950", "1.20000000", "0", "1"],
				["62955", "0.88000000", "0", "1"],
				["62960", "2.40000000", "0", "1"]
			],
			"bids": [
				["62945", "1.50000000", "0", "1"],
				["62940", "3.10000000", "0", "1"],
				["62935", "0.90000000", "0", "1"]
			],
			"ts": "1800000000300",
			"checksum": 0,
			"seqId": 4429784547,
			"prevSeqId": -1
		}
	]
}`,
		// 04 unknown action
		`{
	"id": "76c8fa22-92ef-4b5e-a34e-ad8f56f54535",
	"simulation": 1,
	"arg": {
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "partial",
	"data": [
		{
			"asks": [["62950", "9999.00000000", "0", "1"]],
			"bids": [["62945", "9999.00000000", "0", "1"]],
			"ts": "1800000000450",
			"checksum": 0,
			"seqId": 4429784550,
			"prevSeqId": 4429784547
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
		"channel": "books",
		"instId": "BTC-USDT"
	},
	"action": "update",
	"data": [
		{
			"asks": [
				["62955", "0", "0", "0"],
				["62970", "0.60000000", "0", "1"]
			],
			"bids": [["62945", "1.65000000", "0", "1"]],
			"ts": "1800000000600",
			"checksum": 0,
			"seqId": 4429784551,
			"prevSeqId": 4429784547
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

// Ex8RestSnapshotResync — the SECOND stream on `ex8-raw` (added 2026-09-05): okx's REST depth
// response, which NiFi tags `action: "snapshot"` and stamps with the market as a top-level `pair`.
//
// This is the regression test for the black hole found on the live dev server: job 1 had no branch
// for this shape, so `arg.instId` read null and the WHOLE FRAME was discarded. Job 2 therefore
// never saw a `type == "snapshot"`, `resyncPending` never cleared, and every later update
// dead-lettered as `awaiting_snapshot` until the job restarted — a `snapshot_request` with no path
// back to an accepted snapshot.
//
// 40-ex8-sequence-gap passed the whole time it was broken, because its resync answer is a WS
// snapshot. On a delta feed NiFi answers by REST, so that scenario tested a frame production never
// sends. That is the coverage gap this closes, and it is the same gap ex5 (31) and ex6 (48) had
// already been given scenarios for.
//
// Three ways the REST shape differs from ex5's and ex6's, each of which the sources exercise:
//
//   - there is NO `arg`, and that absence is the discriminator. `data` is an ARRAY on both okx
//     streams and `action` reads "snapshot" on both, so neither can separate them (ex5 splits on
//     `data` being an object, ex6 on the book sitting under `result` — neither works here).
//   - levels are FOUR-element arrays, `[price, qty, "0", orderCount]`, where the WS frame sends
//     two. Only elements 0 and 1 are read.
//   - the body carries `seqId` but no `prevSeqId`, so it cannot be chained. Since the move to the
//     `books` channel this is the SAME counter the WS frames use — source 05's 4428333610 is a real
//     captured value — and it still must NOT seed lastSeq: a snapshot's seqId is not any later
//     update's prevSeqId, because the counter advances between NiFi's fetch and the next WS frame.
//     Adopting it would break source 06's chain check instead of repairing it.
//
// Null-seq is what makes it work. Job 2 orders the body by event time on the resync exemption —
// source 05's `ts` is 50 ms BEHIND source 04's, coming off the REST endpoint's clock rather than
// the WS one — and then `baselinePending` lets source 06 adopt its own `seqId` as the fresh
// baseline unconditionally, so the REST body's counter is never compared against a WS one.
// Source 07 then chains to 06 normally.
//
// A RESUBSCRIBE would avoid the baseline gap entirely (the WS snapshot re-seeds the counter
// exactly); this scenario pins the REST fallback, which is the path that used to be a black hole.
//
// ONE reject episode and ONE control command is the whole assertion: a second command means the
// reset → request → snapshot → gap loop is back.
var Ex8RestSnapshotResync = Scenario{
	ExchangeID: 8,
	PairID:     1,
	Sources: []string{
		// 01 WS snapshot — the baseline
		`{
	"id": "3f7c1a90-5e28-4b6d-9a41-7c05e2d8b361",
	"simulation": 1,
	"arg": { "channel": "books", "instId": "BTC-USDT" },
	"action": "snapshot",
	"data": [
		{
			"asks": [["62800", "1.50000000", "0", "1"], ["62801", "0.85000000", "0", "1"], ["62802", "2.20000000", "0", "1"]],
			"bids": [["62799", "1.20000000", "0", "1"], ["62798", "0.60000000", "0", "1"], ["62795", "2.40000000", "0", "1"]],
			"ts": "1800000000300",
			"checksum": 0,
			"seqId": 4429784547,
			"prevSeqId": -1
		}
	]
}`,
		// 02 WS update chaining to 01 (prevSeqId == the snapshot's seqId) — accepted, re-sizes 62801
		`{
	"id": "8b2d4e05-91af-4c73-8d16-2e6fb0a95c47",
	"simulation": 1,
	"arg": { "channel": "books", "instId": "BTC-USDT" },
	"action": "update",
	"data": [
		{ "asks": [["62801", "0.95000000", "0", "1"]], "bids": [["62795", "2.60000000", "0", "1"]], "ts": "1800000000600", "checksum": 0, "seqId": 4429784551, "prevSeqId": 4429784547 }
	]
}`,
		// 03 WS update whose prevSeqId (…563) is NOT lastSeq (…551) — a gap: reset, dead-letter, ask
		`{
	"id": "d5091c76-2b84-4f30-a9e2-6108c3f7a5db",
	"simulation": 1,
	"arg": { "channel": "books", "instId": "BTC-USDT" },
	"action": "update",
	"data": [
		{ "asks": [["62802", "0", "0", "0"]], "bids": [["62790", "1.10000000", "0", "1"]], "ts": "1800000001500", "checksum": 0, "seqId": 4429784567, "prevSeqId": 4429784563 }
	]
}`,
		// 04 WS update while the request is outstanding — awaiting_snapshot, and NO second command
		`{
	"id": "6e83b501-c47d-4a29-b0f5-93da12c8e764",
	"simulation": 1,
	"arg": { "channel": "books", "instId": "BTC-USDT" },
	"action": "update",
	"data": [
		{ "asks": [["62807", "0.50000000", "0", "1"]], "bids": [["62785", "3.80000000", "0", "1"]], "ts": "1800000001800", "checksum": 0, "seqId": 4429784572, "prevSeqId": 4429784567 }
	]
}`,
		// 05 REST snapshot — the resync answer, in the OTHER wire shape. No `arg` at all; the
		// market arrives as the injected `pair`, and there is no `prevSeqId` to chain with — so its
		// `seqId`, though now on the same counter as the WS frames, must be ignored rather than
		// adopted.
		//
		// Its `ts` is 50 ms BEHIND source 04 — the endpoint's clock, not the WS one. The parser
		// leaves the sequence id null, so it is never compared against a WS counter and job 2
		// accepts it on the resync exemption.
		`{
	"id": "a304f0d3-8062-48ab-b971-fc638d9f3f79",
	"simulation": 1,
	"code": "0",
	"msg": "",
	"data": [
		{
			"asks": [["63000", "2.00000000", "0", "3"], ["63010", "1.10000000", "0", "1"], ["63020", "0.75000000", "0", "2"]],
			"bids": [["62990", "1.80000000", "0", "4"], ["62980", "2.50000000", "0", "1"], ["62970", "3.30000000", "0", "2"]],
			"ts": "1800000001750",
			"seqId": 4428333610
		}
	],
	"pair": "BTC-USDT",
	"action": "snapshot"
}`,
		// 06 first WS update after the REST body. Its prevSeqId (…580) chains to nothing job 2 has
		// seen — which is the point: `baselinePending` adopts its seqId as the fresh baseline
		// unconditionally, so the REST body's counter and the WS one never meet.
		`{
	"id": "b71e5f28-0a4c-4d96-8e37-51c2094fab6d",
	"simulation": 1,
	"arg": { "channel": "books", "instId": "BTC-USDT" },
	"action": "update",
	"data": [
		{ "asks": [["63010", "0.90000000", "0", "1"]], "bids": [["62990", "2.00000000", "0", "1"]], "ts": "1800000002610", "checksum": 0, "seqId": 4429784585, "prevSeqId": 4429784580 }
	]
}`,
		// 07 WS update chaining to 06 — ordinary contiguity has resumed on the WS counter, and
		// the qty-"0" delete still removes a level.
		`{
	"id": "0c96a483-7d15-42be-9b38-4a71e5b026cd",
	"simulation": 1,
	"arg": { "channel": "books", "instId": "BTC-USDT" },
	"action": "update",
	"data": [
		{ "asks": [["63020", "0", "0", "0"]], "bids": [["62970", "3.50000000", "0", "1"]], "ts": "1800000002910", "checksum": 0, "seqId": 4429784590, "prevSeqId": 4429784585 }
	]
}`,
	},
	WantSnapshots: []events.OrderbookSnapshot{
		{ // after 01
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62800", Quantity: "1.5"},
				{Price: "62801", Quantity: "0.85"},
				{Price: "62802", Quantity: "2.2"},
			},
			Bids: []events.PriceLevel{
				{Price: "62799", Quantity: "1.2"},
				{Price: "62798", Quantity: "0.6"},
				{Price: "62795", Quantity: "2.4"},
			},
		},
		{ // after 02 — +300 accepted
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:00Z",
			Asks: []events.PriceLevel{
				{Price: "62800", Quantity: "1.5"},
				{Price: "62801", Quantity: "0.95"},
				{Price: "62802", Quantity: "2.2"},
			},
			Bids: []events.PriceLevel{
				{Price: "62799", Quantity: "1.2"},
				{Price: "62798", Quantity: "0.6"},
				{Price: "62795", Quantity: "2.6"},
			},
		},
		{ // after 03 — the gap's reset empties the book
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
			Asks:       []events.PriceLevel{},
			Bids:       []events.PriceLevel{},
		},
		{ // after 05 — the REST body restores it. This is the snapshot that never existed while
			// the parser had no REST branch: the frame was dropped and nothing came out at all.
			// The event time steps BACK to 08:00:01Z's earlier millis because it is the other clock.
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:01Z",
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
		{ // after 06 — the baseline re-anchors on the WS clock
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
		{ // after 07 — +300 accepted, 63020 deleted
			ExchangeID: 8,
			PairID:     1,
			Simulation: 1,
			EventTime:  "2027-01-15T08:00:02Z",
			Asks: []events.PriceLevel{
				{Price: "63000", Quantity: "2"},
				{Price: "63010", Quantity: "0.9"},
			},
			Bids: []events.PriceLevel{
				{Price: "62990", Quantity: "2"},
				{Price: "62980", Quantity: "2.5"},
				{Price: "62970", Quantity: "3.5"},
			},
		},
	},
	WantRejects: []string{"sequence_gap", "awaiting_snapshot"},
	// One command for the episode, not one per rejected event. A SECOND command here would mean
	// the resync answer was thrown away again, which is exactly the bug.
	WantControlCommands: []events.ControlCommand{
		{Action: "snapshot_request", Reason: "sequence_gap", ExchangeID: 8, PairID: 1, Simulation: 1},
	},
	WantAggregated: &AggregatedBook{
		Asks: []events.AggregatedLevel{
			{ExchangeID: 8, Simulation: 1, Price: "63000", Quantity: "2"},
			{ExchangeID: 8, Simulation: 1, Price: "63010", Quantity: "0.9"},
		},
		Bids: []events.AggregatedLevel{
			{ExchangeID: 8, Simulation: 1, Price: "62990", Quantity: "2"},
			{ExchangeID: 8, Simulation: 1, Price: "62980", Quantity: "2.5"},
			{ExchangeID: 8, Simulation: 1, Price: "62970", Quantity: "3.5"},
		},
	},
}
