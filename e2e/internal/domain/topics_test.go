package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"orderbook-e2e/internal/domain"
)

func TestTopicsForNamesEveryTopicAScenarioTouches(t *testing.T) {
	topics := domain.TopicsFor(domain.Scope{ExchangeID: 8, PairID: 1})

	var names []string
	for _, topic := range topics {
		names = append(names, topic.Name)
	}

	// Stage topics first — see the ordering note on TopicsFor.
	assert.Equal(t, []string{
		"ex8-p1-raw-flink",
		"ex8-p1-type-validated-raw-flink",
		"ex8-p1-rebased-flink",
		"ex8-p1-applied-precision-flink",
		"ex8-p1-orderbook-snapshot-flink",
		"ex8-p1-rejected-flink",
		"ex8-raw",
		"p1-asks",
		"p1-bids",
	}, names)
}

func TestTopicsForAppliesWarmupRetentions(t *testing.T) {
	byName := map[string]int64{}
	for _, topic := range domain.TopicsFor(domain.Scope{ExchangeID: 8, PairID: 1}) {
		byName[topic.Name] = topic.RetentionMS
	}

	assert.Equal(t, int64(3600000), byName["ex8-p1-raw-flink"], "stage topics: 1 hour")
	assert.Equal(t, int64(604800000), byName["ex8-p1-rejected-flink"], "dead-letter: 7 days")
	assert.Equal(t, int64(604800000), byName["ex8-raw"], "raw: 7 days")
	assert.Equal(t, int64(21600000), byName["p1-asks"], "output: 6 hours")
}

func TestTopicsForIsScopedToItsExchangeAndPair(t *testing.T) {
	topics := domain.TopicsFor(domain.Scope{ExchangeID: 2, PairID: 37})

	for _, topic := range topics {
		assert.NotContains(t, topic.Name, "ex8", "no other exchange's topics")
		assert.NotContains(t, topic.Name, "p1-", "no other pair's topics")
	}
	assert.Len(t, topics, 9)
}
