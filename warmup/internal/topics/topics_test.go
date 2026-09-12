package topics

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"orderbook-warmup/internal/config"
	"orderbook-warmup/internal/domain"
)

var retentions = config.Retentions{
	Raw:      "172800000",
	Input:    "3600000",
	Output:   "21600000",
	Rejected: "7200000",
	Control:  "60000",
}

func names(plan []domain.Topic) []string {
	out := make([]string, 0, len(plan))
	for _, t := range plan {
		out = append(out, t.Name)
	}
	return out
}

func TestPlan_CoversEveryTopicFamily(t *testing.T) {
	got := Plan([]domain.Subscription{{PairID: 2, ExchangeID: 1}}, retentions)

	assert.Equal(t, []domain.Topic{
		{Name: "control-plane", RetentionMS: "60000"},
		{Name: "ex1-p2-raw-flink", RetentionMS: "3600000"},
		{Name: "ex1-p2-type-validated-raw-flink", RetentionMS: "3600000"},
		{Name: "ex1-p2-rebased-flink", RetentionMS: "3600000"},
		{Name: "ex1-p2-applied-precision-flink", RetentionMS: "3600000"},
		{Name: "ex1-p2-orderbook-snapshot-flink", RetentionMS: "3600000"},
		{Name: "ex1-p2-rejected-flink", RetentionMS: "7200000"},
		{Name: "ex1-raw", RetentionMS: "172800000"},
		{Name: "p2-asks", RetentionMS: "21600000"},
		{Name: "p2-asks-merged", RetentionMS: "21600000"},
		{Name: "p2-asks-adjusted", RetentionMS: "21600000"},
		{Name: "p2-bids", RetentionMS: "21600000"},
		{Name: "p2-bids-merged", RetentionMS: "21600000"},
		{Name: "p2-bids-adjusted", RetentionMS: "21600000"},
	}, got)
}

// Every normalizer source reads from `latest`, so a stage topic that does not
// exist when its job starts loses whatever is produced meanwhile.
func TestPlan_PutsStagesBeforeOutputs(t *testing.T) {
	got := names(Plan([]domain.Subscription{{PairID: 2, ExchangeID: 1}}, retentions))

	assert.Less(t, slices.Index(got, "ex1-p2-raw-flink"), slices.Index(got, "p2-asks"))
}

func TestPlan_DeduplicatesSharedTopics(t *testing.T) {
	subs := []domain.Subscription{
		{PairID: 2, ExchangeID: 1},
		{PairID: 3, ExchangeID: 1},
		{PairID: 2, ExchangeID: 6},
		// exchange_markets is unique on (exchange_id, market), not on
		// (exchange_id, market_id): the same pair can appear twice under two
		// symbol spellings on one exchange.
		{PairID: 2, ExchangeID: 1},
	}

	got := names(Plan(subs, retentions))

	seen := map[string]int{}
	for _, name := range got {
		seen[name]++
	}
	for name, n := range seen {
		assert.Equalf(t, 1, n, "topic %s planned %d times; a duplicate in one create batch is rejected by the broker", name, n)
	}
	assert.Equal(t, 1, seen["ex1-raw"])
	assert.Equal(t, 1, seen["ex6-raw"])
	assert.Equal(t, 1, seen["p2-asks"])
}

func TestPlan_SizeMatchesTheShellScriptItReplaced(t *testing.T) {
	var subs []domain.Subscription
	for ex := int64(1); ex <= 9; ex++ {
		for pair := int64(1); pair <= 50; pair++ {
			subs = append(subs, domain.Subscription{PairID: pair, ExchangeID: ex})
		}
	}

	// 1 control + 450*(5 stages + 1 rejected) + 9 raw + 50*6 outputs
	assert.Len(t, Plan(subs, retentions), 1+450*6+9+50*6)
}

func TestDiff_CreatesMissingAndAltersChangedRetention(t *testing.T) {
	desired := []domain.Topic{
		{Name: "a", RetentionMS: "1000"},
		{Name: "b", RetentionMS: "1000"},
		{Name: "c", RetentionMS: "2000"},
	}
	existing := map[string]string{
		"b": "1000", // unchanged
		"c": "9000", // retention changed in .env
	}

	create, alter := Diff(desired, existing)

	assert.Equal(t, []domain.Topic{{Name: "a", RetentionMS: "1000"}}, create)
	assert.Equal(t, []domain.Topic{{Name: "c", RetentionMS: "2000"}}, alter)
}

// A topic whose retention.ms the broker does not report is left alone rather
// than altered on every single run.
func TestDiff_IgnoresUnknownCurrentRetention(t *testing.T) {
	create, alter := Diff([]domain.Topic{{Name: "a", RetentionMS: "1000"}}, map[string]string{"a": ""})

	assert.Empty(t, create)
	assert.Empty(t, alter)
}
