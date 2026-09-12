package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"orderbook-warmup/internal/domain"
)

func TestGroupByRetention_BatchesTopicsPerValue(t *testing.T) {
	got := groupByRetention([]domain.Topic{
		{Name: "a", RetentionMS: "1000"},
		{Name: "b", RetentionMS: "2000"},
		{Name: "c", RetentionMS: "1000"},
	})

	assert.Equal(t, map[string][]string{"1000": {"a", "c"}, "2000": {"b"}}, got)
}

func TestChunk_SplitsOnSize(t *testing.T) {
	assert.Equal(t,
		[][]string{{"a", "b"}, {"c", "d"}, {"e"}},
		chunk([]string{"a", "b", "c", "d", "e"}, 2))
}

func TestChunk_EmptyInputProducesNoRequests(t *testing.T) {
	assert.Empty(t, chunk(nil, 500))
}
