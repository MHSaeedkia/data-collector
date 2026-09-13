package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func levels(n int) []Level {
	out := make([]Level, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, Level{Price: "1", Quantity: "1"})
	}
	return out
}

// The levels nearest the spread are the ones worth keeping, and every
// producer emits asks ascending and bids descending — so the first n are
// the best n on either side.
func TestBook_LimitKeepsTheFirstLevels(t *testing.T) {
	b := Book{Side: "asks", Levels: []Level{
		{Price: "10"}, {Price: "11"}, {Price: "12"}, {Price: "13"},
	}}

	got := b.Limit(2)

	require.Len(t, got.Levels, 2)
	assert.Equal(t, "10", got.Levels[0].Price)
	assert.Equal(t, "11", got.Levels[1].Price)
}

func TestBook_LimitLeavesAShallowerBookAlone(t *testing.T) {
	b := Book{Levels: levels(3)}

	assert.Len(t, b.Limit(25).Levels, 3)
	assert.Len(t, b.Limit(3).Levels, 3)
}

// The hub holds one book per key and every client watching it gets its
// own depth, so limiting must never touch what is stored.
func TestBook_LimitDoesNotChangeTheOriginal(t *testing.T) {
	b := Book{Levels: levels(10)}

	shallow := b.Limit(2)

	assert.Len(t, shallow.Levels, 2)
	assert.Len(t, b.Levels, 10, "the stored book must keep every level")
}

func TestBook_LimitZeroOrLessMeansNoLimit(t *testing.T) {
	b := Book{Levels: levels(10)}

	assert.Len(t, b.Limit(0).Levels, 10)
	assert.Len(t, b.Limit(-1).Levels, 10)
}

func TestValidLevelLimit(t *testing.T) {
	for _, n := range LevelLimits {
		assert.True(t, ValidLevelLimit(n), "%d is offered in the UI", n)
	}
	for _, n := range []int{0, -1, 1, 26, 500} {
		assert.False(t, ValidLevelLimit(n), "%d is not a depth anyone may ask for", n)
	}
}
