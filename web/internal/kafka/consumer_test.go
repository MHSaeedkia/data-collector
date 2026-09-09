// The rest of this package is a thin franz-go adapter with no branching
// logic of its own, which is why it had no tests. shouldPurge is the
// exception: it is a real decision, and it is the one that stops a
// recreated topic from disappearing out of the UI forever.
package kafka

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/twmb/franz-go/pkg/kerr"
)

func newTestConsumer() *Consumer {
	return &Consumer{
		name:     "agg",
		perTopic: map[string]int64{},
		purged:   map[string]time.Time{},
	}
}

// Only UNKNOWN_TOPIC_ID means the cursor is pinned to a dead topic ID.
// Every other fetch error either recovers on its own or is not ours to
// fix, and purging on those would churn the client for nothing.
func TestShouldPurge_OnlyForUnknownTopicID(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"unknown topic id", kerr.UnknownTopicID, true},
		{"unknown topic or partition", kerr.UnknownTopicOrPartition, false},
		{"not leader", kerr.NotLeaderForPartition, false},
		{"offset out of range", kerr.OffsetOutOfRange, false},
		{"plain error", errors.New("connection reset"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestConsumer()
			assert.Equal(t, tc.want, c.shouldPurge("p1-asks", tc.err, now))
		})
	}
}

// The error repeats on every poll, so without the cooldown one recreated
// topic would fire a blocking metadata call several times a second.
func TestShouldPurge_CooldownStopsAPurgeStorm(t *testing.T) {
	c := newTestConsumer()
	start := time.Now()

	require.True(t, c.shouldPurge("p1-asks", kerr.UnknownTopicID, start))

	for _, d := range []time.Duration{0, time.Second, 30 * time.Second, purgeCooldown - time.Millisecond} {
		assert.False(t, c.shouldPurge("p1-asks", kerr.UnknownTopicID, start.Add(d)),
			"a repeat %s after the purge must be ignored", d)
	}
}

// If the first purge did not fix it, we try again rather than giving up —
// the topic could have been recreated a second time while we waited.
func TestShouldPurge_RetriesAfterTheCooldown(t *testing.T) {
	c := newTestConsumer()
	start := time.Now()

	require.True(t, c.shouldPurge("p1-asks", kerr.UnknownTopicID, start))
	assert.True(t, c.shouldPurge("p1-asks", kerr.UnknownTopicID, start.Add(purgeCooldown+time.Second)))
}

// One stuck topic must not silence the purge for the others: after a
// broker rebuild every subscribed topic can have a new ID at once.
func TestShouldPurge_TracksEachTopicSeparately(t *testing.T) {
	c := newTestConsumer()
	now := time.Now()

	assert.True(t, c.shouldPurge("p1-asks", kerr.UnknownTopicID, now))
	assert.True(t, c.shouldPurge("p1-bids", kerr.UnknownTopicID, now))
	assert.True(t, c.shouldPurge("ex8-p1-orderbook-snapshot-flink", kerr.UnknownTopicID, now))
	assert.False(t, c.shouldPurge("p1-asks", kerr.UnknownTopicID, now), "still in cooldown")
}

// "first record from X" is how an operator sees that the purge worked, so
// the topic has to be forgotten when it is purged — otherwise recovery is
// silent and indistinguishable from still being broken.
func TestShouldPurge_ForgetsTheTopicSoRecoveryIsVisible(t *testing.T) {
	c := newTestConsumer()
	c.perTopic["p1-asks"] = 42
	c.perTopic["p1-bids"] = 7

	require.True(t, c.shouldPurge("p1-asks", kerr.UnknownTopicID, time.Now()))

	assert.NotContains(t, c.perTopic, "p1-asks", "the purged topic must announce itself again when it comes back")
	assert.Contains(t, c.perTopic, "p1-bids", "an untouched topic must keep its count")
}

// A rejected error must not consume the topic's cooldown slot, or a real
// UNKNOWN_TOPIC_ID arriving right after would be swallowed.
func TestShouldPurge_UnrelatedErrorDoesNotStartTheCooldown(t *testing.T) {
	c := newTestConsumer()
	now := time.Now()

	require.False(t, c.shouldPurge("p1-asks", errors.New("connection reset"), now))
	assert.True(t, c.shouldPurge("p1-asks", kerr.UnknownTopicID, now))
}
