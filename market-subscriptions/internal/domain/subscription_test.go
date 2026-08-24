package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPending_NeverReturnsASettledStatus(t *testing.T) {
	// The core invariant: this service writes only pending states, NiFi settles them.
	assert.Equal(t, PendingSubscribe, ActionSubscribe.Pending())
	assert.Equal(t, PendingUnsubscribe, ActionUnsubscribe.Pending())
	for _, a := range []Action{ActionSubscribe, ActionUnsubscribe} {
		assert.NotEqual(t, Subscribed, a.Pending())
		assert.NotEqual(t, Unsubscribed, a.Pending())
	}
}

func TestParseAction(t *testing.T) {
	for _, in := range []string{"subscribe", "unsubscribe"} {
		got, err := ParseAction(in)
		require.NoError(t, err)
		assert.EqualValues(t, in, got)
	}
	for _, in := range []string{"", "SUBSCRIBE", "disable", "drop table"} {
		_, err := ParseAction(in)
		assert.Error(t, err, "must reject %q before it can reach a URL or the status column", in)
	}
}

func TestStatusValuesMatchThePostgresEnum(t *testing.T) {
	// postgres/01_schema.sql defines these four labels; a typo here would be written
	// straight into the column and rejected by the enum at runtime.
	assert.Equal(t, "subscribe", string(Subscribed))
	assert.Equal(t, "unsubscribe", string(Unsubscribed))
	assert.Equal(t, "pending-subscribe", string(PendingSubscribe))
	assert.Equal(t, "pending-unsubscribe", string(PendingUnsubscribe))
}
