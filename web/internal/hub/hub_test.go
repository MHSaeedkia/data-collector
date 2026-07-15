package hub

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"orderbook-web/internal/domain"
)

// fakeConn records what was written to it and can be made to fail writes,
// standing in for a real *websocket.Conn.
type fakeConn struct {
	writes  []any
	failErr error
	closed  bool
}

func (f *fakeConn) WriteJSON(v any) error {
	if f.failErr != nil {
		return f.failErr
	}
	f.writes = append(f.writes, v)
	return nil
}

func (f *fakeConn) ReadMessage() (int, []byte, error) { return 0, nil, nil }

func (f *fakeConn) Close() error {
	f.closed = true
	return nil
}

func TestAdd_SendsSnapshotOfExistingBooks(t *testing.T) {
	h := New()
	h.latest["p1-asks"] = domain.Book{PairID: 1, Side: "asks"}
	c := &fakeConn{}

	h.add(c)

	require.Len(t, c.writes, 1)
	snap, ok := c.writes[0].(domain.WSSnapshot)
	require.True(t, ok)
	assert.Equal(t, "snapshot", snap.Type)
	require.Len(t, snap.Books, 1)
	assert.Equal(t, 1, snap.Books[0].PairID)
}

func TestPublish_BroadcastsToAllClients(t *testing.T) {
	h := New()
	c1, c2 := &fakeConn{}, &fakeConn{}
	h.add(c1)
	h.add(c2)

	h.Publish("p1-asks", domain.Book{PairID: 1, Side: "asks"})

	for _, c := range []*fakeConn{c1, c2} {
		require.Len(t, c.writes, 2) // snapshot on add + the update
		upd, ok := c.writes[1].(domain.WSUpdate)
		require.True(t, ok)
		assert.Equal(t, "update", upd.Type)
		assert.Equal(t, 1, upd.Book.PairID)
	}
	assert.Equal(t, domain.Book{PairID: 1, Side: "asks"}, h.latest["p1-asks"])
}

func TestPublish_DropsClientWhoseWriteFails(t *testing.T) {
	h := New()
	bad := &fakeConn{failErr: errors.New("broken pipe")}
	good := &fakeConn{}
	h.add(bad)
	h.add(good)

	h.Publish("p1-asks", domain.Book{PairID: 1})

	assert.True(t, bad.closed, "failing client should be closed")
	_, stillRegistered := h.clients[bad]
	assert.False(t, stillRegistered, "failing client should be removed from the client set")
	assert.True(t, good.closed == false)
}

func TestRemove_ClosesAndUnregistersConn(t *testing.T) {
	h := New()
	c := &fakeConn{}
	h.add(c)

	h.remove(c)

	assert.True(t, c.closed)
	_, stillRegistered := h.clients[c]
	assert.False(t, stillRegistered)
}
