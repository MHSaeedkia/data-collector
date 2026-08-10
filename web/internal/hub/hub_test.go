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

func aggregatedBook(pairID int, side string) domain.Book {
	return domain.Book{PairID: pairID, Side: side}
}

func exchangeBook(pairID, exchangeID int, side string) domain.Book {
	return domain.Book{
		PairID:   pairID,
		Side:     side,
		Exchange: &domain.Exchange{ID: exchangeID, Name: "okx"},
	}
}

func TestAdd_SendsCatalog(t *testing.T) {
	h := New()
	h.SetCatalog(domain.Catalog{Markets: []domain.Market{{ID: 1, Base: "BTC", Quote: "USDT"}}})
	c := &fakeConn{}

	h.add(c)

	require.Len(t, c.writes, 1)
	cat, ok := c.writes[0].(domain.WSCatalog)
	require.True(t, ok, "a client's first message is the catalog, not book data")
	assert.Equal(t, "catalog", cat.Type)
	require.Len(t, cat.Markets, 1)
	assert.Equal(t, "BTC", cat.Markets[0].Base)
}

func TestSelect_AnswersWithHeldBooksForThatSelectionOnly(t *testing.T) {
	h := New()
	for _, b := range []domain.Book{
		aggregatedBook(1, "asks"),
		aggregatedBook(1, "bids"),
		aggregatedBook(2, "asks"),
		exchangeBook(1, 8, "asks"),
	} {
		h.latest[b.Key()] = b
	}
	c := &fakeConn{}
	cl := h.add(c)

	h.selectBooks(cl, domain.Selection{PairID: 1, ExchangeID: 0})

	snap, ok := c.writes[1].(domain.WSSnapshot)
	require.True(t, ok)
	assert.Equal(t, "snapshot", snap.Type)
	assert.Len(t, snap.Books, 2, "only the aggregated books of pair 1")
	for _, b := range snap.Books {
		assert.Equal(t, 1, b.PairID)
		assert.Nil(t, b.Exchange)
	}
}

func TestSelect_ExchangeSelectionExcludesTheAggregatedBook(t *testing.T) {
	h := New()
	for _, b := range []domain.Book{
		aggregatedBook(1, "asks"),
		exchangeBook(1, 8, "asks"),
		exchangeBook(1, 6, "asks"),
	} {
		h.latest[b.Key()] = b
	}
	c := &fakeConn{}
	cl := h.add(c)

	h.selectBooks(cl, domain.Selection{PairID: 1, ExchangeID: 8})

	snap := c.writes[1].(domain.WSSnapshot)
	require.Len(t, snap.Books, 1)
	assert.Equal(t, 8, snap.Books[0].ExchangeID())
}

func TestPublish_ReachesOnlyClientsWatchingThatBook(t *testing.T) {
	h := New()
	watching, other, unselected := &fakeConn{}, &fakeConn{}, &fakeConn{}
	h.selectBooks(h.add(watching), domain.Selection{PairID: 1, ExchangeID: 8})
	h.selectBooks(h.add(other), domain.Selection{PairID: 1, ExchangeID: 0})
	h.add(unselected) // connected but has not chosen yet

	b := exchangeBook(1, 8, "asks")
	h.Publish(b)

	require.Len(t, watching.writes, 3) // catalog + snapshot + the update
	upd, ok := watching.writes[2].(domain.WSUpdate)
	require.True(t, ok)
	assert.Equal(t, "update", upd.Type)
	assert.Equal(t, 8, upd.Book.ExchangeID())
	assert.Len(t, other.writes, 2, "the aggregated view must not receive a per-exchange book")
	assert.Len(t, unselected.writes, 1, "a client with no selection receives only the catalog")
	assert.Equal(t, b, h.latest[b.Key()])
}

// The aggregated book and a per-exchange book of the same pair and side
// are different books, not the same one overwritten.
func TestPublish_KeysAggregatedAndPerExchangeSeparately(t *testing.T) {
	h := New()
	h.Publish(aggregatedBook(1, "asks"))
	h.Publish(exchangeBook(1, 8, "asks"))

	assert.Len(t, h.latest, 2)
}

func TestPublish_DropsClientWhoseWriteFails(t *testing.T) {
	h := New()
	bad := &fakeConn{failErr: errors.New("broken pipe")}
	good := &fakeConn{}
	sel := domain.Selection{PairID: 1}
	h.selectBooks(h.add(bad), sel)
	h.selectBooks(h.add(good), sel)

	h.Publish(aggregatedBook(1, "asks"))

	assert.True(t, bad.closed, "failing client should be closed")
	assert.Len(t, h.clients, 1, "failing client should be removed from the client set")
	assert.False(t, good.closed)
}

func TestSetCatalog_BroadcastsOnlyWhenItChanged(t *testing.T) {
	h := New()
	c := &fakeConn{}
	h.add(c)
	require.Len(t, c.writes, 1) // the catalog sent on add

	h.SetCatalog(domain.Catalog{Exchanges: []domain.Exchange{{ID: 1, Name: "nobitex"}}})
	require.Len(t, c.writes, 2)

	h.SetCatalog(domain.Catalog{Exchanges: []domain.Exchange{{ID: 1, Name: "nobitex"}}})
	assert.Len(t, c.writes, 2, "an unchanged catalog must not be re-broadcast on every refresh tick")
}

func TestRemove_ClosesAndUnregistersConn(t *testing.T) {
	h := New()
	c := &fakeConn{}
	cl := h.add(c)

	h.remove(cl)

	assert.True(t, c.closed)
	assert.Empty(t, h.clients)
}
