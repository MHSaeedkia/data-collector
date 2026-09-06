package hub

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"orderbook-web/internal/domain"
)

// fakeConn records what was written to it, can be made to fail writes,
// and can be made to STALL them — the condition that used to freeze the
// whole hub. It stands in for a real *websocket.Conn.
//
// Every field is mutex-guarded because writes now happen on the client's
// own goroutine, not on the caller's.
type fakeConn struct {
	mu        sync.Mutex
	writes    []any
	deadlines []time.Time
	pings     int
	closed    bool

	// failAfter is how many writes succeed before every later one fails;
	// -1 never fails.
	failAfter int
	// gate, when non-nil, holds every write until it is closed, and
	// parked announces that a write is stuck on it — without that signal a
	// test cannot know the writer is actually blocked before it starts
	// queueing behind it.
	gate   chan struct{}
	parked chan struct{}
}

func newFakeConn() *fakeConn { return &fakeConn{failAfter: -1} }

func (f *fakeConn) WriteJSON(v any) error {
	f.mu.Lock()
	gate, parked := f.gate, f.parked
	f.mu.Unlock()
	if gate != nil {
		if parked != nil {
			select {
			case parked <- struct{}{}:
			default:
			}
		}
		<-gate
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failAfter >= 0 && len(f.writes) >= f.failAfter {
		return errors.New("broken pipe")
	}
	f.writes = append(f.writes, v)
	return nil
}

func (f *fakeConn) SetWriteDeadline(t time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deadlines = append(f.deadlines, t)
	return nil
}

func (f *fakeConn) WriteControl(messageType int, _ []byte, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if messageType == websocket.PingMessage {
		f.pings++
	}
	if f.failAfter >= 0 && len(f.writes) >= f.failAfter {
		return errors.New("broken pipe")
	}
	return nil
}

func (f *fakeConn) ReadMessage() (int, []byte, error) { return 0, nil, nil }

func (f *fakeConn) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeConn) sent() []any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]any(nil), f.writes...)
}

func (f *fakeConn) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func (f *fakeConn) pingCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pings
}

// waitSent blocks until the conn has received n messages and returns them.
func waitSent(t *testing.T, f *fakeConn, n int) []any {
	t.Helper()
	require.Eventually(t, func() bool { return len(f.sent()) >= n }, 2*time.Second, time.Millisecond,
		"expected %d messages, got %d", n, len(f.sent()))
	return f.sent()
}

// clientCount reads the client set under the hub's own lock. The writer
// goroutines can remove a client at any moment, so a bare len() here is a
// race in the test even when the code under test is correct.
func clientCount(h *Hub) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

// settle gives the writer goroutines a moment to deliver anything they
// were going to, so a "must NOT receive" assertion means something.
func settle() { time.Sleep(50 * time.Millisecond) }

// newStalledConn is a conn whose writer parks on its very first message:
// a browser that accepted the websocket and then stopped reading. The
// gate must exist before the client is added, so this is a constructor
// rather than something a test switches on afterwards.
func newStalledConn() *fakeConn {
	f := newFakeConn()
	f.gate = make(chan struct{})
	f.parked = make(chan struct{}, 1)
	return f
}

// waitParked returns once the writer is genuinely stuck, so what a test
// queues next is queued behind a browser that is not reading.
func waitParked(t *testing.T, f *fakeConn) {
	t.Helper()
	f.mu.Lock()
	parked := f.parked
	f.mu.Unlock()
	select {
	case <-parked:
	case <-time.After(2 * time.Second):
		t.Fatal("writer never reached its first write")
	}
}

// resume lets the stalled writer drain — the browser starts reading again.
func (f *fakeConn) resume() {
	f.mu.Lock()
	gate := f.gate
	f.mu.Unlock()
	close(gate)
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

func mergedBook(pairID int, side string) domain.Book {
	return domain.Book{PairID: pairID, Side: side, Merged: true}
}

func TestAdd_SendsCatalog(t *testing.T) {
	h := New()
	h.SetCatalog(domain.Catalog{Markets: []domain.Market{{ID: 1, Base: "BTC", Quote: "USDT"}}})
	c := newFakeConn()

	h.add(c)

	sent := waitSent(t, c, 1)
	cat, ok := sent[0].(domain.WSCatalog)
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
	c := newFakeConn()
	cl := h.add(c)

	h.selectBooks(cl, domain.Selection{PairID: 1, ExchangeID: domain.AggregatedExchangeID})

	sent := waitSent(t, c, 2)
	snap, ok := sent[1].(domain.WSSnapshot)
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
	c := newFakeConn()
	cl := h.add(c)

	h.selectBooks(cl, domain.Selection{PairID: 1, ExchangeID: 8})

	snap := waitSent(t, c, 2)[1].(domain.WSSnapshot)
	require.Len(t, snap.Books, 1)
	assert.Equal(t, 8, snap.Books[0].ExchangeID())
}

func TestPublish_ReachesOnlyClientsWatchingThatBook(t *testing.T) {
	h := New()
	watching, other, unselected := newFakeConn(), newFakeConn(), newFakeConn()
	h.selectBooks(h.add(watching), domain.Selection{PairID: 1, ExchangeID: 8})
	h.selectBooks(h.add(other), domain.Selection{PairID: 1, ExchangeID: domain.AggregatedExchangeID})
	h.add(unselected) // connected but has not chosen yet

	b := exchangeBook(1, 8, "asks")
	h.Publish(b)

	sent := waitSent(t, watching, 3) // catalog + snapshot + the update
	upd, ok := sent[2].(domain.WSUpdate)
	require.True(t, ok)
	assert.Equal(t, "update", upd.Type)
	assert.Equal(t, 8, upd.Book.ExchangeID())

	settle()
	assert.Len(t, other.sent(), 2, "the aggregated view must not receive a per-exchange book")
	assert.Len(t, unselected.sent(), 1, "a client with no selection receives only the catalog")
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

// Merged and aggregated arrive for the same pair+side with no exchange on
// either, so only the merged flag keeps them apart — the one thing the
// MergedExchangeID sentinel has to get right.
func TestPublish_KeysMergedSeparatelyFromAggregated(t *testing.T) {
	h := New()
	h.Publish(aggregatedBook(1, "asks"))
	h.Publish(mergedBook(1, "asks"))

	assert.Len(t, h.latest, 2)
}

func TestSelect_MergedSelectionExcludesTheAggregatedBook(t *testing.T) {
	h := New()
	for _, b := range []domain.Book{
		aggregatedBook(1, "asks"),
		mergedBook(1, "asks"),
		exchangeBook(1, 8, "asks"),
	} {
		h.latest[b.Key()] = b
	}
	c := newFakeConn()
	cl := h.add(c)

	h.selectBooks(cl, domain.Selection{PairID: 1, ExchangeID: domain.MergedExchangeID})

	snap := waitSent(t, c, 2)[1].(domain.WSSnapshot)
	require.Len(t, snap.Books, 1)
	assert.True(t, snap.Books[0].Merged)
}

func TestPublish_DropsClientWhoseWriteFails(t *testing.T) {
	h := New()
	bad, good := newFakeConn(), newFakeConn()
	bad.failAfter = 2 // catalog and snapshot land; the update breaks the pipe
	sel := domain.Selection{PairID: 1}
	h.selectBooks(h.add(bad), sel)
	h.selectBooks(h.add(good), sel)
	waitSent(t, bad, 2)

	h.Publish(aggregatedBook(1, "asks"))

	require.Eventually(t, func() bool { return bad.isClosed() }, 2*time.Second, time.Millisecond,
		"failing client should be closed")
	assert.Equal(t, 1, clientCount(h), "failing client should be removed from the client set")
	assert.False(t, good.isClosed())
	waitSent(t, good, 3)
}

func TestSetCatalog_BroadcastsOnlyWhenItChanged(t *testing.T) {
	h := New()
	c := newFakeConn()
	h.add(c)
	waitSent(t, c, 1) // the catalog sent on add

	h.SetCatalog(domain.Catalog{Exchanges: []domain.Exchange{{ID: 1, Name: "nobitex"}}})
	waitSent(t, c, 2)

	h.SetCatalog(domain.Catalog{Exchanges: []domain.Exchange{{ID: 1, Name: "nobitex"}}})
	settle()
	assert.Len(t, c.sent(), 2, "an unchanged catalog must not be re-broadcast on every refresh tick")
}

func TestRemove_ClosesAndUnregistersConn(t *testing.T) {
	h := New()
	c := newFakeConn()
	cl := h.add(c)

	h.remove(cl)

	assert.True(t, c.isClosed())
	assert.Zero(t, clientCount(h))
}

// remove is reached from both the read loop and the write loop, so it has
// to survive being called twice — closing a channel twice would panic.
func TestRemove_IsIdempotent(t *testing.T) {
	h := New()
	cl := h.add(newFakeConn())

	assert.NotPanics(t, func() {
		h.remove(cl)
		h.remove(cl)
	})
}

// The regression that motivated all of this: a browser that has stopped
// reading must not stop the hub. Before, the write happened under h.mu
// with no deadline, so one stalled socket froze Publish — and with it the
// Kafka consumers, which call Publish synchronously.
func TestPublish_DoesNotBlockOnAStalledClient(t *testing.T) {
	h := New()
	stalled := newStalledConn() // never resumed: this socket never drains
	healthy := newFakeConn()
	sel := domain.Selection{PairID: 1}
	h.selectBooks(h.add(stalled), sel)
	waitParked(t, stalled)
	h.selectBooks(h.add(healthy), sel)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			h.Publish(aggregatedBook(1, "asks"))
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked on a stalled client — the hub is serialised behind one socket again")
	}
	waitSent(t, healthy, 3)
	assert.Empty(t, stalled.sent(), "the stalled socket is still stuck on its first write")
}

// The other half of the same freeze: a NEW browser used to block in add()
// behind the stalled one, so it got the websocket handshake and then
// nothing — no catalog, no books, forever.
func TestAdd_DoesNotBlockOnAStalledClient(t *testing.T) {
	h := New()
	stalled := newStalledConn()
	h.selectBooks(h.add(stalled), domain.Selection{PairID: 1})
	waitParked(t, stalled)

	arriving := newFakeConn()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.add(arriving)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("add blocked on another client's stalled socket")
	}
	waitSent(t, arriving, 1)
}

// Every message carries a COMPLETE book, so a browser that falls behind
// should skip frames rather than queue them: what it eventually receives
// must be the newest state, and the queue must not grow with the backlog.
func TestOutbox_CoalescesUpdatesForTheSameBook(t *testing.T) {
	h := New()
	c := newStalledConn()
	cl := h.add(c)
	waitParked(t, c) // the writer is stuck on the catalog; everything below queues
	h.selectBooks(cl, domain.Selection{PairID: 1})

	var last domain.Book
	for i := 1; i <= 50; i++ {
		last = aggregatedBook(1, "asks")
		last.EventTime = int64(i)
		h.Publish(last)
	}

	cl.mu.Lock()
	queued := len(cl.pending)
	cl.mu.Unlock()
	assert.LessOrEqual(t, queued, 2, "50 updates for one book must collapse to one queued update")

	c.resume()
	sent := waitSent(t, c, 3)
	assert.Len(t, sent, 3, "catalog + snapshot + one coalesced update, not 52 messages")
	assert.Equal(t, last, sent[2].(domain.WSUpdate).Book, "the surviving update is the newest one")
}

// Two sides of one book are two keys, so coalescing must not merge them.
func TestOutbox_KeepsBothSidesOfTheBook(t *testing.T) {
	h := New()
	c := newStalledConn()
	cl := h.add(c)
	waitParked(t, c)
	h.selectBooks(cl, domain.Selection{PairID: 1})

	h.Publish(aggregatedBook(1, "asks"))
	h.Publish(aggregatedBook(1, "bids"))
	h.Publish(aggregatedBook(1, "asks"))

	c.resume()
	sent := waitSent(t, c, 4)
	assert.Len(t, sent, 4, "catalog + snapshot + one update per side")
	assert.Equal(t, "asks", sent[2].(domain.WSUpdate).Book.Side)
	assert.Equal(t, "bids", sent[3].(domain.WSUpdate).Book.Side)
}

// A snapshot answers the client's whole selection, so book messages
// queued before it are already accounted for and must not be replayed
// after it — that would paint a stale book over the fresh one. The same
// applies to an earlier snapshot: the newest one is the whole answer.
func TestOutbox_SnapshotSupersedesQueuedUpdates(t *testing.T) {
	h := New()
	c := newStalledConn()
	cl := h.add(c)
	waitParked(t, c)
	h.selectBooks(cl, domain.Selection{PairID: 1})
	h.Publish(aggregatedBook(1, "asks"))

	// The browser switches pair; the queued update belongs to the old one.
	h.selectBooks(cl, domain.Selection{PairID: 2})

	c.resume()
	sent := waitSent(t, c, 2)
	settle()
	require.Len(t, c.sent(), 2, "catalog + the newest snapshot; the stale update and snapshot are dropped")
	snap, ok := sent[1].(domain.WSSnapshot)
	require.True(t, ok, "no update should survive the snapshot that answers it")
	assert.Equal(t, "snapshot", snap.Type)
}

// A write with no deadline is what let a stalled socket park forever.
func TestWrite_SetsADeadlineBeforeEveryWrite(t *testing.T) {
	h := New()
	c := newFakeConn()
	cl := h.add(c)
	h.selectBooks(cl, domain.Selection{PairID: 1})
	h.Publish(aggregatedBook(1, "asks"))

	waitSent(t, c, 3)
	c.mu.Lock()
	defer c.mu.Unlock()
	assert.Len(t, c.deadlines, 3, "one deadline set per message written")
	for _, d := range c.deadlines {
		assert.False(t, d.IsZero(), "a zero deadline means no deadline at all")
	}
}

// The ping is what makes a half-open connection detectable: without it a
// client watching a quiet pair is never written to, so it is never found
// to be gone.
func TestWriteLoop_PingsIdleClients(t *testing.T) {
	restore := pingPeriod
	pingPeriod = time.Millisecond
	defer func() { pingPeriod = restore }()

	h := New()
	c := newFakeConn()
	h.add(c)

	require.Eventually(t, func() bool { return c.pingCount() >= 3 }, 2*time.Second, time.Millisecond,
		"an idle client should still be pinged")
	assert.Equal(t, 1, clientCount(h), "a client answering pings stays connected")
}

func TestWriteLoop_DropsClientWhosePingFails(t *testing.T) {
	restore := pingPeriod
	pingPeriod = time.Millisecond
	defer func() { pingPeriod = restore }()

	h := New()
	c := newFakeConn()
	c.failAfter = 1 // the catalog lands, then everything fails
	h.add(c)
	waitSent(t, c, 1)

	require.Eventually(t, func() bool { return c.isClosed() }, 2*time.Second, time.Millisecond,
		"a client whose ping fails should be dropped")
	assert.Zero(t, clientCount(h))
}
