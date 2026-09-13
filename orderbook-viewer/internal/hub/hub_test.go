package hub

import (
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"orderbook-viewer/internal/domain"
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

// testLimit is the depth these hubs are configured with. It is one of
// domain.LevelLimits and deeper than any book built here, so it changes
// nothing except in the tests that are about limiting.
const testLimit = 200

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
	h := New(testLimit)
	h.SetCatalog(domain.Catalog{Markets: []domain.Market{{ID: 1, Base: "BTC", Quote: "USDT"}}})
	c := newFakeConn()

	h.add(c, "test")

	sent := waitSent(t, c, 1)
	cat, ok := sent[0].(domain.WSCatalog)
	require.True(t, ok, "a client's first message is the catalog, not book data")
	assert.Equal(t, "catalog", cat.Type)
	require.Len(t, cat.Markets, 1)
	assert.Equal(t, "BTC", cat.Markets[0].Base)
}

func TestSelect_AnswersWithHeldBooksForThatSelectionOnly(t *testing.T) {
	h := New(testLimit)
	for _, b := range []domain.Book{
		aggregatedBook(1, "asks"),
		aggregatedBook(1, "bids"),
		aggregatedBook(2, "asks"),
		exchangeBook(1, 8, "asks"),
	} {
		h.latest[b.Key()] = b
	}
	c := newFakeConn()
	cl := h.add(c, "test")

	h.selectBooks(cl, domain.Selection{PairID: 1, ExchangeID: domain.AggregatedExchangeID}, 0)

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
	h := New(testLimit)
	for _, b := range []domain.Book{
		aggregatedBook(1, "asks"),
		exchangeBook(1, 8, "asks"),
		exchangeBook(1, 6, "asks"),
	} {
		h.latest[b.Key()] = b
	}
	c := newFakeConn()
	cl := h.add(c, "test")

	h.selectBooks(cl, domain.Selection{PairID: 1, ExchangeID: 8}, 0)

	snap := waitSent(t, c, 2)[1].(domain.WSSnapshot)
	require.Len(t, snap.Books, 1)
	assert.Equal(t, 8, snap.Books[0].ExchangeID())
}

func TestPublish_ReachesOnlyClientsWatchingThatBook(t *testing.T) {
	h := New(testLimit)
	watching, other, unselected := newFakeConn(), newFakeConn(), newFakeConn()
	h.selectBooks(h.add(watching, "test"), domain.Selection{PairID: 1, ExchangeID: 8}, 0)
	h.selectBooks(h.add(other, "test"), domain.Selection{PairID: 1, ExchangeID: domain.AggregatedExchangeID}, 0)
	h.add(unselected, "test") // connected but has not chosen yet

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
	h := New(testLimit)
	h.Publish(aggregatedBook(1, "asks"))
	h.Publish(exchangeBook(1, 8, "asks"))

	assert.Len(t, h.latest, 2)
}

// Merged and aggregated arrive for the same pair+side with no exchange on
// either, so only the merged flag keeps them apart — the one thing the
// MergedExchangeID sentinel has to get right.
func TestPublish_KeysMergedSeparatelyFromAggregated(t *testing.T) {
	h := New(testLimit)
	h.Publish(aggregatedBook(1, "asks"))
	h.Publish(mergedBook(1, "asks"))

	assert.Len(t, h.latest, 2)
}

func TestSelect_MergedSelectionExcludesTheAggregatedBook(t *testing.T) {
	h := New(testLimit)
	for _, b := range []domain.Book{
		aggregatedBook(1, "asks"),
		mergedBook(1, "asks"),
		exchangeBook(1, 8, "asks"),
	} {
		h.latest[b.Key()] = b
	}
	c := newFakeConn()
	cl := h.add(c, "test")

	h.selectBooks(cl, domain.Selection{PairID: 1, ExchangeID: domain.MergedExchangeID}, 0)

	snap := waitSent(t, c, 2)[1].(domain.WSSnapshot)
	require.Len(t, snap.Books, 1)
	assert.True(t, snap.Books[0].Merged)
}

func TestPublish_DropsClientWhoseWriteFails(t *testing.T) {
	h := New(testLimit)
	bad, good := newFakeConn(), newFakeConn()
	bad.failAfter = 2 // catalog and snapshot land; the update breaks the pipe
	sel := domain.Selection{PairID: 1}
	h.selectBooks(h.add(bad, "test"), sel, 0)
	h.selectBooks(h.add(good, "test"), sel, 0)
	waitSent(t, bad, 2)

	h.Publish(aggregatedBook(1, "asks"))

	require.Eventually(t, func() bool { return bad.isClosed() }, 2*time.Second, time.Millisecond,
		"failing client should be closed")
	assert.Equal(t, 1, clientCount(h), "failing client should be removed from the client set")
	assert.False(t, good.isClosed())
	waitSent(t, good, 3)
}

func TestSetCatalog_BroadcastsOnlyWhenItChanged(t *testing.T) {
	h := New(testLimit)
	c := newFakeConn()
	h.add(c, "test")
	waitSent(t, c, 1) // the catalog sent on add

	h.SetCatalog(domain.Catalog{Exchanges: []domain.Exchange{{ID: 1, Name: "nobitex"}}})
	waitSent(t, c, 2)

	h.SetCatalog(domain.Catalog{Exchanges: []domain.Exchange{{ID: 1, Name: "nobitex"}}})
	settle()
	assert.Len(t, c.sent(), 2, "an unchanged catalog must not be re-broadcast on every refresh tick")
}

func TestRemove_ClosesAndUnregistersConn(t *testing.T) {
	h := New(testLimit)
	c := newFakeConn()
	cl := h.add(c, "test")

	h.remove(cl, "test")

	assert.True(t, c.isClosed())
	assert.Zero(t, clientCount(h))
}

// remove is reached from both the read loop and the write loop, so it has
// to survive being called twice — closing a channel twice would panic.
func TestRemove_IsIdempotent(t *testing.T) {
	h := New(testLimit)
	cl := h.add(newFakeConn(), "test")

	assert.NotPanics(t, func() {
		h.remove(cl, "test")
		h.remove(cl, "test")
	})
}

// The regression that motivated all of this: a browser that has stopped
// reading must not stop the hub. Before, the write happened under h.mu
// with no deadline, so one stalled socket froze Publish — and with it the
// Kafka consumers, which call Publish synchronously.
func TestPublish_DoesNotBlockOnAStalledClient(t *testing.T) {
	h := New(testLimit)
	stalled := newStalledConn() // never resumed: this socket never drains
	healthy := newFakeConn()
	sel := domain.Selection{PairID: 1}
	h.selectBooks(h.add(stalled, "test"), sel, 0)
	waitParked(t, stalled)
	h.selectBooks(h.add(healthy, "test"), sel, 0)

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
	h := New(testLimit)
	stalled := newStalledConn()
	h.selectBooks(h.add(stalled, "test"), domain.Selection{PairID: 1}, 0)
	waitParked(t, stalled)

	arriving := newFakeConn()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.add(arriving, "test")
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
	h := New(testLimit)
	c := newStalledConn()
	cl := h.add(c, "test")
	waitParked(t, c) // the writer is stuck on the catalog; everything below queues
	h.selectBooks(cl, domain.Selection{PairID: 1}, 0)

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
	h := New(testLimit)
	c := newStalledConn()
	cl := h.add(c, "test")
	waitParked(t, c)
	h.selectBooks(cl, domain.Selection{PairID: 1}, 0)

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
	h := New(testLimit)
	c := newStalledConn()
	cl := h.add(c, "test")
	waitParked(t, c)
	h.selectBooks(cl, domain.Selection{PairID: 1}, 0)
	h.Publish(aggregatedBook(1, "asks"))

	// The browser switches pair; the queued update belongs to the old one.
	h.selectBooks(cl, domain.Selection{PairID: 2}, 0)

	c.resume()
	sent := waitSent(t, c, 2)
	settle()
	require.Len(t, c.sent(), 2, "catalog + the newest snapshot; the stale update and snapshot are dropped")
	snap, ok := sent[1].(domain.WSSnapshot)
	require.True(t, ok, "no update should survive the snapshot that answers it")
	assert.Equal(t, "snapshot", snap.Type)
}

// The skipped counter is what the heartbeat reports, and it is the only
// signal that separates "this browser is slow" from "this pair is quiet".
func TestOutbox_CountsTheFramesASlowClientNeverSaw(t *testing.T) {
	h := New(testLimit)
	c := newStalledConn()
	cl := h.add(c, "test")
	waitParked(t, c)
	h.selectBooks(cl, domain.Selection{PairID: 1}, 0)

	for i := 0; i < 10; i++ {
		h.Publish(aggregatedBook(1, "asks"))
	}

	// The first update queues; the nine after it each replace their
	// predecessor, and every replacement is a frame this browser lost.
	assert.Equal(t, int64(9), cl.skipped.Load())

	c.resume()
	waitSent(t, c, 3)
}

// The heartbeat runs on a ticker in production, so it must be safe to
// call while writers are removing clients underneath it.
func TestLogStats_IsSafeWhileClientsComeAndGo(t *testing.T) {
	h := New(testLimit)
	slow := newStalledConn()
	cl := h.add(slow, "test")
	waitParked(t, slow)
	h.selectBooks(cl, domain.Selection{PairID: 1}, 0)
	h.Publish(aggregatedBook(1, "asks"))
	h.Publish(aggregatedBook(1, "asks"))

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			h.LogStats(time.Second)
		}
	}()
	for i := 0; i < 20; i++ {
		h.remove(h.add(newFakeConn(), "churn"), "test")
	}
	<-done
	assert.NotZero(t, cl.skipped.Load(), "the slow client is the one the heartbeat should name")
}

// A write with no deadline is what let a stalled socket park forever.
func TestWrite_SetsADeadlineBeforeEveryWrite(t *testing.T) {
	h := New(testLimit)
	c := newFakeConn()
	cl := h.add(c, "test")
	h.selectBooks(cl, domain.Selection{PairID: 1}, 0)
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

	h := New(testLimit)
	c := newFakeConn()
	h.add(c, "test")

	require.Eventually(t, func() bool { return c.pingCount() >= 3 }, 2*time.Second, time.Millisecond,
		"an idle client should still be pinged")
	assert.Equal(t, 1, clientCount(h), "a client answering pings stays connected")
}

func TestWriteLoop_DropsClientWhosePingFails(t *testing.T) {
	restore := pingPeriod
	pingPeriod = time.Millisecond
	defer func() { pingPeriod = restore }()

	h := New(testLimit)
	c := newFakeConn()
	c.failAfter = 1 // the catalog lands, then everything fails
	h.add(c, "test")
	waitSent(t, c, 1)

	require.Eventually(t, func() bool { return c.isClosed() }, 2*time.Second, time.Millisecond,
		"a client whose ping fails should be dropped")
	assert.Zero(t, clientCount(h))
}

// deepBook is a book with more levels than any depth the UI offers, so a
// test can tell a limited book from an unlimited one.
func deepBook(pairID int, side string) domain.Book {
	levels := make([]domain.Level, 0, 300)
	for i := 0; i < 300; i++ {
		levels = append(levels, domain.Level{Price: strconv.Itoa(i), Quantity: "1"})
	}
	return domain.Book{PairID: pairID, Side: side, Levels: levels}
}

// The depth is a server-side cut, not something the page does after the
// fact: a client that asked for 25 levels is sent 25, not 300 it has to
// throw away.
func TestSelect_AnswersAtTheDepthTheClientAskedFor(t *testing.T) {
	h := New(testLimit)
	b := deepBook(1, "asks")
	h.latest[b.Key()] = b
	c := newFakeConn()
	cl := h.add(c, "test")

	h.selectBooks(cl, domain.Selection{PairID: 1, ExchangeID: domain.AggregatedExchangeID}, 25)

	snap := waitSent(t, c, 2)[1].(domain.WSSnapshot)
	require.Len(t, snap.Books, 1)
	assert.Len(t, snap.Books[0].Levels, 25)
	assert.Equal(t, "0", snap.Books[0].Levels[0].Price, "the levels kept are the ones nearest the spread")
	assert.Len(t, h.latest[b.Key()].Levels, 300, "the stored book keeps every level")
}

// One book, two browsers, two depths — so the message cannot be built
// once and shared.
func TestPublish_SendsEachClientItsOwnDepth(t *testing.T) {
	h := New(testLimit)
	shallow, deep := newFakeConn(), newFakeConn()
	sel := domain.Selection{PairID: 1, ExchangeID: domain.AggregatedExchangeID}
	h.selectBooks(h.add(shallow, "test"), sel, 25)
	h.selectBooks(h.add(deep, "test"), sel, 100)

	h.Publish(deepBook(1, "asks"))

	assert.Len(t, waitSent(t, shallow, 3)[2].(domain.WSUpdate).Book.Levels, 25)
	assert.Len(t, waitSent(t, deep, 3)[2].(domain.WSUpdate).Book.Levels, 100)
}

// A client asking for a depth nobody offers is served the default, not
// the whole book: the limit exists to bound what a browser receives, so
// an unrecognised request must not be the way around it.
func TestSelect_ADepthTheUIDoesNotOfferFallsBackToTheDefault(t *testing.T) {
	for _, asked := range []int{0, -1, 5000, 7} {
		t.Run(strconv.Itoa(asked), func(t *testing.T) {
			h := New(25)
			b := deepBook(1, "asks")
			h.latest[b.Key()] = b
			c := newFakeConn()

			h.selectBooks(h.add(c, "test"), domain.Selection{PairID: 1}, asked)

			snap := waitSent(t, c, 2)[1].(domain.WSSnapshot)
			require.Len(t, snap.Books, 1)
			assert.Len(t, snap.Books[0].Levels, 25)
		})
	}
}

// The hub forwards the catalog exactly as the registry built it — the
// dropdown contents, defaults included, are not its business.
func TestCatalog_IsForwardedUnchanged(t *testing.T) {
	h := New(100)
	built := domain.Catalog{
		Markets:           []domain.Market{{ID: 1, Base: "BTC", Quote: "USDT"}},
		LevelLimits:       domain.LevelLimits,
		DefaultLevelLimit: 50,
		DefaultPairID:     1,
		DefaultExchangeID: domain.MergedExchangeID,
	}
	h.SetCatalog(built)
	c := newFakeConn()

	h.add(c, "test")

	cat := waitSent(t, c, 1)[0].(domain.WSCatalog)
	assert.Equal(t, built, cat.Catalog)
}
