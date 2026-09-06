// Package hub fans books out to connected browsers over websocket. It
// holds the latest book per (pair, exchange, side) and pushes to each
// client only the pair+exchange that client selected — with a book per
// exchange on top of the aggregated one, broadcasting everything to
// everyone would send each browser far more than it can display.
//
// Nothing here ever writes to a socket while h.mu is held. It used to,
// and one browser that stopped reading — a backgrounded tab, a sleeping
// laptop, a slow link — blocked that write forever (gorilla has no write
// deadline of its own), froze every other hub method behind the mutex,
// and with it the Kafka consumers, which call Publish synchronously. The
// symptom was a server that answered the websocket handshake and then
// sent nothing at all, to anybody, until it was restarted. Writes now
// happen on a per-client goroutine, off the shared lock, under a
// deadline.
package hub

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"orderbook-web/internal/domain"
)

const (
	// writeWait bounds a single write. Past it the client is dropped
	// rather than waited on — the browser reconnects on its own and asks
	// for a fresh snapshot, which costs far less than a stuck socket.
	writeWait = 10 * time.Second
	// pongWait is how long a silent connection is tolerated. Without it
	// and the ping below, a half-open connection (sleeping laptop, dropped
	// wifi, NAT timeout) never errors on read, so its client is never
	// removed and every Publish keeps writing to a socket nobody is on.
	pongWait = 60 * time.Second
	// readLimit caps a client -> server message. The only thing a browser
	// ever sends is a select, which is a few dozen bytes.
	readLimit = 4096
)

// pingPeriod is how often we prove the link is alive; it must stay
// comfortably below pongWait so one lost ping is survivable. A var rather
// than a const only so the tests can exercise the ping path without
// waiting on the real interval.
var pingPeriod = 25 * time.Second

// conn is the subset of *websocket.Conn the hub needs. Depending on this
// instead of the concrete gorilla type lets the broadcast/prune logic be
// unit-tested with a fake, without a real socket. The read side's
// deadline and pong handler are set on the concrete conn in ServeWS, so
// they are deliberately not here.
type conn interface {
	WriteJSON(v any) error
	SetWriteDeadline(t time.Time) error
	WriteControl(messageType int, data []byte, deadline time.Time) error
	ReadMessage() (messageType int, p []byte, err error)
	Close() error
}

// client is one browser plus what it asked to see. selected stays false
// until the first select message arrives, so a client that has only just
// connected receives nothing but the catalog.
//
// pending is this client's outbound queue, drained by its own writeLoop.
// It coalesces rather than grows: every message here carries a COMPLETE
// book or a complete catalog, so a newer one for the same slot makes the
// older one worthless. That bounds pending at one catalog, one snapshot
// and one update per side of the single selection — about four entries,
// no matter how far behind the browser falls. A slow client therefore
// loses intermediate frames, never correctness, and is never dropped for
// being slow.
type client struct {
	c    conn
	id   int64
	addr string

	sel      domain.Selection
	selected bool

	mu      sync.Mutex
	pending []any
	wake    chan struct{} // capacity 1: a coalescing wakeup, never blocks
	done    chan struct{}
	once    sync.Once

	// skipped counts frames this client never saw because a newer one
	// replaced them in the queue. It is the one number that says "this
	// browser is not keeping up" — nothing else distinguishes a slow
	// client from a quiet pair, and that ambiguity is what made the
	// original freeze so hard to see.
	skipped atomic.Int64
}

// clientIDs numbers connections so a log line about one browser can be
// followed from connect to disconnect. An address alone is not enough:
// the same browser reconnects from the same address every 2s.
var clientIDs atomic.Int64

func newClient(c conn, addr string) *client {
	return &client{
		c:    c,
		id:   clientIDs.Add(1),
		addr: addr,
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
	}
}

// String is what every log line about this client uses, so the format is
// defined once.
func (cl *client) String() string {
	return "client #" + strconv.FormatInt(cl.id, 10) + " (" + cl.addr + ")"
}

// enqueue queues one message and nudges the writer. It never blocks, so
// callers may hold h.mu.
func (cl *client) enqueue(msg any) {
	cl.mu.Lock()
	before := len(cl.pending)
	cl.pending = coalesce(cl.pending, msg)
	// Anything the queue did not grow by is a frame this browser will
	// never see, because a newer one took its place.
	if dropped := before + 1 - len(cl.pending); dropped > 0 {
		cl.skipped.Add(int64(dropped))
	}
	cl.mu.Unlock()
	select {
	case cl.wake <- struct{}{}:
	default: // a wakeup is already pending; one is as good as two
	}
}

// take hands the queued messages to the writer and empties the queue. It
// must not be called with any other lock held.
func (cl *client) take() []any {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	p := cl.pending
	cl.pending = nil
	return p
}

// stop is idempotent: both the read loop and the write loop end up here.
func (cl *client) stop() {
	cl.once.Do(func() {
		close(cl.done)
		cl.c.Close()
	})
}

// coalesce adds msg to the queue, replacing in place anything it makes
// obsolete rather than queueing behind it. Order is otherwise preserved:
// a message that supersedes nothing goes to the back.
func coalesce(pending []any, msg any) []any {
	switch m := msg.(type) {
	case domain.WSCatalog:
		for i, p := range pending {
			if _, ok := p.(domain.WSCatalog); ok {
				pending[i] = m
				return pending
			}
		}
	case domain.WSSnapshot:
		// A snapshot is the whole of the client's selection, so every book
		// message queued before it is answered by it — including updates
		// for the selection it just switched away from.
		kept := pending[:0]
		for _, p := range pending {
			if _, ok := p.(domain.WSCatalog); ok {
				kept = append(kept, p)
			}
		}
		pending = kept
	case domain.WSUpdate:
		for i, p := range pending {
			if prev, ok := p.(domain.WSUpdate); ok && prev.Book.Key() == m.Book.Key() {
				pending[i] = m
				return pending
			}
		}
	}
	return append(pending, msg)
}

// Hub holds the connected clients, the latest book per key, and the
// catalog last published to the browsers.
type Hub struct {
	mu      sync.Mutex
	clients map[*client]bool
	latest  map[domain.Selection]domain.Book
	catalog domain.Catalog

	// published counts books that have reached the hub, so the heartbeat
	// can report a RATE. A count that stops moving is the signature of a
	// stalled consumer, and it is invisible in a snapshot of state alone.
	published  atomic.Int64
	lastLogged int64
}

func New() *Hub {
	return &Hub{
		clients: map[*client]bool{},
		latest:  map[domain.Selection]domain.Book{},
	}
}

// SetCatalog publishes the dropdown content, broadcasting only when it
// actually changed — it is recomputed on the registry's refresh tick, and
// that is almost always the same list as last time.
func (h *Hub) SetCatalog(c domain.Catalog) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if reflect.DeepEqual(c, h.catalog) {
		return
	}
	h.catalog = c
	msg := domain.WSCatalog{Type: "catalog", Catalog: c}
	for cl := range h.clients {
		cl.enqueue(msg)
	}
}

// Publish stores the latest book for its key and pushes it to every
// client currently looking at it.
func (h *Hub) Publish(b domain.Book) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, seen := h.latest[b.Key()]; !seen {
		// Once per book, so it is cheap, and it answers the first question
		// anyone asks: is this pair/exchange/side reaching the UI at all?
		log.Printf("hub: first book for pair %d, exchange %d, %s (%d level(s))",
			b.PairID, b.ExchangeID(), b.Side, len(b.Levels))
	}
	h.published.Add(1)
	h.latest[b.Key()] = b
	msg := domain.WSUpdate{Type: "update", Book: b}
	for cl := range h.clients {
		if cl.selected && cl.sel.Matches(b) {
			cl.enqueue(msg)
		}
	}
}

// add registers a client, starts its writer, and queues the catalog,
// which is all it can use before it has told us what to show.
func (h *Hub) add(c conn, addr string) *client {
	cl := newClient(c, addr)

	h.mu.Lock()
	h.clients[cl] = true
	cl.enqueue(domain.WSCatalog{Type: "catalog", Catalog: h.catalog})
	total, books := len(h.clients), len(h.latest)
	h.mu.Unlock()

	log.Printf("ws connect: %s — %d client(s) now, %d book(s) held", cl, total, books)
	go h.writeLoop(cl)
	return cl
}

// remove unregisters a client. reason says which of the three ways out it
// took (the browser closed, a write failed, a ping went unanswered) —
// telling those apart is the difference between "a user closed a tab" and
// "we are dropping clients we should be keeping".
func (h *Hub) remove(cl *client, reason string) {
	h.mu.Lock()
	_, present := h.clients[cl]
	delete(h.clients, cl)
	total := len(h.clients)
	h.mu.Unlock()

	// remove is reached from both loops; only the first one through says so.
	if present {
		log.Printf("ws disconnect: %s — %s (%d client(s) left, %d frame(s) skipped while connected)",
			cl, reason, total, cl.skipped.Load())
	}
	cl.stop()
}

// selectBooks records a client's selection and immediately answers with
// everything held for it, so switching pair or exchange paints at once
// instead of waiting for the next Kafka record.
func (h *Hub) selectBooks(cl *client, sel domain.Selection) {
	h.mu.Lock()
	defer h.mu.Unlock()
	cl.sel = sel
	cl.selected = true

	books := make([]domain.Book, 0, 2)
	for _, b := range h.latest {
		if sel.Matches(b) {
			books = append(books, b)
		}
	}
	cl.enqueue(domain.WSSnapshot{Type: "snapshot", Books: books})

	// An empty snapshot is the single most useful line in this file when
	// someone reports "the page shows nothing": it says the request
	// arrived and was answered, and that the hub simply holds no book for
	// what was asked — so the question is upstream, not here.
	if len(books) == 0 {
		log.Printf("ws select: %s wants pair %d, exchange %d — NOTHING HELD for it yet (%d book(s) held in total)",
			cl, sel.PairID, sel.ExchangeID, len(h.latest))
		return
	}
	log.Printf("ws select: %s wants pair %d, exchange %d — answered with %d book(s)",
		cl, sel.PairID, sel.ExchangeID, len(books))
}

// LogStats prints one heartbeat line: enough to tell a healthy idle
// server (books held, rate zero because the market is quiet) from a stuck
// one (clients connected, books held, rate zero because nothing is being
// consumed) without attaching a debugger. Called on a ticker from main.
//
// Clients that are falling behind get a line of their own, because that
// is the early warning the original freeze never gave: back then a slow
// browser silently took the whole server with it.
func (h *Hub) LogStats(every time.Duration) {
	published := h.published.Load()
	rate := float64(published-h.lastLogged) / every.Seconds()
	h.lastLogged = published

	h.mu.Lock()
	clients, books := len(h.clients), len(h.latest)
	behind := make([]*client, 0, len(h.clients))
	for cl := range h.clients {
		if cl.skipped.Load() > 0 {
			behind = append(behind, cl)
		}
	}
	h.mu.Unlock()

	log.Printf("hub: %d client(s), %d book(s) held, %.1f book(s)/s in (%d total)",
		clients, books, rate, published)

	sort.Slice(behind, func(i, j int) bool { return behind[i].id < behind[j].id })
	for _, cl := range behind {
		cl.mu.Lock()
		queued := len(cl.pending)
		cl.mu.Unlock()
		log.Printf("hub: %s is behind — %d frame(s) skipped so far, %d queued. "+
			"It sees fewer updates but stays correct; it is not blocking anyone else.",
			cl, cl.skipped.Load(), queued)
	}
}

// writeLoop owns every write to one client's socket. Running one per
// client is what keeps a slow browser's cost to itself, and gorilla
// forbids concurrent writes to a conn — being the only writer is what
// satisfies that now the hub's mutex no longer does.
func (h *Hub) writeLoop(cl *client) {
	ping := time.NewTicker(pingPeriod)
	defer ping.Stop()
	for {
		select {
		case <-cl.done:
			return
		case <-cl.wake:
			for _, msg := range cl.take() {
				if err := cl.write(msg); err != nil {
					h.remove(cl, "write failed: "+err.Error())
					return
				}
			}
		case <-ping.C:
			if err := cl.c.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait)); err != nil {
				h.remove(cl, "ping failed: "+err.Error())
				return
			}
		}
	}
}

// write sends one message under a deadline, so a peer that has stopped
// reading fails this call instead of parking on it forever.
func (cl *client) write(msg any) error {
	if err := cl.c.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
		return err
	}
	return cl.c.WriteJSON(msg)
}

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

// ServeWS upgrades the request to a websocket and serves it until the
// client disconnects. Unlike before, the read loop carries meaning: the
// browser sends a select message on connect and on every dropdown change.
// The read deadline is what turns a half-open connection into a normal
// disconnect: our pings draw pongs, each pong pushes the deadline out,
// and a peer that has silently gone away simply stops doing so.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		// The browser never gets a usable socket, so if this fires the page
		// looks dead and nothing else in this file will ever say why.
		log.Printf("ws upgrade failed for %s: %v", r.RemoteAddr, err)
		return
	}
	c.SetReadLimit(readLimit)
	_ = c.SetReadDeadline(time.Now().Add(pongWait))
	c.SetPongHandler(func(string) error {
		return c.SetReadDeadline(time.Now().Add(pongWait))
	})

	cl := h.add(c, r.RemoteAddr)
	var readErr error
	for {
		_, data, err := c.ReadMessage()
		if err != nil {
			readErr = err
			break
		}
		var sel domain.WSSelect
		if err := json.Unmarshal(data, &sel); err != nil || sel.Type != "select" {
			// Not fatal, but worth saying: a browser talking a shape we do
			// not understand is silently getting nothing back.
			log.Printf("ws: ignoring unrecognised message from %s: %.120q", cl, data)
			continue
		}
		h.selectBooks(cl, domain.Selection{PairID: sel.PairID, ExchangeID: sel.ExchangeID})
	}
	h.remove(cl, readReason(readErr))
}

// readReason turns the read loop's exit into something a person can act
// on. A normal tab close and a read deadline that expired because the
// browser stopped answering pings mean very different things.
func readReason(err error) string {
	switch {
	case err == nil:
		return "read loop ended"
	case websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway):
		return "browser closed the connection"
	case os.IsTimeout(err):
		return "no pong within " + pongWait.String() + " — connection was half-open"
	default:
		return "read failed: " + err.Error()
	}
}
