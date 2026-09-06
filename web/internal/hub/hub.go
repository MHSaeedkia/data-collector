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
	"net/http"
	"reflect"
	"sync"
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
	c        conn
	sel      domain.Selection
	selected bool

	mu      sync.Mutex
	pending []any
	wake    chan struct{} // capacity 1: a coalescing wakeup, never blocks
	done    chan struct{}
	once    sync.Once
}

func newClient(c conn) *client {
	return &client{
		c:    c,
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
	}
}

// enqueue queues one message and nudges the writer. It never blocks, so
// callers may hold h.mu.
func (cl *client) enqueue(msg any) {
	cl.mu.Lock()
	cl.pending = coalesce(cl.pending, msg)
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
func (h *Hub) add(c conn) *client {
	cl := newClient(c)

	h.mu.Lock()
	h.clients[cl] = true
	cl.enqueue(domain.WSCatalog{Type: "catalog", Catalog: h.catalog})
	h.mu.Unlock()

	go h.writeLoop(cl)
	return cl
}

func (h *Hub) remove(cl *client) {
	h.mu.Lock()
	delete(h.clients, cl)
	h.mu.Unlock()
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
					h.remove(cl)
					return
				}
			}
		case <-ping.C:
			if err := cl.c.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait)); err != nil {
				h.remove(cl)
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
		return
	}
	c.SetReadLimit(readLimit)
	_ = c.SetReadDeadline(time.Now().Add(pongWait))
	c.SetPongHandler(func(string) error {
		return c.SetReadDeadline(time.Now().Add(pongWait))
	})

	cl := h.add(c)
	for {
		_, data, err := c.ReadMessage()
		if err != nil {
			break
		}
		var sel domain.WSSelect
		if err := json.Unmarshal(data, &sel); err != nil || sel.Type != "select" {
			continue // ignore anything we don't understand rather than dropping the client
		}
		h.selectBooks(cl, domain.Selection{PairID: sel.PairID, ExchangeID: sel.ExchangeID})
	}
	h.remove(cl)
}
