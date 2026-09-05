// Package hub fans books out to connected browsers over websocket. It
// holds the latest book per (pair, exchange, side) and pushes to each
// client only the pair+exchange that client selected — with a book per
// exchange on top of the aggregated one, broadcasting everything to
// everyone would send each browser far more than it can display.
package hub

import (
	"encoding/json"
	"net/http"
	"reflect"
	"sync"

	"github.com/gorilla/websocket"

	"orderbook-web/internal/domain"
)

// conn is the subset of *websocket.Conn the hub needs. Depending on this
// instead of the concrete gorilla type lets the broadcast/prune logic be
// unit-tested with a fake, without a real socket.
type conn interface {
	WriteJSON(v any) error
	ReadMessage() (messageType int, p []byte, err error)
	Close() error
}

// client is one browser plus what it asked to see. selected stays false
// until the first select message arrives, so a client that has only just
// connected receives nothing but the catalog.
type client struct {
	c        conn
	sel      domain.Selection
	selected bool
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
		h.send(cl, msg)
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
			h.send(cl, msg)
		}
	}
}

// add registers a client and sends it the catalog, which is all it can
// use before it has told us what to show.
func (h *Hub) add(c conn) *client {
	h.mu.Lock()
	defer h.mu.Unlock()
	cl := &client{c: c}
	h.clients[cl] = true
	h.send(cl, domain.WSCatalog{Type: "catalog", Catalog: h.catalog})
	return cl
}

func (h *Hub) remove(cl *client) {
	h.mu.Lock()
	delete(h.clients, cl)
	h.mu.Unlock()
	cl.c.Close()
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
	h.send(cl, domain.WSSnapshot{Type: "snapshot", Books: books})
}

// send writes to one client, dropping it if the write fails. Callers hold
// h.mu — which is also what serializes writes to a single connection,
// since gorilla forbids concurrent ones.
func (h *Hub) send(cl *client, msg any) {
	if err := cl.c.WriteJSON(msg); err != nil {
		cl.c.Close()
		delete(h.clients, cl)
	}
}

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

// ServeWS upgrades the request to a websocket and serves it until the
// client disconnects. Unlike before, the read loop carries meaning: the
// browser sends a select message on connect and on every dropdown change.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
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
