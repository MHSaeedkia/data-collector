// Package hub fans the latest book per topic out to all connected
// browsers over websocket.
package hub

import (
	"net/http"
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

// Hub holds the set of connected clients and the latest book per topic.
type Hub struct {
	mu      sync.Mutex
	clients map[conn]bool
	latest  map[string]domain.Book
}

func New() *Hub {
	return &Hub{clients: map[conn]bool{}, latest: map[string]domain.Book{}}
}

// add registers a client and immediately sends everything held so it
// renders at once.
func (h *Hub) add(c conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c] = true
	books := make([]domain.Book, 0, len(h.latest))
	for _, b := range h.latest {
		books = append(books, b)
	}
	_ = c.WriteJSON(domain.WSSnapshot{Type: "snapshot", Books: books})
}

func (h *Hub) remove(c conn) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
	c.Close()
}

// Publish stores the latest book for a topic and broadcasts it to every
// client, dropping any client whose write fails.
func (h *Hub) Publish(topic string, b domain.Book) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.latest[topic] = b
	msg := domain.WSUpdate{Type: "update", Book: b}
	for c := range h.clients {
		if err := c.WriteJSON(msg); err != nil {
			c.Close()
			delete(h.clients, c)
		}
	}
}

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

// ServeWS upgrades the request to a websocket and serves it until the
// client disconnects.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	h.add(c)
	// Block on reads only to detect the close; the browser never sends anything.
	for {
		if _, _, err := c.ReadMessage(); err != nil {
			break
		}
	}
	h.remove(c)
}
