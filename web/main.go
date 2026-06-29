package main

import (
	"context"
	"embed"
	"encoding/json"
	"io/fs"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The UI is baked into the binary so it ships as a single self-contained executable.
//
//go:embed public
var staticFiles embed.FS

// Consolidated output topics: {side}-p{pair_id} (e.g. asks-p2). Input topics
// ({side}-p{pair_id}-ex{exchange_id}) carry a trailing -ex... so they don't match.
const topicPattern = `^(asks|bids)-p[0-9]+$`

// --- Display metadata resolved from postgres ---
// pair_id / exchange_id are the identity; base, quote, name, label are display-only
// (the Flink output no longer carries them). See memory: event-identity-by-ids.

type Market struct {
	ID    int    `json:"id"`
	Base  string `json:"base"`
	Quote string `json:"quote"`
}

type Exchange struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Label string `json:"label"`
}

// --- Raw consolidated book as produced by the Flink job ---

type rawLevel struct {
	ExchangeID int    `json:"exchange_id"`
	Price      string `json:"price"`
	Quantity   string `json:"quantity"`
}

type rawBook struct {
	PairID    int        `json:"pair_id"`
	Side      string     `json:"side"`
	Levels    []rawLevel `json:"levels"`
	EventTime int64      `json:"event_time"`
}

// --- Enriched book pushed to the browser (display fields resolved) ---

type level struct {
	Price    string   `json:"price"`
	Quantity string   `json:"quantity"`
	Exchange Exchange `json:"exchange"`
}

type book struct {
	PairID    int     `json:"pair_id"`
	Base      string  `json:"base"`
	Quote     string  `json:"quote"`
	Side      string  `json:"side"`
	Levels    []level `json:"levels"`
	EventTime int64   `json:"event_time"`
}

// registry holds the id -> display maps, refreshed periodically from postgres.
type registry struct {
	mu        sync.RWMutex
	markets   map[int]Market
	exchanges map[int]Exchange
}

func (r *registry) refresh(ctx context.Context, pool *pgxpool.Pool) {
	markets := map[int]Market{}
	if rows, err := pool.Query(ctx, "SELECT m.id, b.name, q.name FROM markets m JOIN currencies b ON m.base_id = b.id JOIN currencies q ON m.quote_id = q.id"); err != nil {
		log.Printf("markets query error: %v", err)
	} else {
		for rows.Next() {
			var m Market
			if err := rows.Scan(&m.ID, &m.Base, &m.Quote); err == nil {
				markets[m.ID] = m
			}
		}
		rows.Close()
	}

	exchanges := map[int]Exchange{}
	if rows, err := pool.Query(ctx, "SELECT id, name, label FROM exchanges"); err != nil {
		log.Printf("exchanges query error: %v", err)
	} else {
		for rows.Next() {
			var e Exchange
			if err := rows.Scan(&e.ID, &e.Name, &e.Label); err == nil {
				exchanges[e.ID] = e
			}
		}
		rows.Close()
	}

	// Only replace a map if the load returned something, so a transient DB error
	// doesn't blank out display data we already have.
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(markets) > 0 {
		r.markets = markets
	}
	if len(exchanges) > 0 {
		r.exchanges = exchanges
	}
}

func (r *registry) enrich(rb rawBook) book {
	r.mu.RLock()
	defer r.mu.RUnlock()

	m, ok := r.markets[rb.PairID]
	if !ok {
		m = Market{ID: rb.PairID, Base: "p" + strconv.Itoa(rb.PairID), Quote: "?"}
	}

	levels := make([]level, 0, len(rb.Levels))
	for _, rl := range rb.Levels {
		ex, ok := r.exchanges[rl.ExchangeID]
		if !ok {
			ex = Exchange{ID: rl.ExchangeID, Name: "unknown", Label: "نامشخص"}
		}
		levels = append(levels, level{Price: rl.Price, Quantity: rl.Quantity, Exchange: ex})
	}

	return book{
		PairID:    rb.PairID,
		Base:      m.Base,
		Quote:     m.Quote,
		Side:      rb.Side,
		Levels:    levels,
		EventTime: rb.EventTime,
	}
}

// hub fans the latest book per topic out to all connected browsers.
type hub struct {
	mu      sync.Mutex
	clients map[*websocket.Conn]bool
	latest  map[string]book
}

type wsSnapshot struct {
	Type  string `json:"type"`
	Books []book `json:"books"`
}

type wsUpdate struct {
	Type string `json:"type"`
	Book book   `json:"book"`
}

// add registers a client and immediately sends everything we have so it renders at once.
func (h *hub) add(conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[conn] = true
	books := make([]book, 0, len(h.latest))
	for _, b := range h.latest {
		books = append(books, b)
	}
	_ = conn.WriteJSON(wsSnapshot{Type: "snapshot", Books: books})
}

func (h *hub) remove(conn *websocket.Conn) {
	h.mu.Lock()
	delete(h.clients, conn)
	h.mu.Unlock()
	conn.Close()
}

// publish stores the latest book for a topic and broadcasts it to every client.
func (h *hub) publish(topic string, b book) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.latest[topic] = b
	msg := wsUpdate{Type: "update", Book: b}
	for c := range h.clients {
		if err := c.WriteJSON(msg); err != nil {
			c.Close()
			delete(h.clients, c)
		}
	}
}

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

func (h *hub) serveWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	h.add(conn)
	// Block on reads only to detect the close; the browser never sends anything.
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			break
		}
	}
	h.remove(conn)
}

func consume(ctx context.Context, broker string, reg *registry, h *hub) {
	// Fresh group each start + reset-to-start so the current book replays on load (dev only).
	group := "orderbook-web-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeRegex(),
		kgo.ConsumeTopics(topicPattern),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Printf("Kafka consumer error (UI stays up; ensure broker at %s is reachable): %v", broker, err)
		return
	}
	defer cl.Close()

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			log.Printf("Kafka fetch error %s[%d]: %v", t, p, err)
		})
		fetches.EachRecord(func(rec *kgo.Record) {
			var rb rawBook
			if err := json.Unmarshal(rec.Value, &rb); err != nil {
				log.Printf("Skipping bad message on %s: %v", rec.Topic, err)
				return
			}
			h.publish(rec.Topic, reg.enrich(rb))
		})
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	ctx := context.Background()

	port := env("PORT", "3000")
	broker := env("KAFKA_BROKER", "localhost:9092")
	dbURL := env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/markets")

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("postgres pool: %v", err)
	}
	defer pool.Close()

	reg := &registry{markets: map[int]Market{}, exchanges: map[int]Exchange{}}
	reg.refresh(ctx, pool) // initial load before anything uses the maps
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for range t.C {
			reg.refresh(ctx, pool)
		}
	}()

	h := &hub{clients: map[*websocket.Conn]bool{}, latest: map[string]book{}}

	publicFS, err := fs.Sub(staticFiles, "public")
	if err != nil {
		log.Fatalf("embedded ui: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(publicFS)))
	mux.HandleFunc("/ws", h.serveWS)

	go consume(ctx, broker, reg, h)

	// Serve the UI immediately so the page loads even before (or without) Kafka.
	log.Printf("Order book UI:    http://localhost:%s", port)
	log.Printf("Kafka broker:     %s", broker)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}
