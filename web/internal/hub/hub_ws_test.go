package hub

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"orderbook-web/internal/domain"
)

// The tests above drive the hub through a fake conn. These drive it
// through a real websocket, which is the only way to check the parts that
// live on the concrete *websocket.Conn: the read deadline, the pong
// handler that keeps pushing it out, and the fact that *websocket.Conn
// really does satisfy the conn interface the writer goroutine needs.

func dial(t *testing.T, h *Hub) (*websocket.Conn, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(h.ServeWS))
	t.Cleanup(srv.Close)

	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"/ws", nil)
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	return c, srv
}

// readFrame reads one server message and reports its type plus the raw
// JSON, so a test can assert on the envelope the browser actually parses.
func readFrame(t *testing.T, c *websocket.Conn) (string, []byte) {
	t.Helper()
	require.NoError(t, c.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, data, err := c.ReadMessage()
	require.NoError(t, err)
	var env struct {
		Type string `json:"type"`
	}
	require.NoError(t, json.Unmarshal(data, &env))
	return env.Type, data
}

func TestServeWS_CatalogThenSnapshotThenUpdate(t *testing.T) {
	h := New()
	h.SetCatalog(domain.Catalog{
		Markets:   []domain.Market{{ID: 1, Base: "BTC", Quote: "USDT"}},
		Exchanges: []domain.Exchange{{ID: 8, Name: "okx"}},
	})
	held := aggregatedBook(1, "bids")
	held.EventTime = 7
	h.latest[held.Key()] = held

	c, _ := dial(t, h)

	typ, raw := readFrame(t, c)
	assert.Equal(t, "catalog", typ, "the first frame a browser gets is the catalog")
	assert.Contains(t, string(raw), "BTC")

	require.NoError(t, c.WriteJSON(domain.WSSelect{Type: "select", PairID: 1, ExchangeID: domain.AggregatedExchangeID}))

	typ, raw = readFrame(t, c)
	require.Equal(t, "snapshot", typ, "selecting answers immediately with what the hub already holds")
	var snap domain.WSSnapshot
	require.NoError(t, json.Unmarshal(raw, &snap))
	require.Len(t, snap.Books, 1)
	assert.Equal(t, int64(7), snap.Books[0].EventTime)

	live := aggregatedBook(1, "asks")
	live.EventTime = 99
	h.Publish(live)

	typ, raw = readFrame(t, c)
	require.Equal(t, "update", typ)
	var upd domain.WSUpdate
	require.NoError(t, json.Unmarshal(raw, &upd))
	assert.Equal(t, int64(99), upd.Book.EventTime)
}

// A browser that goes away without a close frame — the half-open socket
// that used to sit in the client set forever — is detected by the ping
// going unanswered and the read deadline then expiring.
func TestServeWS_PingsTheBrowserAndKeepsAnsweringClientsAlive(t *testing.T) {
	restore := pingPeriod
	pingPeriod = 20 * time.Millisecond
	defer func() { pingPeriod = restore }()

	h := New()
	c, _ := dial(t, h)

	pinged := make(chan struct{}, 8)
	c.SetPingHandler(func(data string) error {
		select {
		case pinged <- struct{}{}:
		default:
		}
		return c.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(time.Second))
	})

	// Reading is what runs the ping handler, and each pong pushes the
	// server's read deadline out.
	go func() {
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}()

	for i := 0; i < 3; i++ {
		select {
		case <-pinged:
		case <-time.After(3 * time.Second):
			t.Fatal("server stopped pinging an idle browser")
		}
	}
	assert.Equal(t, 1, clientCount(h), "a browser that answers pings must stay connected")
}

func TestServeWS_RemovesTheClientWhenTheBrowserDisconnects(t *testing.T) {
	h := New()
	c, _ := dial(t, h)
	readFrame(t, c) // the catalog, so we know the client is fully registered

	require.NoError(t, c.Close())

	require.Eventually(t, func() bool { return clientCount(h) == 0 },
		3*time.Second, time.Millisecond, "a disconnected browser must leave the client set")
}

// Junk on the socket is ignored, not fatal: dropping the connection over
// one unparsable frame would take the book down with it.
func TestServeWS_IgnoresUnknownMessagesWithoutDroppingTheClient(t *testing.T) {
	h := New()
	c, _ := dial(t, h)
	readFrame(t, c)

	require.NoError(t, c.WriteMessage(websocket.TextMessage, []byte("not json at all")))
	require.NoError(t, c.WriteJSON(map[string]any{"type": "something-else"}))
	require.NoError(t, c.WriteJSON(domain.WSSelect{Type: "select", PairID: 1}))

	typ, _ := readFrame(t, c)
	assert.Equal(t, "snapshot", typ, "the select after the junk is still served")
}
