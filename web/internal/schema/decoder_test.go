package schema

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// aggregatedOrderBookEventSchema mirrors
// schemas/aggregated_order_book_event.avsc — the wire shape the Flink
// aggregator publishes and this package decodes.
const aggregatedOrderBookEventSchema = `{
	"type": "record",
	"name": "AggregatedOrderBookEvent",
	"namespace": "io.tibobit.orderbook",
	"fields": [
		{"name": "pair_id", "type": "int"},
		{"name": "side", "type": {"type": "enum", "name": "Side", "symbols": ["asks", "bids"]}},
		{"name": "sink_id", "type": "string", "default": ""},
		{"name": "event_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
		{"name": "levels", "type": {"type": "array", "items": {
			"type": "record",
			"name": "AggregatedLevel",
			"fields": [
				{"name": "exchange_id", "type": "int"},
				{"name": "simulation", "type": "int", "default": 0},
				{"name": "source_id", "type": "string", "default": ""},
				{"name": "price", "type": "string"},
				{"name": "quantity", "type": "string"}
			]
		}}}
	]
}`

// wireMessage builds a Confluent-wire-format Avro record: magic byte +
// big-endian schema id + Avro binary payload.
func wireMessage(t *testing.T, schemaID uint32, we wireEvent) []byte {
	t.Helper()
	sch, err := avro.Parse(aggregatedOrderBookEventSchema)
	require.NoError(t, err)
	payload, err := avro.Marshal(sch, we)
	require.NoError(t, err)

	header := make([]byte, 5)
	header[0] = magicByte
	binary.BigEndian.PutUint32(header[1:], schemaID)
	return append(header, payload...)
}

func newTestRegistry(t *testing.T, schemaID uint32) (url string, requests *int) {
	t.Helper()
	requests = new(int)
	wantPath := fmt.Sprintf("/schemas/ids/%d", schemaID)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*requests++
		assert.Equal(t, wantPath, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"schema": ` + mustQuoteJSON(aggregatedOrderBookEventSchema) + `}`))
	}))
	t.Cleanup(srv.Close)
	return srv.URL, requests
}

func TestDecoder_Decode_ValidMessageAndCachesSchema(t *testing.T) {
	registryURL, requests := newTestRegistry(t, 2)
	dec := NewDecoder(registryURL)

	value := wireMessage(t, 2, wireEvent{
		PairID:    1,
		Side:      "asks",
		SinkID:    "77777777-7777-4777-8777-777777777777",
		EventTime: time.UnixMilli(1_700_000_000_000).UTC(),
		Levels: []wireLevel{{
			ExchangeID: 3,
			Simulation: 1,
			SourceID:   "66666666-6666-4666-8666-666666666666",
			Price:      "97240.50",
			Quantity:   "0.42",
		}},
	})

	rb, err := dec.Decode(value)
	require.NoError(t, err)
	assert.Equal(t, 1, rb.PairID)
	assert.Equal(t, "asks", rb.Side)
	assert.Equal(t, "77777777-7777-4777-8777-777777777777", rb.SinkID)
	assert.Equal(t, int64(1_700_000_000_000), rb.EventTime)
	require.Len(t, rb.Levels, 1)
	assert.Equal(t, 3, rb.Levels[0].ExchangeID)
	assert.Equal(t, 1, rb.Levels[0].Simulation)
	assert.Equal(t, "66666666-6666-4666-8666-666666666666", rb.Levels[0].SourceID)
	assert.Equal(t, "97240.50", rb.Levels[0].Price)
	assert.Equal(t, "0.42", rb.Levels[0].Quantity)

	_, err = dec.Decode(value)
	require.NoError(t, err)
	assert.Equal(t, 1, *requests, "schema id 2 must be fetched once and cached, not refetched per message")
}

func TestDecoder_Decode_MissingMagicByteIsRejected(t *testing.T) {
	dec := NewDecoder("http://unused.invalid")

	_, err := dec.Decode([]byte{0x1, 0x0, 0x0, 0x0, 0x2, 0xAA})

	assert.Error(t, err)
}

func TestDecoder_Decode_TooShortIsRejected(t *testing.T) {
	dec := NewDecoder("http://unused.invalid")

	_, err := dec.Decode([]byte{0x0, 0x0})

	assert.Error(t, err)
}

func TestDecoder_Decode_SchemaRegistryUnreachableReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	dec := NewDecoder(srv.URL)

	header := make([]byte, 5)
	header[0] = magicByte
	binary.BigEndian.PutUint32(header[1:], 99)

	_, err := dec.Decode(header)

	assert.Error(t, err)
}

// The aggregator unions across exchanges, so one record can carry levels from a
// live exchange and a simulated one at the same time. The flag must stay
// attached to the level it arrived on, never collapse to a per-record value.
func TestDecoder_Decode_SimulationIsPerLevel(t *testing.T) {
	registryURL, _ := newTestRegistry(t, 2)
	dec := NewDecoder(registryURL)

	value := wireMessage(t, 2, wireEvent{
		PairID:    1,
		Side:      "asks",
		EventTime: time.UnixMilli(1_700_000_000_000).UTC(),
		Levels: []wireLevel{
			{ExchangeID: 3, Simulation: 0, Price: "100", Quantity: "1"},
			{ExchangeID: 6, Simulation: 1, Price: "100", Quantity: "2"},
		},
	})

	rb, err := dec.Decode(value)
	require.NoError(t, err)
	require.Len(t, rb.Levels, 2)
	assert.Equal(t, 0, rb.Levels[0].Simulation)
	assert.Equal(t, 1, rb.Levels[1].Simulation)
}

// source_id is per level for exactly the same reason simulation is: the levels
// of one record come from different exchanges, so they come from different
// job-5 snapshots. Two levels at the same price must keep different parents.
func TestDecoder_Decode_SourceIDIsPerLevel(t *testing.T) {
	registryURL, _ := newTestRegistry(t, 2)
	dec := NewDecoder(registryURL)

	value := wireMessage(t, 2, wireEvent{
		PairID:    1,
		Side:      "asks",
		SinkID:    "77777777-7777-4777-8777-777777777777",
		EventTime: time.UnixMilli(1_700_000_000_000).UTC(),
		Levels: []wireLevel{
			{ExchangeID: 3, SourceID: "snapshot-ex3", Price: "100", Quantity: "1"},
			{ExchangeID: 6, SourceID: "snapshot-ex6", Price: "100", Quantity: "2"},
		},
	})

	rb, err := dec.Decode(value)
	require.NoError(t, err)
	require.Len(t, rb.Levels, 2)
	assert.Equal(t, "snapshot-ex3", rb.Levels[0].SourceID)
	assert.Equal(t, "snapshot-ex6", rb.Levels[1].SourceID)
	assert.Equal(t, "77777777-7777-4777-8777-777777777777", rb.SinkID)
}

func mustQuoteJSON(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}
