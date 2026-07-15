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

// consolidatedOrderBookEventSchema mirrors
// schemas/consolidated_order_book_event.avsc — the wire shape the Flink
// consolidator publishes and this package decodes.
const consolidatedOrderBookEventSchema = `{
	"type": "record",
	"name": "ConsolidatedOrderBookEvent",
	"namespace": "io.tibobit.orderbook",
	"fields": [
		{"name": "pair_id", "type": "int"},
		{"name": "side", "type": {"type": "enum", "name": "Side", "symbols": ["asks", "bids"]}},
		{"name": "event_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
		{"name": "levels", "type": {"type": "array", "items": {
			"type": "record",
			"name": "ConsolidatedLevel",
			"fields": [
				{"name": "exchange_id", "type": "int"},
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
	sch, err := avro.Parse(consolidatedOrderBookEventSchema)
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
		w.Write([]byte(`{"schema": ` + mustQuoteJSON(consolidatedOrderBookEventSchema) + `}`))
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
		EventTime: time.UnixMilli(1_700_000_000_000).UTC(),
		Levels:    []wireLevel{{ExchangeID: 3, Price: "97240.50", Quantity: "0.42"}},
	})

	rb, err := dec.Decode(value)
	require.NoError(t, err)
	assert.Equal(t, 1, rb.PairID)
	assert.Equal(t, "asks", rb.Side)
	assert.Equal(t, int64(1_700_000_000_000), rb.EventTime)
	require.Len(t, rb.Levels, 1)
	assert.Equal(t, 3, rb.Levels[0].ExchangeID)
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

func mustQuoteJSON(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}
