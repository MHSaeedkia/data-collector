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
		{"name": "id", "type": "string", "default": ""},
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

// orderBookSnapshotSchema mirrors schemas/order_book_snapshot.avsc — job
// 5's per-exchange book, both sides in one record. Kept complete
// (trigger_id, last_sequence_id, pipeline_timings included) because the
// decoder deliberately has no struct fields for those, and skipping them
// correctly is part of what these tests check.
const orderBookSnapshotSchema = `{
	"type": "record",
	"name": "OrderBookSnapshot",
	"namespace": "io.tibobit.orderbook",
	"fields": [
		{"name": "exchange_id", "type": "int"},
		{"name": "pair_id", "type": "int"},
		{"name": "simulation", "type": "int", "default": 0},
		{"name": "id", "type": "string", "default": ""},
		{"name": "trigger_id", "type": "string", "default": ""},
		{"name": "event_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
		{"name": "last_sequence_id", "type": ["null", "long"], "default": null},
		{"name": "asks", "type": {"type": "array", "items": {
			"type": "record",
			"name": "PriceLevel",
			"fields": [
				{"name": "price", "type": "string"},
				{"name": "quantity", "type": "string"},
				{"name": "source_id", "type": "string", "default": ""}
			]
		}}},
		{"name": "bids", "type": {"type": "array", "items": "PriceLevel"}},
		{"name": "pipeline_timings", "type": ["null", {
			"type": "record",
			"name": "PipelineTimings",
			"fields": [
				{"name": "book_build_out", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": null}
			]
		}], "default": null}
	]
}`

// fullSnapshot carries the snapshot fields the decoder's wireSnapshot
// leaves out, so a test message can put real values on the wire for them.
type fullSnapshot struct {
	ExchangeID     int             `avro:"exchange_id"`
	PairID         int             `avro:"pair_id"`
	Simulation     int             `avro:"simulation"`
	ID             string          `avro:"id"`
	TriggerID      string          `avro:"trigger_id"`
	EventTime      time.Time       `avro:"event_time"`
	LastSequenceID *int64          `avro:"last_sequence_id"`
	Asks           []wireSnapLevel `avro:"asks"`
	Bids           []wireSnapLevel `avro:"bids"`
	Timings        *fullTimings    `avro:"pipeline_timings"`
}

type fullTimings struct {
	BookBuildOut *time.Time `avro:"book_build_out"`
}

// wireMessage builds a Confluent-wire-format Avro record: magic byte +
// big-endian schema id + Avro binary payload.
func wireMessage(t *testing.T, schemaID uint32, schemaJSON string, v any) []byte {
	t.Helper()
	sch, err := avro.Parse(schemaJSON)
	require.NoError(t, err)
	payload, err := avro.Marshal(sch, v)
	require.NoError(t, err)

	header := make([]byte, 5)
	header[0] = magicByte
	binary.BigEndian.PutUint32(header[1:], schemaID)
	return append(header, payload...)
}

func newTestRegistry(t *testing.T, schemaID uint32, schemaJSON string) (url string, requests *int) {
	t.Helper()
	requests = new(int)
	wantPath := fmt.Sprintf("/schemas/ids/%d", schemaID)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*requests++
		assert.Equal(t, wantPath, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"schema": ` + mustQuoteJSON(schemaJSON) + `}`))
	}))
	t.Cleanup(srv.Close)
	return srv.URL, requests
}

func TestDecoder_Decode_ValidMessageAndCachesSchema(t *testing.T) {
	registryURL, requests := newTestRegistry(t, 2, aggregatedOrderBookEventSchema)
	dec := NewDecoder(registryURL)

	value := wireMessage(t, 2, aggregatedOrderBookEventSchema, wireEvent{
		PairID:    1,
		Side:      "asks",
		ID:        "77777777-7777-4777-8777-777777777777",
		EventTime: time.UnixMilli(1_700_000_000_000).UTC(),
		Levels: []wireLevel{{
			ExchangeID: 3,
			Simulation: 1,
			SourceID:   "66666666-6666-4666-8666-666666666666",
			Price:      "97240.50",
			Quantity:   "0.42",
		}},
	})

	books, err := dec.Decode(value)
	require.NoError(t, err)
	require.Len(t, books, 1, "an aggregated record is one side, so one book")
	rb := books[0]
	assert.Equal(t, 1, rb.PairID)
	assert.Equal(t, 0, rb.ExchangeID, "the aggregated book belongs to no single exchange")
	assert.Equal(t, "asks", rb.Side)
	assert.Equal(t, "77777777-7777-4777-8777-777777777777", rb.ID)
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
	registryURL, _ := newTestRegistry(t, 2, aggregatedOrderBookEventSchema)
	dec := NewDecoder(registryURL)

	value := wireMessage(t, 2, aggregatedOrderBookEventSchema, wireEvent{
		PairID:    1,
		Side:      "asks",
		EventTime: time.UnixMilli(1_700_000_000_000).UTC(),
		Levels: []wireLevel{
			{ExchangeID: 3, Simulation: 0, Price: "100", Quantity: "1"},
			{ExchangeID: 6, Simulation: 1, Price: "100", Quantity: "2"},
		},
	})

	books, err := dec.Decode(value)
	require.NoError(t, err)
	require.Len(t, books[0].Levels, 2)
	assert.Equal(t, 0, books[0].Levels[0].Simulation)
	assert.Equal(t, 1, books[0].Levels[1].Simulation)
}

// source_id is per level for exactly the same reason simulation is: the levels
// of one record come from different exchanges, so they come from different
// job-5 snapshots. Two levels at the same price must keep different parents.
func TestDecoder_Decode_SourceIDIsPerLevel(t *testing.T) {
	registryURL, _ := newTestRegistry(t, 2, aggregatedOrderBookEventSchema)
	dec := NewDecoder(registryURL)

	value := wireMessage(t, 2, aggregatedOrderBookEventSchema, wireEvent{
		PairID:    1,
		Side:      "asks",
		ID:        "77777777-7777-4777-8777-777777777777",
		EventTime: time.UnixMilli(1_700_000_000_000).UTC(),
		Levels: []wireLevel{
			{ExchangeID: 3, SourceID: "snapshot-ex3", Price: "100", Quantity: "1"},
			{ExchangeID: 6, SourceID: "snapshot-ex6", Price: "100", Quantity: "2"},
		},
	})

	books, err := dec.Decode(value)
	require.NoError(t, err)
	rb := books[0]
	require.Len(t, rb.Levels, 2)
	assert.Equal(t, "snapshot-ex3", rb.Levels[0].SourceID)
	assert.Equal(t, "snapshot-ex6", rb.Levels[1].SourceID)
	assert.Equal(t, "77777777-7777-4777-8777-777777777777", rb.ID)
}

// Job 5's record holds both sides at once, so one message becomes two
// books — and its record-level exchange_id/simulation are pushed down
// onto every level, which is what lets the rest of the app treat a
// per-exchange book exactly like an aggregated one.
func TestDecoder_Decode_SnapshotSplitsIntoBothSides(t *testing.T) {
	registryURL, _ := newTestRegistry(t, 9, orderBookSnapshotSchema)
	dec := NewDecoder(registryURL)

	seq := int64(4242)
	built := time.UnixMilli(1_700_000_000_500).UTC()
	value := wireMessage(t, 9, orderBookSnapshotSchema, fullSnapshot{
		ExchangeID:     8,
		PairID:         1,
		Simulation:     1,
		ID:             "55555555-5555-4555-8555-555555555555",
		TriggerID:      "44444444-4444-4444-8444-444444444444",
		EventTime:      time.UnixMilli(1_700_000_000_000).UTC(),
		LastSequenceID: &seq,
		Asks: []wireSnapLevel{
			{Price: "97240.50", Quantity: "0.42", SourceID: "event-a"},
			{Price: "97241.00", Quantity: "1.00", SourceID: "event-b"},
		},
		Bids:    []wireSnapLevel{{Price: "97239.00", Quantity: "2.00", SourceID: "event-c"}},
		Timings: &fullTimings{BookBuildOut: &built},
	})

	books, err := dec.Decode(value)
	require.NoError(t, err)
	require.Len(t, books, 2)

	asks, bids := books[0], books[1]
	assert.Equal(t, "asks", asks.Side)
	assert.Equal(t, "bids", bids.Side)
	for _, b := range books {
		assert.Equal(t, 1, b.PairID)
		assert.Equal(t, 8, b.ExchangeID)
		assert.Equal(t, "55555555-5555-4555-8555-555555555555", b.ID)
		assert.Equal(t, int64(1_700_000_000_000), b.EventTime)
		for _, l := range b.Levels {
			assert.Equal(t, 8, l.ExchangeID, "the record's exchange must reach every level")
			assert.Equal(t, 1, l.Simulation, "the record's simulation flag must reach every level")
		}
	}
	require.Len(t, asks.Levels, 2)
	assert.Equal(t, "97240.50", asks.Levels[0].Price)
	assert.Equal(t, "event-a", asks.Levels[0].SourceID)
	require.Len(t, bids.Levels, 1)
	assert.Equal(t, "97239.00", bids.Levels[0].Price)
}

// A reset empties both sides. The empty side still has to be emitted, or
// the browser would keep rendering the book that is no longer there.
func TestDecoder_Decode_SnapshotEmitsEmptySides(t *testing.T) {
	registryURL, _ := newTestRegistry(t, 9, orderBookSnapshotSchema)
	dec := NewDecoder(registryURL)

	value := wireMessage(t, 9, orderBookSnapshotSchema, fullSnapshot{
		ExchangeID: 8,
		PairID:     1,
		EventTime:  time.UnixMilli(1_700_000_000_000).UTC(),
		Asks:       []wireSnapLevel{},
		Bids:       []wireSnapLevel{},
	})

	books, err := dec.Decode(value)
	require.NoError(t, err)
	require.Len(t, books, 2)
	assert.Empty(t, books[0].Levels)
	assert.Empty(t, books[1].Levels)
}

func TestDecoder_Decode_UnknownSchemaIsRejected(t *testing.T) {
	const other = `{"type": "record", "name": "Whatever", "namespace": "io.tibobit.orderbook", "fields": []}`
	registryURL, _ := newTestRegistry(t, 3, other)
	dec := NewDecoder(registryURL)

	_, err := dec.Decode(wireMessage(t, 3, other, struct{}{}))

	assert.Error(t, err, "a record type this app doesn't consume must not be decoded as one that it does")
}

func mustQuoteJSON(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}
