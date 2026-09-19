package event

import (
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests encode against the REAL schema file rather than a mirror copied
// into this package. A mirror drifts silently; reading ../../../schemas means a
// change to the contract that this decoder cannot follow fails here, which is
// the only place it would be noticed before a live run.
const snapshotSchemaPath = "../../../schemas/order_book_snapshot.avsc"

// writeSnapshot is the FULL record: hamba encodes from the schema, so every
// required field needs a value here even though Record reads only a few.
type writeSnapshot struct {
	ExchangeID        int         `avro:"exchange_id"`
	PairID            int         `avro:"pair_id"`
	Simulation        int         `avro:"simulation"`
	ID                string      `avro:"id"`
	TriggerID         string      `avro:"trigger_id"`
	EventTime         time.Time   `avro:"event_time"`
	ExchangeEventTime *time.Time  `avro:"exchange_event_time"`
	LastSequenceID    *int64      `avro:"last_sequence_id"`
	Asks              []writeLvl  `avro:"asks"`
	Bids              []writeLvl  `avro:"bids"`
	Timings           *writeTimes `avro:"pipeline_timings"`
}

type writeLvl struct {
	Price    string `avro:"price"`
	Quantity string `avro:"quantity"`
	SourceID string `avro:"source_id"`
}

type writeTimes struct {
	ParseIn         *time.Time `avro:"parse_in"`
	ParseOut        *time.Time `avro:"parse_out"`
	PairExtractIn   *time.Time `avro:"pair_extract_in"`
	PairExtractOut  *time.Time `avro:"pair_extract_out"`
	TypeValidateIn  *time.Time `avro:"type_validate_in"`
	TypeValidateOut *time.Time `avro:"type_validate_out"`
	RebaseIn        *time.Time `avro:"rebase_in"`
	RebaseOut       *time.Time `avro:"rebase_out"`
	PrecisionIn     *time.Time `avro:"precision_in"`
	PrecisionOut    *time.Time `avro:"precision_out"`
	BookBuildIn     *time.Time `avro:"book_build_in"`
	BookBuildOut    *time.Time `avro:"book_build_out"`
}

func at(t *testing.T, s string) *time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, s)
	require.NoError(t, err)
	return &parsed
}

// registryServing stands in for the schema registry, answering the one id the
// wire header will name.
func registryServing(t *testing.T, id uint32, schema string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/schemas/ids/"+itoa(id) {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": schema})
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func itoa(id uint32) string {
	return string(rune('0' + id%10)) // ids used in these tests are single digit
}

// wireValue prefixes the Confluent header onto an encoded payload.
func wireValue(t *testing.T, id uint32, sch avro.Schema, v any) []byte {
	t.Helper()
	payload, err := avro.Marshal(sch, v)
	require.NoError(t, err)

	value := make([]byte, 5, 5+len(payload))
	value[0] = magicByte
	binary.BigEndian.PutUint32(value[1:5], id)
	return append(value, payload...)
}

func snapshotSchema(t *testing.T) (string, avro.Schema) {
	t.Helper()
	raw, err := os.ReadFile(snapshotSchemaPath)
	require.NoError(t, err)
	sch, err := avro.Parse(string(raw))
	require.NoError(t, err)
	return string(raw), sch
}

// The captured record the user pasted from ex1-p1-orderbook-snapshot-flink.
func sampleTimings(t *testing.T) *writeTimes {
	return &writeTimes{
		ParseIn:         at(t, "2026-09-14T11:04:26.961Z"),
		ParseOut:        at(t, "2026-09-14T11:04:26.962Z"),
		PairExtractIn:   at(t, "2026-09-14T11:04:26.971Z"),
		PairExtractOut:  at(t, "2026-09-14T11:04:26.972Z"),
		TypeValidateIn:  at(t, "2026-09-14T11:04:28.569Z"),
		TypeValidateOut: at(t, "2026-09-14T11:04:28.569Z"),
		RebaseIn:        at(t, "2026-09-14T11:04:32.572Z"),
		RebaseOut:       at(t, "2026-09-14T11:04:32.572Z"),
		PrecisionIn:     at(t, "2026-09-14T11:04:32.989Z"),
		PrecisionOut:    at(t, "2026-09-14T11:04:32.989Z"),
		BookBuildIn:     at(t, "2026-09-14T11:04:33.214Z"),
		BookBuildOut:    at(t, "2026-09-14T11:04:33.214Z"),
	}
}

func TestDecodeReadsTimingsFromTheRealSchema(t *testing.T) {
	raw, sch := snapshotSchema(t)
	seq := int64(734261)
	value := wireValue(t, 9, sch, writeSnapshot{
		ExchangeID:        1,
		PairID:            1,
		ID:                "85153666-e3bb-40a2-ab50-d27da5f8c53a",
		TriggerID:         "6158d34e-4638-4f24-81b4-4da7322f2e6e",
		EventTime:         *at(t, "2026-09-14T11:04:26.146Z"),
		ExchangeEventTime: at(t, "2026-09-14T11:04:26.146Z"),
		LastSequenceID:    &seq,
		Asks:              []writeLvl{{Price: "77794.98", Quantity: "0.0015", SourceID: "x"}},
		Bids:              []writeLvl{},
		Timings:           sampleTimings(t),
	})

	rec, name, err := NewDecoder(registryServing(t, 9, raw)).Decode(value)

	require.NoError(t, err)
	assert.Equal(t, "OrderBookSnapshot", name)
	assert.Equal(t, 1, rec.ExchangeID)
	assert.Equal(t, 1, rec.PairID)
	require.NotNil(t, rec.ExchangeEventTime)
	assert.Equal(t, "2026-09-14T11:04:26.146Z", rec.ExchangeEventTime.UTC().Format(time.RFC3339Nano))

	stages := rec.Timings.Stages()
	require.Len(t, stages, 6)
	assert.Equal(t, "1 parse", stages[0].Name)
	require.NotNil(t, stages[0].In)
	assert.Equal(t, "2026-09-14T11:04:26.961Z", stages[0].In.UTC().Format(time.RFC3339Nano))
	assert.Equal(t, "2 pair-extract", stages[1].Name)
	require.NotNil(t, stages[1].In)
	assert.Equal(t, "2026-09-14T11:04:26.971Z", stages[1].In.UTC().Format(time.RFC3339Nano))
	require.NotNil(t, stages[5].Out)
	assert.Equal(t, "2026-09-14T11:04:33.214Z", stages[5].Out.UTC().Format(time.RFC3339Nano))
}

// A null exchange clock is the normal state for ex3/ex4 and ex7 updates, so it
// must decode to a nil rather than to the zero time — a zero time would measure
// as a 56-year latency instead of as "unknown".
func TestDecodeKeepsANullExchangeClockNil(t *testing.T) {
	raw, sch := snapshotSchema(t)
	value := wireValue(t, 9, sch, writeSnapshot{
		ExchangeID: 3,
		PairID:     1,
		EventTime:  *at(t, "2026-09-14T11:04:26.146Z"),
		Asks:       []writeLvl{},
		Bids:       []writeLvl{},
		Timings:    sampleTimings(t),
	})

	rec, _, err := NewDecoder(registryServing(t, 9, raw)).Decode(value)

	require.NoError(t, err)
	assert.Nil(t, rec.ExchangeEventTime)
}

// A record that never reached the later jobs carries nulls there. Reading it
// must not fail — the nulls are what the output shows as n/a.
func TestDecodeToleratesPartialTimings(t *testing.T) {
	raw, sch := snapshotSchema(t)
	value := wireValue(t, 9, sch, writeSnapshot{
		ExchangeID: 1,
		PairID:     1,
		EventTime:  *at(t, "2026-09-14T11:04:26.146Z"),
		Asks:       []writeLvl{},
		Bids:       []writeLvl{},
		Timings: &writeTimes{
			PairExtractIn:  at(t, "2026-09-14T11:04:26.971Z"),
			PairExtractOut: at(t, "2026-09-14T11:04:26.972Z"),
		},
	})

	rec, _, err := NewDecoder(registryServing(t, 9, raw)).Decode(value)

	require.NoError(t, err)
	stages := rec.Timings.Stages()
	require.Len(t, stages, 6)
	assert.Nil(t, stages[0].In, "written before the job-1 split, so no parse stamp")
	assert.NotNil(t, stages[1].In)
	assert.Nil(t, stages[2].In)
	assert.Nil(t, stages[5].Out)
}

func TestDecodeRejectsANonAvroValue(t *testing.T) {
	raw, _ := snapshotSchema(t)
	d := NewDecoder(registryServing(t, 9, raw))

	_, _, err := d.Decode([]byte("plain text"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "Confluent wire-format")
}

// Stages on a record from a topic that carries no timings at all (the terminal
// p{id}-{side} family) must be empty, not a panic.
func TestStagesOnNilTimings(t *testing.T) {
	var timings *Timings
	assert.Nil(t, timings.Stages())
}
