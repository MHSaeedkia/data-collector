// Package event decodes the Confluent-wire-format Avro records the normalizer
// pipeline writes, keeping only the parts a latency measurement needs.
//
// One struct covers every record shape on the pipeline. hamba/avro drives
// decoding from the WRITER schema, so a field the schema does not have is left
// at its zero value rather than failing — which is what lets the same Record
// read a job-1 parsed event, a job-6 snapshot and a job-7 aggregated record. The
// levels are never decoded: a book is tens of KB and none of it is a timestamp.
package event

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/hamba/avro/v2"
)

const magicByte = 0x0

// Timings mirrors the PipelineTimings record: one nullable millisecond stamp
// per job per phase. Null means "not yet reached this stage", so a record read
// off an early topic has most of them empty — that is data, not an error.
type Timings struct {
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

// Stage is one job's in/out pair, in pipeline order.
type Stage struct {
	Name string
	In   *time.Time
	Out  *time.Time
}

// Stages returns the six jobs in the order the record passes through them.
// Nil receiver yields nil: a record from a topic with no timings at all.
//
// parse was split out of pair-extract on 2026-09-19. The wait rendered in front
// of pair-extract is therefore the Kafka hop that split introduced — measuring
// it is why the two got their own field pair instead of sharing one.
func (t *Timings) Stages() []Stage {
	if t == nil {
		return nil
	}
	return []Stage{
		{"1 parse", t.ParseIn, t.ParseOut},
		{"2 pair-extract", t.PairExtractIn, t.PairExtractOut},
		{"3 type-validate", t.TypeValidateIn, t.TypeValidateOut},
		{"4 rebase", t.RebaseIn, t.RebaseOut},
		{"5 precision", t.PrecisionIn, t.PrecisionOut},
		{"6 book-build", t.BookBuildIn, t.BookBuildOut},
	}
}

// Record is the superset of the pipeline's record shapes.
//
// EventTime is the pipeline's own clock and is SUBSTITUTED with job 1's
// processing time for the feeds that send none. ExchangeEventTime is the
// exchange's own clock or nil, never substituted — which is why the source and
// end-to-end measurements use it and not EventTime.
//
// MaxEventTime/MinEventTime replace EventTime on the terminal p{id}-{side}
// records, where one record unions several exchanges at several times.
type Record struct {
	ExchangeID int `avro:"exchange_id"`
	PairID     int `avro:"pair_id"`
	// EventTime and MaxEventTime are REQUIRED where they appear, so they are
	// values, not pointers — hamba rejects a pointer for a non-union field. A
	// record whose schema lacks one leaves it at the zero time; Optional turns
	// that back into the nil these are rendered as.
	EventTime         time.Time  `avro:"event_time"`
	MaxEventTime      time.Time  `avro:"max_event_time"`
	ExchangeEventTime *time.Time `avro:"exchange_event_time"`
	MinEventTime      *time.Time `avro:"min_event_time"`
	Timings           *Timings   `avro:"pipeline_timings"`
}

// Optional maps the zero time — "this schema had no such field" — onto nil, so
// a missing field and a null field are handled the same way downstream.
func Optional(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// Decoder resolves writer schemas by the id in the Confluent wire header and
// caches them forever — registry ids are immutable, so nothing can go stale.
type Decoder struct {
	registryURL string
	client      *http.Client

	mu      sync.Mutex
	schemas map[uint32]avro.Schema
}

func NewDecoder(registryURL string) *Decoder {
	return &Decoder{
		registryURL: registryURL,
		client:      &http.Client{Timeout: 10 * time.Second},
		schemas:     make(map[uint32]avro.Schema),
	}
}

// Decode reads one Kafka value. The record name is returned alongside so the
// output can say which shape it read.
func (d *Decoder) Decode(value []byte) (Record, string, error) {
	if len(value) < 5 || value[0] != magicByte {
		return Record{}, "", fmt.Errorf("not Confluent wire-format Avro (%d bytes)", len(value))
	}
	id := binary.BigEndian.Uint32(value[1:5])

	sch, err := d.schemaByID(id)
	if err != nil {
		return Record{}, "", fmt.Errorf("resolve schema %d: %w", id, err)
	}

	name := ""
	if named, ok := sch.(avro.NamedSchema); ok {
		name = named.Name()
	}

	var rec Record
	if err := avro.Unmarshal(sch, value[5:], &rec); err != nil {
		return Record{}, name, fmt.Errorf("decode %s: %w", name, err)
	}
	return rec, name, nil
}

func (d *Decoder) schemaByID(id uint32) (avro.Schema, error) {
	d.mu.Lock()
	sch, ok := d.schemas[id]
	d.mu.Unlock()
	if ok {
		return sch, nil
	}

	resp, err := d.client.Get(fmt.Sprintf("%s/schemas/ids/%d", d.registryURL, id))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("schema registry returned %s", resp.Status)
	}

	var body struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}

	sch, err = avro.Parse(body.Schema)
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	d.schemas[id] = sch
	d.mu.Unlock()
	return sch, nil
}
