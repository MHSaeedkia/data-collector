// Package schema decodes Kafka record values written in Confluent's Avro
// wire format (magic byte + big-endian schema-registry id + Avro binary),
// resolving each record's writer schema from the registry by id.
package schema

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/hamba/avro/v2"

	"orderbook-web/internal/domain"
)

const magicByte = 0x0

// The two record types this app consumes, by Avro full name. Dispatching
// on the writer schema's name rather than on the topic keeps topic
// strings opaque past the consumer — and the name is the thing that
// actually determines the payload shape.
const (
	aggregatedSchemaName = "io.tibobit.orderbook.AggregatedOrderBookEvent"
	snapshotSchemaName   = "io.tibobit.orderbook.OrderBookSnapshot"
)

// wireLevel/wireEvent mirror aggregated_order_book_event.avsc for
// decoding. EventTime is time.Time because hamba/avro maps the
// timestamp-millis logical type to time.Time, not int64.
type wireLevel struct {
	ExchangeID int    `avro:"exchange_id"`
	Simulation int    `avro:"simulation"`
	SourceID   string `avro:"source_id"`
	Price      string `avro:"price"`
	Quantity   string `avro:"quantity"`
}

type wireEvent struct {
	PairID    int         `avro:"pair_id"`
	Side      string      `avro:"side"`
	ID        string      `avro:"id"`
	EventTime time.Time   `avro:"event_time"`
	Levels    []wireLevel `avro:"levels"`
}

// wireSnapLevel/wireSnapshot mirror order_book_snapshot.avsc (job 5).
// Fields the UI has no use for (trigger_id, last_sequence_id,
// pipeline_timings) are simply absent — hamba skips schema fields with no
// struct counterpart.
type wireSnapLevel struct {
	Price    string `avro:"price"`
	Quantity string `avro:"quantity"`
	SourceID string `avro:"source_id"`
}

type wireSnapshot struct {
	ExchangeID int             `avro:"exchange_id"`
	PairID     int             `avro:"pair_id"`
	Simulation int             `avro:"simulation"`
	ID         string          `avro:"id"`
	EventTime  time.Time       `avro:"event_time"`
	Asks       []wireSnapLevel `avro:"asks"`
	Bids       []wireSnapLevel `avro:"bids"`
}

// Decoder decodes Confluent-wire-format Avro records, caching each
// resolved schema by id (schema-registry ids are immutable, so entries
// never need to expire).
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

// Decode parses the Confluent wire header, resolves the writer schema by
// id, and decodes the Avro payload into books. An aggregated record
// yields one book; a job-5 snapshot holds both sides in one record and
// yields two.
func (d *Decoder) Decode(value []byte) ([]domain.RawBook, error) {
	if len(value) < 5 || value[0] != magicByte {
		return nil, fmt.Errorf("not Confluent wire-format Avro: missing magic byte")
	}
	id := binary.BigEndian.Uint32(value[1:5])

	sch, err := d.schemaByID(id)
	if err != nil {
		return nil, fmt.Errorf("resolve schema %d: %w", id, err)
	}

	named, ok := sch.(avro.NamedSchema)
	if !ok {
		return nil, fmt.Errorf("schema %d is not a named record", id)
	}

	switch named.FullName() {
	case snapshotSchemaName:
		return decodeSnapshot(sch, value[5:])
	case aggregatedSchemaName:
		return decodeAggregated(sch, value[5:])
	default:
		return nil, fmt.Errorf("unexpected schema %q", named.FullName())
	}
}

func decodeAggregated(sch avro.Schema, payload []byte) ([]domain.RawBook, error) {
	var we wireEvent
	if err := avro.Unmarshal(sch, payload, &we); err != nil {
		return nil, fmt.Errorf("decode avro payload: %w", err)
	}

	levels := make([]domain.RawLevel, len(we.Levels))
	for i, l := range we.Levels {
		levels[i] = domain.RawLevel{
			ExchangeID: l.ExchangeID,
			Simulation: l.Simulation,
			SourceID:   l.SourceID,
			Price:      l.Price,
			Quantity:   l.Quantity,
		}
	}
	return []domain.RawBook{{
		PairID:    we.PairID,
		Side:      we.Side,
		ID:        we.ID,
		Levels:    levels,
		EventTime: we.EventTime.UnixMilli(),
	}}, nil
}

// decodeSnapshot splits job 5's two-sided record into one book per side
// and pushes its record-level exchange_id/simulation down onto every
// level, so the rest of the app sees the same shape the aggregator
// produces. Both sides are always emitted, including empty ones: an empty
// side is a real report of "no liquidity here" (a reset empties both),
// and dropping it would leave the previous book on screen.
func decodeSnapshot(sch avro.Schema, payload []byte) ([]domain.RawBook, error) {
	var ws wireSnapshot
	if err := avro.Unmarshal(sch, payload, &ws); err != nil {
		return nil, fmt.Errorf("decode avro payload: %w", err)
	}

	book := func(side string, wls []wireSnapLevel) domain.RawBook {
		levels := make([]domain.RawLevel, len(wls))
		for i, l := range wls {
			levels[i] = domain.RawLevel{
				ExchangeID: ws.ExchangeID,
				Simulation: ws.Simulation,
				SourceID:   l.SourceID,
				Price:      l.Price,
				Quantity:   l.Quantity,
			}
		}
		return domain.RawBook{
			PairID:     ws.PairID,
			ExchangeID: ws.ExchangeID,
			Side:       side,
			ID:         ws.ID,
			Levels:     levels,
			EventTime:  ws.EventTime.UnixMilli(),
		}
	}

	return []domain.RawBook{book("asks", ws.Asks), book("bids", ws.Bids)}, nil
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
