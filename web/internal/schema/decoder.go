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

// wireLevel/wireEvent mirror aggregated_order_book_event.avsc for
// decoding. EventTime is time.Time because hamba/avro maps the
// timestamp-millis logical type to time.Time, not int64.
type wireLevel struct {
	ExchangeID int    `avro:"exchange_id"`
	Price      string `avro:"price"`
	Quantity   string `avro:"quantity"`
}

type wireEvent struct {
	PairID    int         `avro:"pair_id"`
	Side      string      `avro:"side"`
	EventTime time.Time   `avro:"event_time"`
	Levels    []wireLevel `avro:"levels"`
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
// id, and decodes the Avro payload into a domain.RawBook.
func (d *Decoder) Decode(value []byte) (domain.RawBook, error) {
	if len(value) < 5 || value[0] != magicByte {
		return domain.RawBook{}, fmt.Errorf("not Confluent wire-format Avro: missing magic byte")
	}
	id := binary.BigEndian.Uint32(value[1:5])

	sch, err := d.schemaByID(id)
	if err != nil {
		return domain.RawBook{}, fmt.Errorf("resolve schema %d: %w", id, err)
	}

	var we wireEvent
	if err := avro.Unmarshal(sch, value[5:], &we); err != nil {
		return domain.RawBook{}, fmt.Errorf("decode avro payload: %w", err)
	}

	levels := make([]domain.RawLevel, len(we.Levels))
	for i, l := range we.Levels {
		levels[i] = domain.RawLevel{ExchangeID: l.ExchangeID, Price: l.Price, Quantity: l.Quantity}
	}
	return domain.RawBook{
		PairID:    we.PairID,
		Side:      we.Side,
		Levels:    levels,
		EventTime: we.EventTime.UnixMilli(),
	}, nil
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
