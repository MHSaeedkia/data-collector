package ingest

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"orderbook-web/internal/domain"
)

type fakeDecoder struct {
	toReturn domain.RawBook
	err      error
}

func (f *fakeDecoder) Decode(value []byte) (domain.RawBook, error) {
	return f.toReturn, f.err
}

type fakeEnricher struct {
	received domain.RawBook
	toReturn domain.Book
}

func (f *fakeEnricher) Enrich(rb domain.RawBook) domain.Book {
	f.received = rb
	return f.toReturn
}

type fakePublisher struct {
	topic string
	book  domain.Book
	calls int
}

func (f *fakePublisher) Publish(topic string, b domain.Book) {
	f.topic = topic
	f.book = b
	f.calls++
}

func TestHandleRecord_ValidMessageEnrichesAndPublishes(t *testing.T) {
	dec := &fakeDecoder{toReturn: domain.RawBook{
		PairID:    1,
		Side:      "asks",
		EventTime: 123,
		Levels:    []domain.RawLevel{{ExchangeID: 2, Price: "1", Quantity: "2"}},
	}}
	enricher := &fakeEnricher{toReturn: domain.Book{PairID: 1, Base: "BTC"}}
	pub := &fakePublisher{}

	HandleRecord(dec, enricher, pub, "p1-asks", []byte(`irrelevant, decoder is faked`))

	assert.Equal(t, 1, enricher.received.PairID)
	assert.Equal(t, "asks", enricher.received.Side)
	require.Equal(t, 1, pub.calls)
	assert.Equal(t, "p1-asks", pub.topic)
	assert.Equal(t, "BTC", pub.book.Base)
}

func TestHandleRecord_DecodeErrorIsSkipped(t *testing.T) {
	dec := &fakeDecoder{err: errors.New("boom")}
	enricher := &fakeEnricher{}
	pub := &fakePublisher{}

	HandleRecord(dec, enricher, pub, "p1-asks", []byte(`not avro`))

	assert.Equal(t, 0, pub.calls, "publish must not be called when decode fails")
}
