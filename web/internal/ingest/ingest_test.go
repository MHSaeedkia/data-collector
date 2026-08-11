package ingest

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"orderbook-web/internal/domain"
)

type fakeDecoder struct {
	toReturn []domain.RawBook
	err      error
}

func (f *fakeDecoder) Decode(value []byte) ([]domain.RawBook, error) {
	return f.toReturn, f.err
}

type fakeEnricher struct {
	received []domain.RawBook
}

func (f *fakeEnricher) Enrich(rb domain.RawBook) domain.Book {
	f.received = append(f.received, rb)
	return domain.Book{PairID: rb.PairID, Side: rb.Side, Base: "BTC"}
}

type fakePublisher struct {
	books []domain.Book
}

func (f *fakePublisher) Publish(b domain.Book) {
	f.books = append(f.books, b)
}

func TestHandleRecord_ValidMessageEnrichesAndPublishes(t *testing.T) {
	dec := &fakeDecoder{toReturn: []domain.RawBook{{
		PairID:    1,
		Side:      "asks",
		EventTime: 123,
		Levels:    []domain.RawLevel{{ExchangeID: 2, Price: "1", Quantity: "2"}},
	}}}
	enricher := &fakeEnricher{}
	pub := &fakePublisher{}

	HandleRecord(dec, enricher, pub, "p1-asks", []byte(`irrelevant, decoder is faked`))

	require.Len(t, enricher.received, 1)
	assert.Equal(t, 1, enricher.received[0].PairID)
	assert.Equal(t, "asks", enricher.received[0].Side)
	require.Len(t, pub.books, 1)
	assert.Equal(t, "BTC", pub.books[0].Base)
}

// A per-exchange snapshot decodes to both sides at once, and each must be
// published separately — the hub keys books per side.
func TestHandleRecord_PublishesEveryBookFromOneRecord(t *testing.T) {
	dec := &fakeDecoder{toReturn: []domain.RawBook{
		{PairID: 1, ExchangeID: 8, Side: "asks"},
		{PairID: 1, ExchangeID: 8, Side: "bids"},
	}}
	pub := &fakePublisher{}

	HandleRecord(dec, &fakeEnricher{}, pub, "ex8-p1-orderbook-snapshot-flink", []byte(`faked`))

	require.Len(t, pub.books, 2)
	assert.Equal(t, "asks", pub.books[0].Side)
	assert.Equal(t, "bids", pub.books[1].Side)
}

func TestHandleRecord_DecodeErrorIsSkipped(t *testing.T) {
	dec := &fakeDecoder{err: errors.New("boom")}
	enricher := &fakeEnricher{}
	pub := &fakePublisher{}

	HandleRecord(dec, enricher, pub, "p1-asks", []byte(`not avro`))

	assert.Empty(t, pub.books, "publish must not be called when decode fails")
}
