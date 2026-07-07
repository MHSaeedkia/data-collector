package ingest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"orderbook-web/internal/domain"
)

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
	enricher := &fakeEnricher{toReturn: domain.Book{PairID: 1, Base: "BTC"}}
	pub := &fakePublisher{}
	value := []byte(`{"pair_id":1,"side":"asks","event_time":123,"levels":[{"exchange_id":2,"price":"1","quantity":"2"}]}`)

	HandleRecord(enricher, pub, "asks-p1", value)

	assert.Equal(t, 1, enricher.received.PairID)
	assert.Equal(t, "asks", enricher.received.Side)
	require.Equal(t, 1, pub.calls)
	assert.Equal(t, "asks-p1", pub.topic)
	assert.Equal(t, "BTC", pub.book.Base)
}

func TestHandleRecord_MalformedJSONIsSkipped(t *testing.T) {
	enricher := &fakeEnricher{}
	pub := &fakePublisher{}

	HandleRecord(enricher, pub, "asks-p1", []byte(`not json`))

	assert.Equal(t, 0, pub.calls, "publish must not be called for a malformed message")
}
