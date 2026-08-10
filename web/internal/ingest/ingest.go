// Package ingest wires a raw Kafka record into the registry (enrichment)
// and the hub (broadcast). It is the pure per-record logic that the
// kafka package's poll loop calls into.
package ingest

import (
	"log"

	"orderbook-web/internal/domain"
)

// decoder, enricher and publisher narrow *schema.Decoder, *registry.Registry
// and *hub.Hub down to the one method each used here, keeping this
// package's tests independent of those packages' constructors.
type decoder interface {
	Decode(value []byte) ([]domain.RawBook, error)
}

type enricher interface {
	Enrich(domain.RawBook) domain.Book
}

type publisher interface {
	Publish(b domain.Book)
}

// HandleRecord decodes a record, enriches each book it yielded with
// display metadata, and publishes them. One record is one book on the
// aggregated topics and two (asks + bids) on a per-exchange snapshot.
// Malformed payloads are logged and skipped rather than crashing the
// consume loop; topic is used only to say where a bad message came from.
func HandleRecord(dec decoder, reg enricher, h publisher, topic string, value []byte) {
	books, err := dec.Decode(value)
	if err != nil {
		log.Printf("Skipping bad message on %s: %v", topic, err)
		return
	}
	for _, rb := range books {
		h.Publish(reg.Enrich(rb))
	}
}
