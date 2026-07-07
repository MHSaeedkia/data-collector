// Package ingest wires a raw Kafka record into the registry (enrichment)
// and the hub (broadcast). It is the pure per-record logic that the
// kafka package's poll loop calls into.
package ingest

import (
	"encoding/json"
	"log"

	"orderbook-web/internal/domain"
)

// enricher and publisher narrow *registry.Registry and *hub.Hub down to
// the one method each used here, keeping this package's tests independent
// of those packages' constructors.
type enricher interface {
	Enrich(domain.RawBook) domain.Book
}

type publisher interface {
	Publish(topic string, b domain.Book)
}

// HandleRecord unmarshals a raw consolidated book, enriches it with
// display metadata, and publishes it to the hub. Malformed payloads are
// logged and skipped rather than crashing the consume loop.
func HandleRecord(reg enricher, h publisher, topic string, value []byte) {
	var rb domain.RawBook
	if err := json.Unmarshal(value, &rb); err != nil {
		log.Printf("Skipping bad message on %s: %v", topic, err)
		return
	}
	h.Publish(topic, reg.Enrich(rb))
}
