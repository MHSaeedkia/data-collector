// Package events mirrors the Avro records the normalizer pipeline carries on
// its topics, as they are encoded in JSON.
package events

type OrderbookSnapshot struct {
	ExchangeID int64 `json:"exchange_id"`
	PairID     int64 `json:"pair_id"`
	// Simulation is NiFi's flag carried up the pipeline: 0 = live data, 1 =
	// simulation data, other values not yet defined. The book builder stamps
	// the emitted book with the flag of the event that produced it.
	Simulation int64 `json:"simulation"`
	// ID and TriggerID are record lineage: this record's own id, and the job-4
	// event that caused job 5 to emit it. TriggerID is the record's ONLY
	// record-level parent — the rest of the fan-in is per level, on PriceLevel.
	// It is not necessarily one of those level ids: a delete-only event, or a
	// reset that empties the book, leaves nothing resting.
	//
	// Neither is part of what a scenario declares as wanted: the ids are fresh
	// uuids on every run, so there is nothing stable to write down. The harness
	// checks their shape and their relationships instead, then clears them
	// before the literal comparison — see scenario/lineage.go.
	//
	// swaggerignore keeps them out of the HTTP contract for that reason: a
	// posted scenario that named one would have it silently ignored, so the
	// spec must not offer it as something to fill in.
	ID        string `json:"id" swaggerignore:"true"`
	TriggerID string `json:"trigger_id" swaggerignore:"true"`
	EventTime string `json:"event_time"`
	// LastSequenceID is the event's sequence_id passed through, null for feeds
	// with no ordering field (ex3). Mirrored so this struct matches the schema
	// field for field, but the harness does NOT read it: the snapshot stream is
	// compared with DeepEqual, so decoding it would mean every wanted snapshot in
	// the suite had to spell it out. swaggerignore for the same reason as the
	// lineage fields — a posted scenario naming it would have it ignored.
	LastSequenceID  *int64           `json:"last_sequence_id" swaggerignore:"true"`
	Asks            []PriceLevel     `json:"asks"`
	Bids            []PriceLevel     `json:"bids"`
	PipelineTimings *PipelineTimings `json:"pipeline_timings" swaggerignore:"true"`
}

type PriceLevel struct {
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
	// SourceID is the job-4 event that last SET this level — the per-level half of
	// job 5's lineage, and what makes ONE price traceable back to its raw event.
	// Like the record-level lineage it is a fresh uuid every run, so a scenario
	// never declares it: checked structurally, then cleared before the literal
	// comparison. swaggerignore for the same reason as ID and TriggerID above.
	SourceID string `json:"source_id" swaggerignore:"true"`
}

// AggregatedSide is one record on an aggregated output topic `p{pair_id}-{side}`
// — the terminal aggregator's output and the frozen contract the web app reads.
// Each side is its own topic and its own record, so a book is two of these.
type AggregatedSide struct {
	PairID int64  `json:"pair_id"`
	Side   string `json:"side"` // "asks" or "bids"
	// ID is this record's own lineage id. There is no record-level parent here:
	// the union mixes exchanges, so a level's parent belongs on the level.
	ID string `json:"id"`
	// EventTime is the max event time of the exchanges in the union. Read, but
	// nothing asserts it: for ex3 it is job 1's processing time, so it is not
	// comparable across exchanges. The levels are what a scenario checks.
	EventTime string            `json:"event_time"`
	Levels    []AggregatedLevel `json:"levels"`
}

// AggregatedLevel is one level of the aggregated book. Levels from different
// exchanges are unioned, never summed, so each one stays tagged with the
// exchange it came from even when two exchanges quote the same price.
//
// Simulation is tagged per level for the same reason ExchangeID is: one
// aggregated record mixes exchanges, so the flag only means something attached
// to the level it came with, never to the record as a whole.
// SourceID is per level for the same reason: it is the id of the job-5
// snapshot the level came from, and one record's levels come from several
// snapshots. Like the snapshot's lineage it is checked structurally and then
// cleared before comparison, never declared by a scenario.
type AggregatedLevel struct {
	ExchangeID int64  `json:"exchange_id"`
	Simulation int64  `json:"simulation"`
	SourceID   string `json:"source_id" swaggerignore:"true"`
	Price      string `json:"price"`
	Quantity   string `json:"quantity"`
}

// PipelineTimings is the per-step latency record, mirroring the schema field for
// field: each job fills its own in/out pair in epoch millis, and nil means "not
// yet reached".
//
// The harness never reads it — the values are wall-clock, so there is nothing a
// scenario could write down — which is also why it is kept out of the HTTP
// contract. It is modelled here because this package mirrors the records on the
// topics, not only the parts that get asserted.
//
// It used to be modelled in goavro's WIRE shape (a union wrapper keyed
// "PipelineTimings", each timestamp under "long", as a string). All three were
// wrong: goavro names a union branch by its FULL name, so the keys are
// "io.tibobit.orderbook.PipelineTimings" and "long.timestamp-millis", and the
// value is a number. Nothing caught it because nothing ever decoded into it.
// Union unwrapping belongs in consumer's wire structs anyway, the way
// event_time's epoch millis do — this package holds the harness's normalized
// view.
type PipelineTimings struct {
	PairExtractIn   *int64 `json:"pair_extract_in"`
	PairExtractOut  *int64 `json:"pair_extract_out"`
	TypeValidateIn  *int64 `json:"type_validate_in"`
	TypeValidateOut *int64 `json:"type_validate_out"`
	RebaseIn        *int64 `json:"rebase_in"`
	RebaseOut       *int64 `json:"rebase_out"`
	PrecisionIn     *int64 `json:"precision_in"`
	PrecisionOut    *int64 `json:"precision_out"`
	BookBuildIn     *int64 `json:"book_build_in"`
	BookBuildOut    *int64 `json:"book_build_out"`
}
