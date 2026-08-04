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
	// ID and SourceIDs are record lineage. They are NOT part of what a
	// scenario declares as wanted: the ids are fresh uuids on every run, so
	// there is nothing stable to write down. The harness checks their shape and
	// their cross-topic relationships instead, then clears them before the
	// literal comparison — see scenario/lineage.go.
	//
	// swaggerignore keeps them out of the HTTP contract for that reason: a
	// posted scenario that named one would have it silently ignored, so the
	// spec must not offer it as something to fill in.
	ID              string           `json:"id" swaggerignore:"true"`
	SourceIDs       []string         `json:"source_ids" swaggerignore:"true"`
	EventTime       string           `json:"event_time"`
	Asks            []PriceLevel     `json:"asks"`
	Bids            []PriceLevel     `json:"bids"`
	PipelineTimings *PipelineTimings `json:"pipeline_timings"`
}

type PriceLevel struct {
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
}

// AggregatedSide is one record on an aggregated output topic `p{pair_id}-{side}`
// — the terminal aggregator's output and the frozen contract the web app reads.
// Each side is its own topic and its own record, so a book is two of these.
type AggregatedSide struct {
	PairID int64  `json:"pair_id"`
	Side   string `json:"side"` // "asks" or "bids"
	// ID is this record's own lineage id. There is no SourceIDs counterpart:
	// the union mixes exchanges, so a level's parent belongs on the level.
	ID     string            `json:"id"`
	Levels []AggregatedLevel `json:"levels"`
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

// PipelineTimings keeps the Avro union wrappers the payload carries: the record
// sits under its own name, and every timestamp under "long".
type PipelineTimings struct {
	PipelineTimings StepTimings `json:"PipelineTimings"`
}

// StepTimings is the record itself: each job fills its own in/out pair.
type StepTimings struct {
	PairExtractIn   AvroTime `json:"pair_extract_in"`
	PairExtractOut  AvroTime `json:"pair_extract_out"`
	TypeValidateIn  AvroTime `json:"type_validate_in"`
	TypeValidateOut AvroTime `json:"type_validate_out"`
	RebaseIn        AvroTime `json:"rebase_in"`
	RebaseOut       AvroTime `json:"rebase_out"`
	PrecisionIn     AvroTime `json:"precision_in"`
	PrecisionOut    AvroTime `json:"precision_out"`
	BookBuildIn     AvroTime `json:"book_build_in"`
	BookBuildOut    AvroTime `json:"book_build_out"`
}

type AvroTime struct {
	Long string `json:"long"`
}
