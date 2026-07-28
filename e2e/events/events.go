// Package events mirrors the Avro records the normalizer pipeline carries on
// its topics, as they are encoded in JSON.
package events

type OrderbookSnapshot struct {
	ExchangeID      int64            `json:"exchange_id"`
	PairID          int64            `json:"pair_id"`
	EventTime       string           `json:"event_time"`
	Asks            []PriceLevel     `json:"asks"`
	Bids            []PriceLevel     `json:"bids"`
	PipelineTimings *PipelineTimings `json:"pipeline_timings"`
}

type PriceLevel struct {
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
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
