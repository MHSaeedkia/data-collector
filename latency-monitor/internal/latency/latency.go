// Package latency turns one decoded record plus its Kafka write time into the
// set of durations that say where the time went.
//
// Every duration is a pointer. A nil is the honest answer to "we cannot know" —
// the exchange sent no clock, or the record never reached that stage — and it
// is deliberately not collapsed into a zero, which would read as "instant" and
// quietly drag any average it lands in towards 0.
package latency

import (
	"time"

	"orderbook-latency/internal/event"
)

// Stage is one job's measured time, plus the wait in front of it.
type Stage struct {
	Name string
	In   *time.Time
	Out  *time.Time
	// Job is out - in: time spent inside the job.
	Job *time.Duration
	// Gap is this job's in minus the PREVIOUS job's out: time the record spent
	// in Kafka between the two. Nil on the first stage, which has no predecessor.
	Gap *time.Duration
}

// Metrics is everything measurable about one record.
type Metrics struct {
	Stages []Stage

	// FirstIn is the earliest stage `in` present, LastOut the latest `out`.
	// They anchor the totals below and are nil when no stage was reached.
	FirstIn *time.Time
	LastOut *time.Time

	// Pipeline is FirstIn → LastOut: the whole journey through the jobs.
	Pipeline *time.Duration
	// Write is LastOut → the Kafka write time of THIS record: how long the
	// final emit took to land on the topic.
	Write *time.Duration
	// Source is the exchange's own clock → FirstIn: everything before Flink —
	// the network from the exchange, NiFi, and the raw topic.
	Source *time.Duration
	// EndToEnd is the exchange's own clock → the Kafka write time. Nil whenever
	// the feed sends no clock of its own (ex3, ex4, ex7 updates).
	EndToEnd *time.Duration
}

// Compute measures one record against the time the broker wrote it.
func Compute(rec event.Record, kafkaWrite time.Time) Metrics {
	stages := rec.Timings.Stages()

	m := Metrics{Stages: make([]Stage, 0, len(stages))}

	var prevOut *time.Time
	for _, s := range stages {
		measured := Stage{Name: s.Name, In: s.In, Out: s.Out}
		measured.Job = between(s.In, s.Out)
		measured.Gap = between(prevOut, s.In)
		m.Stages = append(m.Stages, measured)

		if s.In != nil && m.FirstIn == nil {
			m.FirstIn = s.In
		}
		if s.Out != nil {
			m.LastOut = s.Out
			prevOut = s.Out
		}
	}

	m.Pipeline = between(m.FirstIn, m.LastOut)
	m.Write = between(m.LastOut, &kafkaWrite)
	m.Source = between(rec.ExchangeEventTime, m.FirstIn)
	m.EndToEnd = between(rec.ExchangeEventTime, &kafkaWrite)
	return m
}

// between returns to-from, or nil when either end is unknown. The result may be
// NEGATIVE: the stamps come from different machines' clocks, and hiding that
// would turn a clock-skew problem into a latency mystery.
func between(from, to *time.Time) *time.Duration {
	if from == nil || to == nil {
		return nil
	}
	d := to.Sub(*from)
	return &d
}
