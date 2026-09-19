package latency

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"orderbook-latency/internal/event"
)

func at(t *testing.T, s string) *time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, s)
	require.NoError(t, err)
	return &parsed
}

// The timings off the record the user captured from
// ex1-p1-orderbook-snapshot-flink, with a parse pair added in front: the capture
// predates the 2026-09-19 job-1 split, so the real record had none. 9ms between
// parse_out and pair_extract_in is the hop that split introduced.
func sample(t *testing.T) *event.Timings {
	t.Helper()
	return &event.Timings{
		ParseIn:         at(t, "2026-09-14T11:04:26.961Z"),
		ParseOut:        at(t, "2026-09-14T11:04:26.962Z"),
		PairExtractIn:   at(t, "2026-09-14T11:04:26.971Z"),
		PairExtractOut:  at(t, "2026-09-14T11:04:26.972Z"),
		TypeValidateIn:  at(t, "2026-09-14T11:04:28.569Z"),
		TypeValidateOut: at(t, "2026-09-14T11:04:28.569Z"),
		RebaseIn:        at(t, "2026-09-14T11:04:32.572Z"),
		RebaseOut:       at(t, "2026-09-14T11:04:32.572Z"),
		PrecisionIn:     at(t, "2026-09-14T11:04:32.989Z"),
		PrecisionOut:    at(t, "2026-09-14T11:04:32.989Z"),
		BookBuildIn:     at(t, "2026-09-14T11:04:33.214Z"),
		BookBuildOut:    at(t, "2026-09-14T11:04:33.214Z"),
	}
}

func TestComputeOnTheCapturedRecord(t *testing.T) {
	rec := event.Record{Timings: sample(t)}
	kafka := *at(t, "2026-09-14T11:04:33.500Z")

	m := Compute(rec, kafka)

	require.Len(t, m.Stages, 6)
	// Job latency: out - in, per job.
	assert.Equal(t, time.Millisecond, *m.Stages[0].Job)
	assert.Equal(t, time.Millisecond, *m.Stages[1].Job)
	assert.Equal(t, time.Duration(0), *m.Stages[2].Job)
	assert.Equal(t, time.Duration(0), *m.Stages[5].Job)
	// Inter-job latency: this job's in minus the previous job's out.
	assert.Nil(t, m.Stages[0].Gap, "the first job has no predecessor to wait for")
	assert.Equal(t, 9*time.Millisecond, *m.Stages[1].Gap, "the hop the job-1 split added")
	assert.Equal(t, 1597*time.Millisecond, *m.Stages[2].Gap)
	assert.Equal(t, 4003*time.Millisecond, *m.Stages[3].Gap)
	assert.Equal(t, 417*time.Millisecond, *m.Stages[4].Gap)
	assert.Equal(t, 225*time.Millisecond, *m.Stages[5].Gap)
	// Totals.
	assert.Equal(t, 6253*time.Millisecond, *m.Pipeline)
	assert.Equal(t, 286*time.Millisecond, *m.Write)
}

// The whole reason exchange_event_time exists: when the feed sends no clock,
// the two measurements that depend on it must say so rather than invent one.
func TestComputeWithoutAnExchangeClock(t *testing.T) {
	m := Compute(event.Record{Timings: sample(t)}, *at(t, "2026-09-14T11:04:33.500Z"))

	assert.Nil(t, m.Source)
	assert.Nil(t, m.EndToEnd)
	assert.NotNil(t, m.Pipeline, "the in-pipeline measurements do not depend on it")
}

func TestComputeWithAnExchangeClock(t *testing.T) {
	rec := event.Record{
		Timings:           sample(t),
		ExchangeEventTime: at(t, "2026-09-14T11:04:26.146Z"),
	}

	m := Compute(rec, *at(t, "2026-09-14T11:04:33.500Z"))

	assert.Equal(t, 815*time.Millisecond, *m.Source)    // 26.146 -> 26.961
	assert.Equal(t, 7354*time.Millisecond, *m.EndToEnd) // 26.146 -> 33.500
}

// A record read off an early topic has only the stages it reached. The totals
// must anchor on what IS there rather than give up.
func TestComputeOnPartialTimings(t *testing.T) {
	rec := event.Record{Timings: &event.Timings{
		PairExtractIn:   at(t, "2026-09-14T11:04:26.971Z"),
		PairExtractOut:  at(t, "2026-09-14T11:04:26.972Z"),
		TypeValidateIn:  at(t, "2026-09-14T11:04:28.569Z"),
		TypeValidateOut: at(t, "2026-09-14T11:04:28.600Z"),
	}}

	m := Compute(rec, *at(t, "2026-09-14T11:04:28.700Z"))

	assert.Equal(t, 1629*time.Millisecond, *m.Pipeline) // 26.971 -> 28.600
	assert.Equal(t, 100*time.Millisecond, *m.Write)     // 28.600 -> kafka
	// A record written before the split, or read off a topic job 1 never stamped: the
	// parse stage is simply absent, and the stage after it has no predecessor to wait for.
	assert.Nil(t, m.Stages[0].Job)
	assert.Nil(t, m.Stages[1].Gap)
	assert.Nil(t, m.Stages[3].Job)
	assert.Nil(t, m.Stages[3].Gap)
}

// A topic with no pipeline_timings at all (the terminal p{id}-{side} family).
func TestComputeWithNoTimings(t *testing.T) {
	m := Compute(event.Record{}, *at(t, "2026-09-14T11:04:33.500Z"))

	assert.Empty(t, m.Stages)
	assert.Nil(t, m.Pipeline)
	assert.Nil(t, m.Write)
	assert.Nil(t, m.FirstIn)
}

// The stamps come from different machines. A negative duration is a clock-skew
// finding and must survive to the screen, not be clamped to zero.
func TestComputeKeepsNegativeDurations(t *testing.T) {
	rec := event.Record{Timings: &event.Timings{
		PairExtractIn:  at(t, "2026-09-14T11:04:26.971Z"),
		PairExtractOut: at(t, "2026-09-14T11:04:26.900Z"),
	}}

	m := Compute(rec, *at(t, "2026-09-14T11:04:27.000Z"))

	assert.Equal(t, -71*time.Millisecond, *m.Stages[1].Job)
}

func TestRenderShowsEveryStampAndNAForTheUnknown(t *testing.T) {
	rec := event.Record{Timings: sample(t), EventTime: *at(t, "2026-09-14T11:04:26.146Z")}
	kafka := *at(t, "2026-09-14T11:04:33.500Z")
	meta := Meta{Topic: "ex1-p1-orderbook-snapshot-flink", Offset: 1505638, WriteTime: kafka}

	var out bytes.Buffer
	Render(&out, meta, rec, Compute(rec, kafka))
	got := out.String()

	assert.Contains(t, got, "offset=1505638")
	assert.Contains(t, got, "1 parse")
	assert.Contains(t, got, "2 pair-extract")
	assert.Contains(t, got, "6 book-build")
	assert.Contains(t, got, "1.597s") // the inter-job wait
	assert.Contains(t, got, "6.253s") // the pipeline total
	assert.Contains(t, got, "286ms")  // the write latency
	assert.Contains(t, got, "n/a")    // source and end-to-end, with no exchange clock
	assert.NotContains(t, got, "0001-01-01", "a missing stamp must never render as the zero time")
}

func TestRenderOnATopicWithNoTimings(t *testing.T) {
	kafka := *at(t, "2026-09-14T11:04:33.500Z")
	rec := event.Record{MaxEventTime: *at(t, "2026-09-14T11:04:33.400Z")}

	var out bytes.Buffer
	Render(&out, Meta{Topic: "p1-asks", WriteTime: kafka}, rec, Compute(rec, kafka))

	assert.Contains(t, out.String(), "no pipeline_timings")
	assert.Contains(t, out.String(), "max_event_time")
}

// The p{id}-{side} family has no exchange clock, so its only end-to-end is the
// stalest contributing book's event_time against the write.
func TestComputeStalestOnAnAggregatedRecord(t *testing.T) {
	rec := event.Record{
		MaxEventTime: *at(t, "2026-09-14T11:04:33.400Z"),
		MinEventTime: at(t, "2026-09-14T11:04:26.300Z"),
	}

	m := Compute(rec, *at(t, "2026-09-14T11:04:33.500Z"))

	assert.Equal(t, 7200*time.Millisecond, *m.Stalest)
	assert.Nil(t, m.EndToEnd)
}

func TestRenderPrintsEndToEndOnEveryTopic(t *testing.T) {
	kafka := *at(t, "2026-09-14T11:04:33.500Z")

	aggregated := event.Record{
		MaxEventTime: *at(t, "2026-09-14T11:04:33.400Z"),
		MinEventTime: at(t, "2026-09-14T11:04:26.300Z"),
	}
	var out bytes.Buffer
	Render(&out, Meta{Topic: "p1-asks", WriteTime: kafka}, aggregated, Compute(aggregated, kafka))
	assert.Contains(t, out.String(), "end-to-end")
	assert.Contains(t, out.String(), "7.200s")

	// An exchange clock with no timings must not lose its end-to-end either.
	untimed := event.Record{ExchangeEventTime: at(t, "2026-09-14T11:04:26.146Z")}
	out.Reset()
	Render(&out, Meta{Topic: "ex1-raw", WriteTime: kafka}, untimed, Compute(untimed, kafka))
	assert.Contains(t, out.String(), "7.354s")
	assert.NotContains(t, out.String(), "stalest", "only the aggregated shape has a min_event_time")
}

func TestDurFormatting(t *testing.T) {
	sub := 450 * time.Microsecond
	ms := 1597 * time.Millisecond
	zero := time.Duration(0)

	assert.Equal(t, "n/a", dur(nil))
	assert.Equal(t, "450µs", dur(&sub), "sub-millisecond work must not print as 0s")
	assert.Equal(t, "1.597s", dur(&ms))
	assert.Equal(t, "0s", dur(&zero))
}

// Go's own formatting drops trailing zeros, which puts "5.6s" next to "1.597s"
// in the same column. Three decimals past a second keeps a column scannable.
func TestDurPadsSecondsToThreeDecimals(t *testing.T) {
	sixSec := 5600 * time.Millisecond
	exact := 6 * time.Second
	sub := 417 * time.Millisecond
	negative := -71 * time.Millisecond

	assert.Equal(t, "5.600s", dur(&sixSec))
	assert.Equal(t, "6.000s", dur(&exact))
	assert.Equal(t, "417ms", dur(&sub), "below a second stays in milliseconds")
	assert.Equal(t, "-71ms", dur(&negative))
}
