package latency

import (
	"fmt"
	"io"
	"strings"
	"time"

	"orderbook-latency/internal/event"
)

// stampFormat carries the zone because the stamps come from containers and the
// reader is on a host that may not share it. Milliseconds because that is the
// resolution every timestamp in the pipeline is stored at — more digits would
// be invented precision.
const stampFormat = "2006-01-02 15:04:05.000 -0700"

// The stage table repeats one record's own timestamps, all within a second or
// two of the block header above it, so the rows carry the time of day only —
// the date and zone are stated once, at the top.
const clockFormat = "15:04:05.000"

// Meta is the Kafka-side facts about a record: everything the old
// scripts/watch-topic.sh printed, which this replaces.
type Meta struct {
	Topic      string
	Partition  int32
	Offset     int64
	WriteTime  time.Time
	RecordName string
}

// Render writes one human-readable block per record. One block rather than one
// line because the brief asks for every raw timestamp to be shown as well as
// every derived duration, and eighteen values do not fit a terminal line.
func Render(w io.Writer, meta Meta, rec event.Record, m Metrics) {
	var b strings.Builder

	b.WriteString(strings.Repeat("─", 78) + "\n")
	fmt.Fprintf(&b, "%s  partition=%d  offset=%d", meta.Topic, meta.Partition, meta.Offset)
	if rec.ExchangeID != 0 || rec.PairID != 0 {
		fmt.Fprintf(&b, "  ex%d-p%d", rec.ExchangeID, rec.PairID)
	}
	if meta.RecordName != "" {
		fmt.Fprintf(&b, "  (%s)", meta.RecordName)
	}
	b.WriteString("\n\n")

	fmt.Fprintf(&b, "  kafka write         %s\n", stamp(&meta.WriteTime))
	fmt.Fprintf(&b, "  exchange event      %s\n", stamp(rec.ExchangeEventTime))
	if t := event.Optional(rec.EventTime); t != nil {
		fmt.Fprintf(&b, "  event_time          %s\n", stamp(t))
	}
	if t := event.Optional(rec.MaxEventTime); t != nil {
		fmt.Fprintf(&b, "  max_event_time      %s\n", stamp(t))
	}
	if rec.MinEventTime != nil {
		fmt.Fprintf(&b, "  min_event_time      %s\n", stamp(rec.MinEventTime))
	}

	if len(m.Stages) == 0 {
		b.WriteString("\n  no pipeline_timings on this record\n")
		io.WriteString(w, b.String())
		return
	}

	b.WriteString("\n")
	fmt.Fprintf(&b, "  %-16s %-14s %-14s %10s %10s\n", "job", "in", "out", "job", "wait")
	for _, s := range m.Stages {
		fmt.Fprintf(&b, "  %-16s %-14s %-14s %10s %10s\n",
			s.Name, clock(s.In), clock(s.Out), dur(s.Job), dur(s.Gap))
	}

	b.WriteString("\n")
	fmt.Fprintf(&b, "  source     exchange → job 1 in   %12s\n", dur(m.Source))
	fmt.Fprintf(&b, "  pipeline   job 1 in → job 5 out  %12s\n", dur(m.Pipeline))
	fmt.Fprintf(&b, "  write      job 5 out → kafka     %12s\n", dur(m.Write))
	fmt.Fprintf(&b, "  end-to-end exchange → kafka      %12s\n", dur(m.EndToEnd))

	io.WriteString(w, b.String())
}

func clock(t *time.Time) string {
	if t == nil {
		return "n/a"
	}
	return t.Local().Format(clockFormat)
}

func stamp(t *time.Time) string {
	if t == nil {
		return "n/a"
	}
	return t.Local().Format(stampFormat)
}

// dur prints a duration the way a reader compares them in a column: always
// three decimals once past a second, because Go's own formatting drops trailing
// zeros ("5.6s" next to "1.597s") and unequal decimal places are exactly what
// makes a column of numbers hard to scan.
//
// Below a second it is whole milliseconds, the resolution the stamps are stored
// at — except under a millisecond, where it falls back to microseconds rather
// than printing "0s" for a step that really did take measurable time.
func dur(d *time.Duration) string {
	if d == nil {
		return "n/a"
	}
	switch {
	case *d > -time.Millisecond && *d < time.Millisecond:
		return d.Round(time.Microsecond).String()
	case *d > -time.Second && *d < time.Second:
		return d.Round(time.Millisecond).String()
	default:
		return fmt.Sprintf("%.3fs", d.Round(time.Millisecond).Seconds())
	}
}
