package scenario

import (
	"fmt"

	"orderbook-e2e/events"
)

// The exchange clock in the e2e harness.
//
// `exchange_event_time` is the exchange's OWN timestamp, taken straight off the
// wire by job 1 and then carried untouched to job 6. `event_time` next to it is
// NOT the same thing: where a feed sends no clock, job 1 substitutes its own
// processing time there, and only the null in this field says so.
//
// A scenario cannot declare the value. For the feeds that do send a clock it is
// whatever the source frame said, which the scenario already spells out as
// `event_time`; repeating it in every wanted snapshot would assert nothing and
// cost the whole suite an edit. What IS assertable is the rule, and the rule is
// what a bug would break:
//
//   - a feed with no clock must stay null, forever — substituting processing
//     time here would make a lag measurement silently read as ~0
//   - a feed with a clock must carry one, or a hop dropped it
//   - and when it is set it must equal `event_time`, because job 1 fills both
//     from the same number
//
// So it is checked here and then cleared before the literal comparison, exactly
// the way scenario/lineage.go handles the ids.

// exchangesWithoutClock never put a timestamp on the wire at all, so job 1
// leaves this field null for every frame they send.
//
// ex7/ompfinex is deliberately NOT here: it sends a clock on snapshots and none
// on updates, so either shape is legal for it and only the equality rule below
// applies.
var exchangesWithoutClock = map[int64]bool{
	3: true, // wallex
	4: true, // ramzinex
}

const ompfinexExchangeID = 7

// checkExchangeEventTime enforces the three rules above over one topic's
// snapshots. It runs BEFORE IgnoreEventTime blanks anything, so the equality
// rule still has an event_time to compare against.
//
// One assumption: no snapshot here was produced by job 3's SILENCE reset, which
// carries event_time = now with a null exchange clock and would trip the
// "a feed with a clock must carry one" rule. That holds because the suite's runs
// finish well inside `staleness_threshold_seconds` (60 in 02_seed.sql) — see the
// note on Ex5SeqCarriedOverFromDepth. A scenario that deliberately waits out
// that timer would need this to know about empty books.
func checkExchangeEventTime(topic string, exchangeID int64, snapshots []events.OrderbookSnapshot) error {
	for i, snapshot := range snapshots {
		got := snapshot.ExchangeEventTime

		if exchangesWithoutClock[exchangeID] {
			if got != "" {
				return fmt.Errorf(
					"%s: snapshot %d: ex%d sends no timestamp of its own, so exchange_event_time must be null, got %q",
					topic, i, exchangeID, got)
			}
			continue
		}

		if got == "" && exchangeID != ompfinexExchangeID {
			return fmt.Errorf(
				"%s: snapshot %d: ex%d sends a timestamp, so exchange_event_time must be set, got null",
				topic, i, exchangeID)
		}

		if got != "" && got != snapshot.EventTime {
			return fmt.Errorf(
				"%s: snapshot %d: exchange_event_time %q != event_time %q — job 1 fills both from the same wire value",
				topic, i, got, snapshot.EventTime)
		}
	}
	return nil
}

func stripExchangeEventTime(snapshots []events.OrderbookSnapshot) {
	for i := range snapshots {
		snapshots[i].ExchangeEventTime = ""
	}
}
