package scenario

import (
	"strings"
	"testing"

	"orderbook-e2e/events"
)

// These run without a stack. The rule they pin is the whole point of
// exchange_event_time: a feed with no clock of its own must keep a NULL here,
// because the substitute sitting in event_time would otherwise read as a real
// exchange timestamp and make any lag measured off it look like ~0.

func snapshot(exchangeClock, eventTime string) events.OrderbookSnapshot {
	return events.OrderbookSnapshot{EventTime: eventTime, ExchangeEventTime: exchangeClock}
}

func TestCheckExchangeEventTime(t *testing.T) {
	const at = "2027-01-15T08:00:00Z"

	cases := []struct {
		name       string
		exchangeID int64
		snapshots  []events.OrderbookSnapshot
		wantErr    string
	}{
		{
			name:       "feed with a clock carries it, equal to event_time",
			exchangeID: 1,
			snapshots:  []events.OrderbookSnapshot{snapshot(at, at)},
		},
		{
			name:       "feed with a clock lost it on some hop",
			exchangeID: 1,
			snapshots:  []events.OrderbookSnapshot{snapshot("", at)},
			wantErr:    "must be set",
		},
		{
			name:       "feed with a clock disagrees with event_time",
			exchangeID: 1,
			snapshots:  []events.OrderbookSnapshot{snapshot("2027-01-15T09:00:00Z", at)},
			wantErr:    "!= event_time",
		},
		{
			name:       "ex3 wallex sends no clock, so null is correct",
			exchangeID: 3,
			snapshots:  []events.OrderbookSnapshot{snapshot("", at)},
		},
		{
			name:       "ex4 ramzinex filled with processing time — the bug this field exists to stop",
			exchangeID: 4,
			snapshots:  []events.OrderbookSnapshot{snapshot(at, at)},
			wantErr:    "must be null",
		},
		{
			name:       "ex7 ompfinex may carry one (snapshot frame)",
			exchangeID: 7,
			snapshots:  []events.OrderbookSnapshot{snapshot(at, at)},
		},
		{
			name:       "ex7 ompfinex may carry none (update frame)",
			exchangeID: 7,
			snapshots:  []events.OrderbookSnapshot{snapshot("", at)},
		},
		{
			name:       "the rule holds for every snapshot in the stream, not just the first",
			exchangeID: 1,
			snapshots:  []events.OrderbookSnapshot{snapshot(at, at), snapshot("", at)},
			wantErr:    "snapshot 1",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkExchangeEventTime("ex-p1-orderbook-snapshot-flink", tc.exchangeID, tc.snapshots)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("want no error, got %v", err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("want error containing %q, got none", tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

// Stripping is what keeps the field out of the literal comparison, so no wanted
// snapshot in the suite has to spell it out.
func TestStripExchangeEventTime(t *testing.T) {
	snapshots := []events.OrderbookSnapshot{
		snapshot("2027-01-15T08:00:00Z", "2027-01-15T08:00:00Z"),
		snapshot("", "2027-01-15T08:00:01Z"),
	}
	stripExchangeEventTime(snapshots)
	for i, s := range snapshots {
		if s.ExchangeEventTime != "" {
			t.Errorf("snapshot %d: not stripped, got %q", i, s.ExchangeEventTime)
		}
		if s.EventTime == "" {
			t.Errorf("snapshot %d: event_time must survive stripping", i)
		}
	}
}
