package scenario

import (
	"reflect"
	"strings"
	"testing"

	"orderbook-e2e/events"
)

// The Kafka key is derived, not declared, so it is the one part of a control
// command a scenario cannot get wrong — which means this check is the only thing
// standing between a key built from the wrong ids and a green run.
func TestCheckControlKeys(t *testing.T) {
	ok := []events.ControlCommand{
		{Action: "snapshot_request", ExchangeID: 6, PairID: 1, Key: "6|1"},
		{Action: "snapshot_request", ExchangeID: 1, PairID: 52, Key: "1|52"},
	}
	if err := checkControlKeys("control-plane", ok); err != nil {
		t.Fatalf("well-keyed commands rejected: %v", err)
	}

	// The mistake this exists for: the two ids the wrong way round, which still
	// partitions and still delivers.
	swapped := []events.ControlCommand{
		{Action: "snapshot_request", ExchangeID: 6, PairID: 1, Key: "1|6"},
	}
	err := checkControlKeys("control-plane", swapped)
	if err == nil {
		t.Fatal("a swapped key was accepted")
	}
	if !strings.Contains(err.Error(), `want "6|1"`) {
		t.Errorf("error does not name the wanted key: %v", err)
	}

	if err := checkControlKeys("control-plane", []events.ControlCommand{{ExchangeID: 6, PairID: 1}}); err == nil {
		t.Error("an empty key was accepted")
	}
}

// A scenario that declares nothing must assert that NOTHING was requested. If
// nil ever came to mean "skip", every healthy scenario in the suite would stop
// noticing a spurious snapshot request, which is the failure this whole check is
// here to catch.
func TestWantControlCommandsTreatsNilAsEmpty(t *testing.T) {
	if got := (Scenario{}).wantControlCommands(); got == nil || len(got) != 0 {
		t.Fatalf("nil WantControlCommands = %+v, want an empty non-nil stream", got)
	}

	if err := compare("control-plane",
		[]events.ControlCommand{{Action: "snapshot_request", ExchangeID: 6, PairID: 1}},
		(Scenario{}).wantControlCommands()); err == nil {
		t.Error("an undeclared command compared as a match")
	}
}

// Every scenario's declared commands must name that scenario's own market: one
// scenario feeds one exchange's raw topic, so a command for anything else could
// never be produced. Cheaper to catch here than in a multi-minute run.
func TestScenarioControlCommandsNameTheirOwnMarket(t *testing.T) {
	for _, sc := range Scenarios {
		for i, command := range sc.S.WantControlCommands {
			if command.Action != "snapshot_request" {
				t.Errorf("%s: want_control_commands[%d] action = %q, want snapshot_request",
					sc.Name, i, command.Action)
			}
			if command.ExchangeID != sc.S.ExchangeID || command.PairID != sc.S.PairID {
				t.Errorf("%s: want_control_commands[%d] names exchange %d pair %d, scenario feeds exchange %d pair %d",
					sc.Name, i, command.ExchangeID, command.PairID, sc.S.ExchangeID, sc.S.PairID)
			}
			if command.Simulation != 1 {
				t.Errorf("%s: want_control_commands[%d] simulation = %d, want 1 — every fixture in the suite feeds simulated data, and a command that lost the flag would have NiFi call a real exchange",
					sc.Name, i, command.Simulation)
			}
			if command.Key != "" || command.ID != "" || command.SourceIDs != nil {
				t.Errorf("%s: want_control_commands[%d] declares a key or lineage; both are checked structurally, then stripped",
					sc.Name, i)
			}
		}
	}
}

// The lineage check is the only thing that would notice a command built by
// copying the triggering event's id across instead of minting one — every field
// still holds a well-formed uuid either way, and the literal comparison never
// sees them because they are stripped first.
func TestCheckControlLineage(t *testing.T) {
	const own = "11111111-1111-4111-8111-111111111111"
	const parent = "22222222-2222-4222-8222-222222222222"

	ok := []events.ControlCommand{{ID: own, SourceIDs: []string{parent}}}
	if err := checkControlLineage("control-plane", ok); err != nil {
		t.Fatalf("well-formed lineage rejected: %v", err)
	}

	// The mistake this exists for: the command carrying the triggering event's
	// id as its own, which is what the producer did before 2026-08-18.
	inherited := []events.ControlCommand{{ID: parent, SourceIDs: []string{parent}}}
	if err := checkControlLineage("control-plane", inherited); err == nil {
		t.Error("a command that inherited its parent's id was accepted")
	}

	for name, commands := range map[string][]events.ControlCommand{
		"no id":             {{SourceIDs: []string{parent}}},
		"no parent":         {{ID: own}},
		"two parents":       {{ID: own, SourceIDs: []string{parent, own}}},
		"parent not a uuid": {{ID: own, SourceIDs: []string{"job1-gap"}}},
		"id reused across records": {
			{ID: own, SourceIDs: []string{parent}},
			{ID: own, SourceIDs: []string{parent}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := checkControlLineage("control-plane", commands); err == nil {
				t.Errorf("%s was accepted", name)
			}
		})
	}
}

// Stripping has to clear all three derived fields, or a scenario would have to
// declare a uuid it cannot know.
func TestStripControlLineage(t *testing.T) {
	commands := []events.ControlCommand{{
		Action: "snapshot_request", ExchangeID: 6, PairID: 1, Simulation: 1,
		Key: "6|1", ID: "an-id", SourceIDs: []string{"a-parent"},
	}}
	stripControlLineage(commands)

	want := events.ControlCommand{
		Action: "snapshot_request", ExchangeID: 6, PairID: 1, Simulation: 1,
	}
	if !reflect.DeepEqual(commands[0], want) {
		t.Errorf("stripped = %+v, want %+v", commands[0], want)
	}
}
