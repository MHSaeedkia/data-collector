package scenario

import (
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
			if command.Key != "" {
				t.Errorf("%s: want_control_commands[%d] declares a key; keys are checked structurally, then stripped",
					sc.Name, i)
			}
		}
	}
}
