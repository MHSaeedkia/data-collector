package consumer

import "testing"

// The control topic is the only one the harness reads that is not backed by a
// registered schema — its shape is a convention between job 2 and NiFi, so
// nothing but this pins it. The wire form nests the ids under `payload`; the
// harness's view flattens them, and getting that mapping backwards would make
// every control assertion in the suite compare zeroes against zeroes.
func TestDecodeControlCommand(t *testing.T) {
	command, err := decodeControlCommand([]byte(
		`{"action":"snapshot_request","payload":{"pair_id":1,"exchange_id":6}}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if command.Action != "snapshot_request" {
		t.Errorf("action = %q, want snapshot_request", command.Action)
	}
	if command.ExchangeID != 6 {
		t.Errorf("exchange_id = %d, want 6", command.ExchangeID)
	}
	if command.PairID != 1 {
		t.Errorf("pair_id = %d, want 1", command.PairID)
	}
}

// A field renamed on the producing side must fail loudly here. Without
// DisallowUnknownFields a renamed `exchange_id` would decode as a zero and the
// scenario would fail on a diff of ids rather than on the rename.
func TestDecodeControlCommandRejectsUnknownFields(t *testing.T) {
	for name, payload := range map[string]string{
		"unknown at the root":      `{"action":"snapshot_request","target":{"pair_id":1}}`,
		"unknown inside payload":   `{"action":"snapshot_request","payload":{"market_id":1}}`,
		"not an object":            `["snapshot_request"]`,
		"payload is not an object": `{"action":"snapshot_request","payload":7}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeControlCommand([]byte(payload)); err == nil {
				t.Fatalf("decoded %s without an error", payload)
			}
		})
	}
}
