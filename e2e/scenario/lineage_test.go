package scenario

import (
	"encoding/json"
	"strings"
	"testing"

	"orderbook-e2e/events"
)

// These run without a stack: they are about the harness's own lineage handling,
// which every scenario now depends on. Job 1 drops a payload with no sink_id, so
// a stamping bug does not fail loudly — it empties every snapshot stream in the
// suite and looks like a broken pipeline. Cheap to catch here instead.

// TestStampSinkIDOnEverySource is the important one: it stamps all 177 real
// source payloads and checks each result is still valid JSON carrying the id
// where its parser will look for it. A payload shape the stamper cannot handle
// shows up here rather than as 41 mysteriously empty runs.
func TestStampSinkIDOnEverySource(t *testing.T) {
	stamped := 0
	for _, sc := range Scenarios {
		for i, source := range sc.S.Sources {
			out, id, err := stampSinkID(source)
			if err != nil {
				t.Fatalf("%s source %d: %v", sc.Name, i, err)
			}
			if err := validID(id); err != nil {
				t.Fatalf("%s source %d: generated id: %v", sc.Name, i, err)
			}
			if got := carriedSinkID(t, out); got != id {
				t.Fatalf("%s source %d: payload carries sink_id %q, want %q\npayload: %s",
					sc.Name, i, got, id, out)
			}
			// Stamping must not change an array envelope's LENGTH — the length
			// is the whole point of several noise frames, and a 3-element frame
			// that grew a fourth would be dropped by the parser.
			if strings.HasPrefix(strings.TrimSpace(source), "[") {
				want := len(elements(t, source))
				if want < 3 {
					want = 3 // a 2-element frame gains the metadata object
				}
				if got := len(elements(t, out)); got != want {
					t.Fatalf("%s source %d: envelope went from %d to %d elements\n%s",
						sc.Name, i, len(elements(t, source)), got, out)
				}
			}
			stamped++
		}
	}
	if stamped == 0 {
		t.Fatal("no sources stamped — the scenario list is empty")
	}
	t.Logf("stamped %d sources across %d scenarios", stamped, len(Scenarios))
}

// carriedSinkID reads the id back out of a stamped payload from exactly where
// the Java parser looks: a root field for an object root, the element at INDEX 2
// for an array root (WallexParser's root.path(2)).
func carriedSinkID(t *testing.T, payload string) string {
	t.Helper()

	if strings.HasPrefix(strings.TrimSpace(payload), "[") {
		elems := elements(t, payload)
		if len(elems) < 3 {
			t.Fatalf("stamped array payload has %d elements, want at least 3\n%s",
				len(elems), payload)
		}
		var meta struct {
			SinkID string `json:"sink_id"`
		}
		if err := json.Unmarshal(elems[2], &meta); err != nil {
			t.Fatalf("element 2 is not an object: %v", err)
		}
		return meta.SinkID
	}

	var root struct {
		SinkID string `json:"sink_id"`
	}
	if err := json.Unmarshal([]byte(payload), &root); err != nil {
		t.Fatalf("stamped object payload is not valid JSON: %v\n%s", err, payload)
	}
	return root.SinkID
}

func elements(t *testing.T, payload string) []json.RawMessage {
	t.Helper()
	var elems []json.RawMessage
	if err := json.Unmarshal([]byte(payload), &elems); err != nil {
		t.Fatalf("array payload is not valid JSON: %v\n%s", err, payload)
	}
	return elems
}

// Every source must get its OWN id — they become separate records on the raw
// topic, and job 5's fan-in would collapse if they shared one.
func TestStampSinkIDIsUniquePerCall(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 500; i++ {
		_, id, err := stampSinkID(`{"a":1}`)
		if err != nil {
			t.Fatal(err)
		}
		if seen[id] {
			t.Fatalf("duplicate id %s after %d calls", id, i)
		}
		seen[id] = true
	}
}

// The stamper must not reformat the payload around the id. ex3 and ex5 carry
// prices as JSON numbers, and a decode/encode round trip through float64 would
// quietly rewrite them — the exact failure memory/project_bigdecimal_rules.md
// exists to prevent.
func TestStampSinkIDPreservesNumericLiterals(t *testing.T) {
	object := `{"price": 62525.040, "qty": 0.000451000, "big": 123456789012345678901234567890}`
	stampedObject, _, err := stampSinkID(object)
	if err != nil {
		t.Fatal(err)
	}
	for _, literal := range []string{"62525.040", "0.000451000", "123456789012345678901234567890"} {
		if !strings.Contains(stampedObject, literal) {
			t.Errorf("object root: literal %s was reformatted:\n%s", literal, stampedObject)
		}
	}

	array := `["BTCUSDT@buyDepth", [{"price": 62525.040, "quantity": 0.000451000}], {"simulation":1}]`
	stampedArray, _, err := stampSinkID(array)
	if err != nil {
		t.Fatal(err)
	}
	for _, literal := range []string{"62525.040", "0.000451000"} {
		if !strings.Contains(stampedArray, literal) {
			t.Errorf("array root: literal %s was reformatted:\n%s", literal, stampedArray)
		}
	}
	// simulation must survive alongside the id in the SAME object.
	if !strings.Contains(stampedArray, `"simulation":1`) {
		t.Errorf("array root: simulation flag lost:\n%s", stampedArray)
	}
}

// A pre-flag 2-element ex3 frame gains the metadata object rather than being
// rejected, so the stamper stays usable if such a fixture is ever added.
func TestStampSinkIDAddsMetadataToTwoElementFrame(t *testing.T) {
	out, id, err := stampSinkID(`["BTCUSDT@buyDepth", [{"price": 1, "quantity": 2}]]`)
	if err != nil {
		t.Fatal(err)
	}
	if got := carriedSinkID(t, out); got != id {
		t.Fatalf("got sink_id %q, want %q\n%s", got, id, out)
	}
}

func TestCheckSnapshotLineageRejectsBrokenChains(t *testing.T) {
	const good = "11111111-1111-4111-8111-111111111111"
	const other = "22222222-2222-4222-8222-222222222222"

	cases := []struct {
		name      string
		snapshots []events.OrderbookSnapshot
		wantErr   string
	}{
		{
			name:      "valid",
			snapshots: []events.OrderbookSnapshot{{SinkID: good, SourceIDs: []string{other}}},
		},
		{
			name:      "missing sink id",
			snapshots: []events.OrderbookSnapshot{{SourceIDs: []string{other}}},
			wantErr:   "sink_id",
		},
		{
			name:      "sink id is not a uuid",
			snapshots: []events.OrderbookSnapshot{{SinkID: "nope", SourceIDs: []string{other}}},
			wantErr:   "not a uuid",
		},
		{
			name:      "no sources",
			snapshots: []events.OrderbookSnapshot{{SinkID: good}},
			wantErr:   "lost the chain",
		},
		{
			name: "two records share an id",
			snapshots: []events.OrderbookSnapshot{
				{SinkID: good, SourceIDs: []string{other}},
				{SinkID: good, SourceIDs: []string{other}},
			},
			wantErr: "already used by record 0",
		},
		{
			name:      "repeated source",
			snapshots: []events.OrderbookSnapshot{{SinkID: good, SourceIDs: []string{other, other}}},
			wantErr:   "must be deduplicated",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkSnapshotLineage("topic", tc.snapshots)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("want an error containing %q, got none", tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

// The cross-job check: an aggregated level must name the snapshot it came from.
func TestCheckAggregatedLineage(t *testing.T) {
	const lastSnapshot = "11111111-1111-4111-8111-111111111111"
	const earlier = "22222222-2222-4222-8222-222222222222"
	const aggregated = "33333333-3333-4333-8333-333333333333"

	snapshots := []events.OrderbookSnapshot{{SinkID: earlier}, {SinkID: lastSnapshot}}

	ok := events.AggregatedSide{
		SinkID: aggregated,
		Levels: []events.AggregatedLevel{{SourceID: lastSnapshot}},
	}
	if err := checkAggregatedLineage("topic", ok, snapshots); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// A level pointing at an id job 5 never emitted is the failure this exists
	// to catch — a source that was invented rather than carried.
	invented := events.AggregatedSide{
		SinkID: aggregated,
		Levels: []events.AggregatedLevel{{SourceID: "44444444-4444-4444-8444-444444444444"}},
	}
	if err := checkAggregatedLineage("topic", invented, snapshots); err == nil ||
		!strings.Contains(err.Error(), "names no snapshot") {
		t.Fatalf("want a 'names no snapshot' error, got %v", err)
	}

	// A stale but real id: the level came from a snapshot that is no longer the
	// current book for this single-exchange run.
	stale := events.AggregatedSide{
		SinkID: aggregated,
		Levels: []events.AggregatedLevel{{SourceID: earlier}},
	}
	if err := checkAggregatedLineage("topic", stale, snapshots); err == nil ||
		!strings.Contains(err.Error(), "is not the final snapshot") {
		t.Fatalf("want a 'not the final snapshot' error, got %v", err)
	}

	// An empty book has no levels to attribute, which is not a failure.
	empty := events.AggregatedSide{SinkID: aggregated}
	if err := checkAggregatedLineage("topic", empty, snapshots); err != nil {
		t.Fatalf("empty book: unexpected error: %v", err)
	}
}
