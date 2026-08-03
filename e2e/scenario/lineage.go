package scenario

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"orderbook-e2e/events"
)

// uuidPattern is the canonical 8-4-4-4-12 form java.util.UUID#toString emits,
// which is what every job mints. Matching it rather than merely checking for a
// non-empty string is what would catch a field that arrived truncated, or one
// silently filled with something that is not an id at all.
var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// newUUID returns a random v4 uuid. Hand-rolled on crypto/rand rather than
// pulling in a uuid module: the harness needs to generate ids in exactly one
// place, and the module would have to be vendored.
func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("crypto/rand: %v", err)) // no sensible way to continue a run
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10x
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// Record lineage in the e2e harness.
//
// The ids are fresh uuids on every run, so there is nothing a scenario could
// write down as a wanted value — a literal comparison against them would fail
// every time. What IS assertable is their shape and the relationships between
// them, and those turn out to be the interesting part anyway: that every record
// has an id, that no two records share one, and above all that the parent a
// record names is a record that actually exists upstream.
//
// So the harness does three things: it injects a sink_id into every source the
// way NiFi does, it checks the emitted lineage structurally, and it then clears
// the lineage fields so the existing per-scenario expectations still compare
// literally. The clearing is deliberate and is why events.OrderbookSnapshot
// documents these fields as never-declared.

// stampSinkID injects a fresh sink_id into a source payload exactly where NiFi
// puts it, and returns the payload with the id it used.
//
// Two carriers, the same split as the simulation flag: a root field for the
// six object-root exchanges, and the trailing metadata object for ex3/wallex's
// array envelope. This is why the harness injects rather than having the 177
// fixtures spell an id out — a per-record unique value cannot be a fixture, and
// injecting it here is what NiFi actually does.
//
// The payload is edited textually rather than being unmarshalled and
// re-marshalled: ex3 and ex5 carry prices as JSON NUMBERS, and a round trip
// through float64 would silently reformat them (see
// memory/project_bigdecimal_rules.md). Object roots get a field spliced in after
// the opening brace; array roots are split into raw elements so the levels array
// is copied through byte-for-byte and only the small metadata object is rebuilt.
func stampSinkID(source string) (string, string, error) {
	id := newUUID()
	trimmed := strings.TrimSpace(source)

	switch {
	case strings.HasPrefix(trimmed, "{"):
		stamped, err := stampObjectRoot(trimmed, id)
		return stamped, id, err
	case strings.HasPrefix(trimmed, "["):
		return stampArrayRoot(trimmed, id)
	default:
		return "", "", fmt.Errorf("payload is neither a JSON object nor an array: %.40s", trimmed)
	}
}

// stampObjectRoot splices "sink_id" in as the first field, leaving every other
// byte of the payload untouched.
func stampObjectRoot(payload, id string) (string, error) {
	rest := strings.TrimSpace(payload[1:])
	if strings.HasPrefix(rest, "}") {
		return fmt.Sprintf(`{"sink_id":%q}`, id), nil
	}
	return fmt.Sprintf(`{"sink_id":%q,%s`, id, payload[1:]), nil
}

// stampArrayRoot puts sink_id in the SAME object that carries simulation — the
// element at INDEX 2, which is where WallexParser reads it from (root.path(2)),
// not simply the last element. A 2-element frame (the pre-flag form) gains the
// object.
//
// Index 2 rather than "the last element" is what keeps the deliberately
// malformed noise frames intact: one of them is a four-element envelope ending
// in a bare string, fed to prove job 1 drops it. Merging an id into that string
// would fail; merging into slot 2 stamps the metadata object and leaves the
// fourth element — and so the test's whole point — untouched.
func stampArrayRoot(payload, id string) (string, string, error) {
	var elems []json.RawMessage
	if err := json.Unmarshal([]byte(payload), &elems); err != nil {
		return "", "", fmt.Errorf("parse array payload: %w", err)
	}
	if len(elems) < 2 {
		return "", "", fmt.Errorf("array payload has %d elements, want at least 2", len(elems))
	}

	meta := map[string]json.RawMessage{}
	if len(elems) > 2 {
		if err := json.Unmarshal(elems[2], &meta); err != nil {
			return "", "", fmt.Errorf("element 2 is not the metadata object: %w", err)
		}
	}
	meta["sink_id"] = json.RawMessage(fmt.Sprintf("%q", id))

	encoded, err := json.Marshal(meta)
	if err != nil {
		return "", "", err
	}
	if len(elems) > 2 {
		elems[2] = encoded
	} else {
		elems = append(elems, encoded)
	}

	// json.RawMessage marshals verbatim, so the levels array survives exactly.
	out, err := json.Marshal(elems)
	if err != nil {
		return "", "", err
	}
	return string(out), id, nil
}

// checkSnapshotLineage asserts what job 5's output must satisfy on every run.
//
// The counts cannot be predicted — how many events still hold a resting level
// depends on the scenario — so what is checked is that the lineage is present,
// well-formed and unique. A record with no source at all would mean job 5 lost
// the chain, and duplicate sink ids would mean two records claim one identity.
func checkSnapshotLineage(topic string, snapshots []events.OrderbookSnapshot) error {
	seen := map[string]int{}
	for i, s := range snapshots {
		if err := validID(s.SinkID); err != nil {
			return fmt.Errorf("%s record %d: sink_id: %w", topic, i, err)
		}
		if first, dup := seen[s.SinkID]; dup {
			return fmt.Errorf("%s record %d: sink_id %s already used by record %d",
				topic, i, s.SinkID, first)
		}
		seen[s.SinkID] = i

		if len(s.SourceIDs) == 0 {
			return fmt.Errorf("%s record %d: no source_ids — job 5 lost the chain", topic, i)
		}
		for j, source := range s.SourceIDs {
			if err := validID(source); err != nil {
				return fmt.Errorf("%s record %d: source_ids[%d]: %w", topic, i, j, err)
			}
		}
		if dupes := duplicates(s.SourceIDs); len(dupes) > 0 {
			return fmt.Errorf("%s record %d: source_ids repeat %v — they must be deduplicated",
				topic, i, dupes)
		}
	}
	return nil
}

// checkAggregatedLineage is the one exact, cross-job assertion available: every
// level of the final aggregated record must name the sink_id of a snapshot that
// job 5 really emitted. A scenario feeds a single exchange, so the levels of the
// final book all come from its last snapshot.
//
// This is what makes the per-level design testable end to end. A per-record
// source_ids could only have been checked for shape; a per-level source_id can
// be matched against the record it claims to come from.
func checkAggregatedLineage(topic string, final events.AggregatedSide,
	snapshots []events.OrderbookSnapshot) error {

	if err := validID(final.SinkID); err != nil {
		return fmt.Errorf("%s: sink_id: %w", topic, err)
	}
	if len(final.Levels) == 0 {
		return nil
	}

	emitted := map[string]bool{}
	for _, s := range snapshots {
		emitted[s.SinkID] = true
	}
	var lastSinkID string
	if len(snapshots) > 0 {
		lastSinkID = snapshots[len(snapshots)-1].SinkID
	}

	for i, level := range final.Levels {
		if err := validID(level.SourceID); err != nil {
			return fmt.Errorf("%s level %d: source_id: %w", topic, i, err)
		}
		if !emitted[level.SourceID] {
			return fmt.Errorf("%s level %d: source_id %s names no snapshot job 5 emitted",
				topic, i, level.SourceID)
		}
		if level.SourceID != lastSinkID {
			return fmt.Errorf("%s level %d: source_id %s is not the final snapshot %s — "+
				"a single-exchange run's final book comes from its last snapshot",
				topic, i, level.SourceID, lastSinkID)
		}
	}
	return nil
}

// stripSnapshotLineage clears the lineage so what remains can be compared
// literally against the scenario's wanted snapshots, which cannot name a uuid.
func stripSnapshotLineage(snapshots []events.OrderbookSnapshot) {
	for i := range snapshots {
		snapshots[i].SinkID = ""
		snapshots[i].SourceIDs = nil
	}
}

func stripAggregatedLineage(levels []events.AggregatedLevel) {
	for i := range levels {
		levels[i].SourceID = ""
	}
}

func validID(id string) error {
	if id == "" {
		return fmt.Errorf("empty")
	}
	if !uuidPattern.MatchString(id) {
		return fmt.Errorf("%q is not a uuid", id)
	}
	return nil
}

func duplicates(ids []string) []string {
	seen := map[string]bool{}
	var dupes []string
	for _, id := range ids {
		if seen[id] {
			dupes = append(dupes, id)
		}
		seen[id] = true
	}
	return dupes
}
