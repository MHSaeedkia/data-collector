//go:build e2e

package e2e

import (
	"testing"
	"time"
)

// topic matches the consumer's per-exchange regex
// (^ex[0-9]+-p[0-9]+-orderbook-snapshot-flink$) but uses ids no real exchange or
// pair will ever have, so it cannot be confused with pipeline data even if this
// ever ran against a shared broker.
const topic = "ex99-p99-orderbook-snapshot-flink"

// recoveryLine is matched with its full prefix and trailing "(partition", not as
// a loose substring. The loose version silently matched a DIFFERENT log line
// that happened to quote this one, and passed against a build with the fix
// removed — the whole test proved nothing for two runs. If this ever stops
// matching, check internal/kafka's count() before changing it here.
var recoveryLine = "kafka[ex]: first record from " + topic + " (partition"

// TestRecreatedTopicIsPurgedAndRediscovered is the regression test for the
// 2026-09-08 outage: `UNKNOWN_TOPIC_ID` repeating forever on a topic that
// `kafka-topics --describe` showed as perfectly healthy, with only a container
// restart to clear it.
//
// franz-go pins a topic's ID when it creates the cursor and never adopts a new
// one. It self-heals when it WATCHES a topic vanish from metadata for 15s — so a
// plain delete-and-recreate recovers on its own and proves nothing. The bug
// needs the client to be blind across the whole window, which is what a broker
// outage does and what `docker pause` reproduces here.
//
// Failure of this test means one of two things, and the assertions are ordered
// to tell them apart: either the error stopped happening (franz-go changed, and
// this test now guards nothing), or it happens and we no longer recover.
func TestRecreatedTopicIsPurgedAndRediscovered(t *testing.T) {
	s := startStack(t)

	// The topic exists BEFORE the app starts, so the regex consumer picks it up
	// at startup rather than on some later metadata refresh.
	s.createTopic(topic)
	s.startWeb()

	// Step 1 — prove the app is really consuming this topic. Junk fails on the
	// Avro magic byte, which is exactly the observable we want: it means a
	// record reached the decoder, so a cursor exists and is pinned to the
	// current topic ID.
	s.produceUntilLog(topic, "hello", time.Time{},
		"Skipping bad message on "+topic, 2*time.Minute)

	// Step 2 — remember the ID, then blind the app for the whole window.
	before := s.topicID(topic)
	s.pause("web")
	// Registered after the stack's own teardown, so it runs BEFORE it: a paused
	// container is awkward to stop, and a test that fails between here and the
	// unpause below must not leave one behind.
	t.Cleanup(func() { s.unpauseQuietly("web") })

	// Step 3 — delete and recreate while it cannot see anything. This is the
	// part a plain delete does not reproduce.
	s.deleteTopic(topic)
	s.createTopic(topic)
	after := s.topicID(topic)
	if before == after {
		t.Fatalf("topic ID did not change across delete+recreate (%s) — "+
			"the test cannot reproduce the bug, so its result means nothing", before)
	}
	t.Logf("topic ID changed: %s -> %s", before, after)

	// Step 4 — wake it into a world where its pinned ID no longer exists.
	mark := time.Now()
	s.unpause("web")

	// Step 5 — it must recover with NO restart. "first record from" only prints
	// for a topic the consumer has forgotten, so seeing it again is proof the
	// purge happened and a fresh cursor was built on the new ID.
	// 3 minutes is ~12x the recovery actually observed (~15s). Generous enough
	// for a loaded server, short enough that a REGRESSION here fails in three
	// minutes instead of five.
	s.produceUntilLog(topic, "hello", mark, recoveryLine, 3*time.Minute)

	// The recovery is only meaningful if the bug actually fired on the way. If
	// these two ever stop appearing, franz-go's behaviour changed and this test
	// is no longer guarding what it claims to.
	logs := s.logsSince("web", mark)
	// Shown under -v. This window is the whole story of the test, and reading it
	// is the only way to tell a real recovery from an accidental one.
	t.Logf("web log after unpause:\n%s", tail(logs, 40))

	s.requireLogged(logs, "UNKNOWN_TOPIC_ID",
		"the stale-topic-ID error must still occur, or this test proves nothing")
	s.requireLogged(logs, "purging it so the regex re-discovers it",
		"recovery must come from our purge, not from luck")
}
