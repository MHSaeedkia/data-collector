package scenario

// scenarios are the cases in numbered order. Each one warms the pipeline up for
// its own exchange before it runs, so they are independent and the list can be
// cut down while working on a single case.
var Scenarios = []struct {
	Name string
	S    Scenario
}{
	// Nobitex — WS pushes are snapshots, not deltas (REVISED 2026-09-02, see data_ex1.go)
	{"01-ex1-ws-snapshots-replace-wholesale", Ex1WsSnapshotsReplaceWholesale},
	{"02-ex1-ws-snapshot-alone-establishes-baseline", Ex1WsSnapshotAloneEstablishesBaseline},
	{"03-ex1-ws-gap-accepted-stale-rejected", Ex1WsGapAcceptedStaleRejected},
	{"04-ex1-noise-frames", Ex1NoiseFrames},
	{"05-ex1-stale-rest-replay", Ex1StaleRestReplay},
	{"06-ex1-precision-dust", Ex1PrecisionDust},
	{"07-ex1-rebase-toman", Ex1RebaseToman},
	{"08-ex1-rebase-scaled-unit", Ex1RebaseScaledUnit},

	// Bitpin — WS pushes are snapshots, not deltas (REVISED 2026-09-02, see data_ex2.go)
	{"09-ex2-ws-snapshots-replace-wholesale", Ex2WsSnapshotsReplaceWholesale},
	{"10-ex2-ws-snapshot-alone-establishes-baseline", Ex2WsSnapshotAloneEstablishesBaseline},
	{"11-ex2-ws-gap-accepted-stale-rejected", Ex2WsGapAcceptedStaleRejected},
	{"12-ex2-noise-frames", Ex2NoiseFrames},
	{"13-ex2-stale-rest-replay", Ex2StaleRestReplay},
	{"14-ex2-precision-dust", Ex2PrecisionDust},

	// Wallex
	{"15-ex3-wallex-half-book", Ex3WallexHalfBook},
	{"16-ex3-empty-side-wipe", Ex3EmptySideWipe},
	{"17-ex3-precision-dust", Ex3PrecisionDust},
	{"18-ex3-noise-frames", Ex3NoiseFrames},
	{"19-ex3-stale-replay", Ex3StaleReplay},

	// Ramzinex
	{"20-ex4-ramzinex-snapshots", Ex4RamzinexSnapshots},
	{"21-ex4-stale-offset", Ex4StaleOffset},
	{"22-ex4-noise-frames", Ex4NoiseFrames},
	{"23-ex4-rebase-toman", Ex4RebaseToman},
	{"24-ex4-rebase-scaled-unit", Ex4RebaseScaledUnit},

	// Bitget — a snapshot/update delta feed since 2026-08-22, and the only exchange
	// whose sequence is a millisecond clock rather than a counter (jump 600 ± 10).
	{"25-ex5-snapshot-then-updates", Ex5SnapshotThenUpdates},
	{"26-ex5-update-before-snapshot", Ex5UpdateBeforeSnapshot},
	{"27-ex5-jump-tolerance", Ex5JumpTolerance},
	{"28-ex5-multi-book-frame", Ex5MultiBookFrame},
	{"29-ex5-noise-frames", Ex5NoiseFrames},
	{"30-ex5-precision-dust", Ex5PrecisionDust},
	{"31-ex5-rest-snapshot-resync", Ex5RestSnapshotResync},

	// Bybit
	{"32-ex6-snapshot-then-deltas", Ex6SnapshotThenDeltas},
	{"33-ex6-one-sided-delta", Ex6OneSidedDelta},
	{"34-ex6-sequence-gap", Ex6SequenceGap},
	{"35-ex6-no-baseline", Ex6NoBaseline},
	{"36-ex6-noise-frames", Ex6NoiseFrames},
	{"37-ex6-precision-dust", Ex6PrecisionDust},

	// OKX
	{"38-ex8-update-before-snapshot", Ex8UpdateBeforeSnapshot},
	{"39-ex8-happy-path", Ex8HappyPath},
	{"40-ex8-sequence-gap", Ex8SequenceGap},
	{"41-ex8-stale-duplicate", Ex8StaleDuplicate},
	{"42-ex8-precision-dust", Ex8PrecisionDust},
	{"43-ex8-noise-frames", Ex8NoiseFrames},

	// Control plane — the snapshot requests job 2 sends NiFi. Grouped by feature
	// rather than by exchange; see data_control.go.
	// 45 (control-ex1-no-baseline-then-gap) was REMOVED 2026-09-02: it asserted a
	// no_baseline/sequence_gap episode on nobitex WS pushes, which are snapshots
	// now, not deltas, so that code path can no longer be reached through ex1.
	// Number left retired rather than reused or renumbered — grep the Go
	// identifiers, not the numbers, per the convention below.
	{"44-control-ex6-gap-resync-gap", ControlEx6GapResyncGap},

	// Control plane — the resync the sequenced ordering guard used to throw away,
	// which is the deadlock fixed on 2026-08-19; proves the resync was ACCEPTED
	// and that the episode re-armed. See data_control.go. 47
	// (control-ex1-lagging-rest-resync, the event-time-guard variant) was REMOVED
	// 2026-09-02 for the same reason as 45.
	{"46-control-ex6-stale-resync-accepted", ControlEx6StaleResyncAccepted},

	// Bybit's SECOND stream, the REST depth snapshot (added 2026-08-24). Lives here
	// rather than with 32-37 because it is a resync scenario: it is the regression
	// test for the ex5 loop, ported to ex6's counter. See data_ex6.go.
	{"48-ex6-rest-snapshot-resync", Ex6RestSnapshotResync},

	// Ompfinex (added 2026-08-24). Appended rather than slotted in after ex8, for the
	// same reason 48 was: renumbering an existing block silently invalidates every
	// reference to it in memory/, todo.md and past run logs, and the numbers are not
	// worth that. Grep the Go identifiers, not the numbers. See data_ex7.go.
	{"49-ex7-rest-then-ws-updates", Ex7RestThenWsUpdates},
	{"50-ex7-sequence-gap", Ex7SequenceGap},
	{"51-ex7-precision-dust", Ex7PrecisionDust},
	{"52-ex7-no-baseline", Ex7NoBaseline},
	{"53-ex7-noise-frames", Ex7NoiseFrames},
	{"54-ex7-one-sided-update", Ex7OneSidedUpdate},

	// LBank (added 2026-08-26). Appended for the same reason 48 and 49 were. ex9 is the
	// second SNAPSHOT-ONLY exchange after ex3/wallex and the first with a real wire clock
	// but no sequence field at all, so its cases are about the event-time ordering guard
	// rather than about contiguity: there is no gap case and no no-baseline case to write,
	// and every one of these asserts an EMPTY control stream. See data_ex9.go.
	{"55-ex9-snapshot-stream", Ex9SnapshotStream},
	{"56-ex9-stale-snapshot-replay", Ex9StaleSnapshotReplay},
	{"57-ex9-duplicate-timestamp", Ex9DuplicateTimestamp},
	{"58-ex9-noise-frames", Ex9NoiseFrames},
	{"59-ex9-precision-dust", Ex9PrecisionDust},

	// Control plane — the EVENT-TIME half of the deadlock (added 2026-09-05). 46 covers
	// the sequenced ordering guard yielding to an outstanding request; this covers the
	// null-seq/event-time guard doing the same. It replaces the retired 47
	// (control-ex1-lagging-rest-resync), ported to ex6 because bybit's REST snapshot is
	// now the platform's only live null-seq resync feeding a real delta stream. Appended
	// as 60 rather than reusing 47, per the convention above. See data_control.go.
	{"60-control-ex6-lagging-rest-resync", ControlEx6LaggingRestResync},
}
