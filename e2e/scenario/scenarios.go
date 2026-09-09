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

	// Bitget — snapshot-only again since 2026-09-07, back on the `books50` channel with a
	// real `seq` counter (jump 0). 26 (ex5-update-before-snapshot), 27 (ex5-jump-tolerance)
	// and 31 (ex5-rest-snapshot-resync) were REMOVED with that change: ex5 sends no updates
	// and has no second REST stream, so none of those code paths is reachable through it any
	// more. (27's subject went further — ex5 was the only feed that ever stamped a jump
	// tolerance, so the field itself was deleted from the schema on 2026-09-07 and job 2's
	// contiguity check is a plain equality again.) Numbers left retired rather than renumbered,
	// per the convention below. Ex5StaleSeq — the snapshot ordering rule that replaces all
	// three — is appended as 62. See data_ex5.go.
	{"25-ex5-snapshot-stream", Ex5SnapshotStream},
	{"28-ex5-multi-book-frame", Ex5MultiBookFrame},
	{"29-ex5-noise-frames", Ex5NoiseFrames},
	{"30-ex5-precision-dust", Ex5PrecisionDust},

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

	// OKX's SECOND stream, the REST depth snapshot (added 2026-09-05). Appended as 61 rather than
	// slotted in after 38-43, for the same reason 48, 49 and 60 were. It is the regression test for
	// the resync black hole found live: job 1 had no branch for this shape, so the frame NiFi sends
	// to answer a `snapshot_request` was DROPPED and the market stayed dark. 40-ex8-sequence-gap
	// passed throughout, because its resync answer is a WS snapshot — a frame production never sends
	// for a delta feed. ex5 (31) and ex6 (48) already had this scenario; ex8 did not.
	// See data_ex8.go.
	{"61-ex8-rest-snapshot-resync", Ex8RestSnapshotResync},

	// Bitget's snapshot ordering rule (added 2026-09-07). Appended as 62 rather than slotted
	// into 25-30, for the same reason 48, 49, 60 and 61 were. With ex5 snapshot-only, job 2's
	// snapshot branch is the ONLY branch it can reach, so this is the whole of ex5's validation
	// coverage: a repeated or older `seq` is stale_or_duplicate, a forward one is accepted
	// however far it jumps, and the control stream stays empty. See data_ex5.go.
	{"62-ex5-stale-seq", Ex5StaleSeq},

	// The ex5 DEPLOY hazard (added 2026-09-07, PR #1 review). Job 2 keeps `lastSeq` in keyed
	// state and is not resubmitted by this change, so at deploy time it still holds the `depth`
	// channel's millisecond sequence — about a trillion above any `books50` seq. Every frame off
	// the new channel is then dead-lettered stale_or_duplicate with no control command and no way
	// back. Resubmit job-type-validator alongside job-pair-extractor. See data_ex5.go.
	{"63-ex5-seq-carried-over-from-depth", Ex5SeqCarriedOverFromDepth},

	// Bybit's documented service restart (added 2026-09-08). `u` drops to 1 mid-stream on a
	// full snapshot; job 1 stamps it null-seq so it re-anchors through baselinePending instead
	// of being ordered against a counter in the hundreds of millions. Appended as 64 rather
	// than slotted into 32-37, for the same reason 48, 49, 60, 61 and 62 were. The assertion
	// that matters is that BOTH the reject and the control streams stay empty: a restart is the
	// one discontinuity that needs no snapshot_request, because the exchange already sent the
	// snapshot. See data_ex6.go.
	{"64-ex6-service-restart", Ex6ServiceRestart},

	// The rest of the ex6 restart surface (added 2026-09-08). 64 covers restart -> delta, the
	// common case; these four cover every other branch the change touches.
	//   65 restart -> SNAPSHOT, the path `baselinePending` does not reach and that job 2's
	//      `lastSeq.clear()` fixes. The only shape where the counter moves backwards across a
	//      re-anchor, which is what makes the bug reachable at all.
	//   66 restart arriving while a resync is already PENDING — one control command in total,
	//      because the restart answers the outstanding request instead of adding to it. This
	//      is the direct e2e for the "an exchange reset must be silent" requirement.
	//   67 a DELTA claiming u == 1 is NOT a restart — pins the type half of the parser guard.
	//   68 a restart whose cts is behind the last accepted event is dropped `out_of_order`.
	//      Current behaviour, asserted so it is visible; see the header on data_ex6.go.
	// See data_ex6.go.
	{"65-ex6-restart-then-snapshot", Ex6RestartThenSnapshot},
	{"66-ex6-restart-answers-pending-resync", Ex6RestartAnswersPendingResync},
	{"67-ex6-u1-delta-is-not-a-restart", Ex6UOneOnADeltaIsNotARestart},
	{"68-ex6-restart-out-of-order", Ex6RestartOutOfOrder},
}
