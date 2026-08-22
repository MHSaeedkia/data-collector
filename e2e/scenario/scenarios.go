package scenario

// scenarios are the cases in numbered order. Each one warms the pipeline up for
// its own exchange before it runs, so they are independent and the list can be
// cut down while working on a single case.
var Scenarios = []struct {
	Name string
	S    Scenario
}{
	// Nobitex
	{"01-ex1-rest-then-ws-resync", Ex1RestThenWsResync},
	{"02-ex1-update-before-snapshot", Ex1UpdateBeforeSnapshot},
	{"03-ex1-sequence-gap", Ex1SequenceGap},
	{"04-ex1-noise-frames", Ex1NoiseFrames},
	{"05-ex1-stale-rest-replay", Ex1StaleRestReplay},
	{"06-ex1-precision-dust", Ex1PrecisionDust},
	{"07-ex1-rebase-toman", Ex1RebaseToman},
	{"08-ex1-rebase-scaled-unit", Ex1RebaseScaledUnit},

	// Bitpin
	{"09-ex2-rest-then-ws-resync", Ex2RestThenWsResync},
	{"10-ex2-update-before-snapshot", Ex2UpdateBeforeSnapshot},
	{"11-ex2-sequence-gap", Ex2SequenceGap},
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

	// Bybit
	{"31-ex6-snapshot-then-deltas", Ex6SnapshotThenDeltas},
	{"32-ex6-one-sided-delta", Ex6OneSidedDelta},
	{"33-ex6-sequence-gap", Ex6SequenceGap},
	{"34-ex6-no-baseline", Ex6NoBaseline},
	{"35-ex6-noise-frames", Ex6NoiseFrames},
	{"36-ex6-precision-dust", Ex6PrecisionDust},

	// OKX
	{"37-ex8-update-before-snapshot", Ex8UpdateBeforeSnapshot},
	{"38-ex8-happy-path", Ex8HappyPath},
	{"39-ex8-sequence-gap", Ex8SequenceGap},
	{"40-ex8-stale-duplicate", Ex8StaleDuplicate},
	{"41-ex8-precision-dust", Ex8PrecisionDust},
	{"42-ex8-noise-frames", Ex8NoiseFrames},

	// Control plane — the snapshot requests job 2 sends NiFi. Grouped by feature
	// rather than by exchange; see data_control.go.
	{"43-control-ex6-gap-resync-gap", ControlEx6GapResyncGap},
	{"44-control-ex1-no-baseline-then-gap", ControlEx1NoBaselineThenGap},

	// Control plane — the resync the ordering guards used to throw away, which
	// is the deadlock fixed on 2026-08-19. 45 is the sequenced guard, 46 the
	// event-time one; both prove the resync was ACCEPTED and that the episode
	// re-armed. See data_control.go.
	{"45-control-ex6-stale-resync-accepted", ControlEx6StaleResyncAccepted},
	{"46-control-ex1-lagging-rest-resync", ControlEx1LaggingRestResync},
}
