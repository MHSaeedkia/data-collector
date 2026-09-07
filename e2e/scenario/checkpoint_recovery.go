package scenario

import (
	"context"
	"fmt"
	"log/slog"

	"orderbook-e2e/config"
	"orderbook-e2e/flink"
	"orderbook-e2e/schemaregistry"
	"orderbook-e2e/warmup"
)

// checkpointedJobs are the Flink job names (the string each job's main()
// passes to env.execute) that CheckpointingConfigurer applies to. merger and
// adjustment are deliberately excluded — this round only covers the six
// chained normalizer jobs — so this test never waits on a checkpoint they
// do not have, and never expects their id to survive a crash.
var checkpointedJobs = []string{
	"normalizer-pair-extractor",
	"normalizer-type-validator",
	"normalizer-rebaser",
	"normalizer-precision",
	"normalizer-book-builder",
	"normalizer-aggregator",
}

// RunCheckpointRecovery is a live fault-injection test, not one of the
// data-driven cases in Scenarios. Those assert pipeline CORRECTNESS — does
// the right output come out for this input; this asserts INFRASTRUCTURE
// fault tolerance — does the pipeline survive losing a TaskManager
// mid-stream without losing or duplicating anything.
//
// It reuses Ex9SnapshotStream (three snapshots for one exchange/pair, no
// rejects, no control commands — nothing about a sequence/gap regime can
// produce a result that looks like a checkpointing failure but is not) and
// splits its sources across a simulated TaskManager crash: the first lands
// BEFORE the crash, the rest AFTER it. If checkpointing genuinely recovers
// job state and the EXACTLY_ONCE sinks genuinely commit exactly once each,
// the pipeline ends on exactly the book Ex9SnapshotStream already asserts
// for an uninterrupted run — this reuses Scenario.verify so the crash run is
// held to the same rigor as every other scenario, not a bespoke, weaker one.
//
// What this proves: no record was lost or duplicated across the crash, and
// every checkpointed job recovered IN PLACE — the same Flink job id after
// the crash as before. A fresh resubmission through warmup mints new ids;
// that is not what a TaskManager restart does, and this test fails loudly
// if it happens.
//
// What this does NOT prove: that any one job's internal keyed state
// specifically (e.g. job 2's per-market lastEventTime/lastSeq) is what
// survived, as opposed to the pipeline merely re-deriving the same answer
// from Kafka alone — Ex9's snapshot-replaces-whole-book semantics does not
// need continuity to reach the right final state. Replaying an earlier
// frame after the crash and asserting it is rejected out_of_order (which
// DOES require lastEventTime to have survived) would close that gap; left
// for a follow-up so this test's own failure modes stay easy to read.
func RunCheckpointRecovery(ctx context.Context, cfg config.Config) error {
	sc := Ex9SnapshotStream
	if len(sc.Sources) < 2 {
		return fmt.Errorf("checkpoint recovery test needs at least 2 sources, got %d", len(sc.Sources))
	}

	if err := schemaregistry.RegisterDir(cfg.SchemaRegistryURL, cfg.SchemasDir); err != nil {
		return err
	}
	if err := warmup.Run(ctx, cfg, sc.ExchangeID, sc.PairID); err != nil {
		return err
	}

	slog.Info("checkpoint-recovery: producing pre-crash batch", "sources", 1)
	if err := produceSources(ctx, cfg, sc.ExchangeID, sc.Sources[:1]); err != nil {
		return fmt.Errorf("pre-crash produce: %w", err)
	}

	before, err := flink.JobIDsByName(ctx, cfg.FlinkAPI)
	if err != nil {
		return fmt.Errorf("job ids before crash: %w", err)
	}
	checkpointedIDs := make([]string, 0, len(checkpointedJobs))
	for _, name := range checkpointedJobs {
		id, ok := before[name]
		if !ok {
			return fmt.Errorf("job %q is not RUNNING before the crash", name)
		}
		checkpointedIDs = append(checkpointedIDs, id)
	}

	slog.Info("checkpoint-recovery: waiting for every checkpointed job to complete a checkpoint")
	for _, id := range checkpointedIDs {
		if err := flink.WaitForCheckpoint(ctx, cfg.FlinkAPI, id); err != nil {
			return fmt.Errorf("waiting for checkpoint: %w", err)
		}
	}

	slog.Info("checkpoint-recovery: restarting task manager(s)", "services", cfg.TaskManagerServices)
	if err := flink.RestartTaskManagers(ctx, cfg.ComposeFile, cfg.TaskManagerServices...); err != nil {
		return fmt.Errorf("restart task managers: %w", err)
	}

	if err := flink.WaitAllRunning(ctx, cfg.FlinkAPI, checkpointedIDs); err != nil {
		return fmt.Errorf("recovery: %w", err)
	}

	after, err := flink.JobIDsByName(ctx, cfg.FlinkAPI)
	if err != nil {
		return fmt.Errorf("job ids after crash: %w", err)
	}
	for _, name := range checkpointedJobs {
		id, ok := after[name]
		if !ok {
			return fmt.Errorf("job %q is not RUNNING after recovery", name)
		}
		if id != before[name] {
			return fmt.Errorf("job %q recovered as a NEW job id (%s -> %s) — that is a fresh "+
				"resubmission, not a checkpoint recovery", name, before[name], id)
		}
	}

	slog.Info("checkpoint-recovery: producing post-crash batch", "sources", len(sc.Sources)-1)
	if err := produceSources(ctx, cfg, sc.ExchangeID, sc.Sources[1:]); err != nil {
		return fmt.Errorf("post-crash produce: %w", err)
	}

	// Same assertion every ordinary scenario makes — the full, uninterrupted-run
	// expectation — against a run that WAS interrupted. A mismatch here means
	// the crash cost (or duplicated) something checkpointing exists to prevent.
	return sc.verify(ctx, cfg)
}
