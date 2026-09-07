package io.tibobit.normalizer.checkpoint;

import java.time.Duration;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Wires production checkpointing onto a job's environment. Call once, at the top of each
 * normalizer job's {@code main()}, before any source or sink is built.
 *
 * <p>Every key below is applied through {@code env.configure(Configuration, ClassLoader)} rather
 * than {@code env.getCheckpointConfig().configure(...)}. Verified against the flink-core 2.2.0
 * jar: {@code CheckpointConfig.configure()} reads the interval/mode/timeout/min-pause/
 * max-concurrent/tolerable-failure/externalized-retention keys but silently ignores
 * {@code CheckpointingOptions.CHECKPOINT_STORAGE} — only {@code CHECKPOINTS_DIRECTORY} has a
 * setter it calls. {@code StreamExecutionEnvironment.configure()} both {@code addAll}s every key
 * into the environment's own {@code Configuration} (which is what {@code CheckpointStorageLoader}
 * actually reads to pick the storage implementation) <em>and</em> delegates to
 * {@code CheckpointConfig.configure()} for the rest, so one call covers all of it. Flink 2.x key
 * names are {@code execution.checkpointing.storage} / {@code .dir} — the 1.x
 * {@code state.checkpoint-storage} / {@code state.checkpoints.dir} names no longer exist.
 */
public final class CheckpointingConfigurer {

    /**
     * A record written under EXACTLY_ONCE is invisible downstream until the producing job's
     * transaction commits at the next checkpoint, and this pipeline chains 6 such jobs — visible
     * latency is roughly {@code hops x interval / 2}. 10s keeps that bounded (~30s average / 60s
     * worst case across the chain) while staying long enough that six sinks' pooled transactional
     * producers committing on this cadence cost the broker little: unlike the reverted round,
     * {@code TransactionNamingStrategy.POOLING} (see the per-job sink config) reuses a small fixed
     * pool of transactional ids rather than minting a new one every checkpoint, so shortening this
     * interval further is a latency/overhead trade now, not a repeat of the producer-id-churn
     * broker OOM that forced the previous revert.
     */
    public static final long CHECKPOINT_INTERVAL_MS = 10_000L;

    /** Generous relative to the interval; a slow checkpoint should fail, not hang the job. */
    public static final long CHECKPOINT_TIMEOUT_MS = 120_000L;

    /** Guarantees the next checkpoint request never overlaps a slow one, given max-concurrent=1. */
    public static final long MIN_PAUSE_BETWEEN_CHECKPOINTS_MS = 5_000L;

    public static final int MAX_CONCURRENT_CHECKPOINTS = 1;

    /**
     * Flink's default is 0 — one failed checkpoint fails the job outright, which is exactly how a
     * root-owned checkpoint volume turned into a permanent restart loop the last time this landed.
     * 3 turns a transient failure (a slow disk, a momentary broker hiccup) into an alert instead of
     * an outage, while a checkpoint that never succeeds still fails the job, just after 4 attempts
     * instead of 1.
     */
    public static final int TOLERABLE_FAILED_CHECKPOINTS = 3;

    /**
     * Max parallelism is fixed at the first checkpoint and cannot change afterwards without a
     * savepoint rescale. Every job runs at parallelism 1 today, but the source key space (one
     * partition per subscribed exchange+pair) grows with the market catalog; 128 leaves headroom
     * for that without ever needing to invalidate an existing checkpoint history to raise it.
     */
    public static final int MAX_PARALLELISM = 128;

    /**
     * Shared filesystem path across the JobManager and every TaskManager (one named volume,
     * mounted the same place everywhere) — required for filesystem checkpoint storage, where the
     * JobManager writes metadata and the TaskManagers write state files that must land somewhere
     * every node can see.
     */
    public static final String CHECKPOINT_DIR = "file:///opt/flink/checkpoints";

    private CheckpointingConfigurer() {
    }

    public static void configure(StreamExecutionEnvironment env) {
        Configuration config = new Configuration();

        config.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMillis(CHECKPOINT_INTERVAL_MS));
        config.set(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE, CheckpointingMode.EXACTLY_ONCE);
        config.set(CheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofMillis(CHECKPOINT_TIMEOUT_MS));
        config.set(CheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS,
                Duration.ofMillis(MIN_PAUSE_BETWEEN_CHECKPOINTS_MS));
        config.set(CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, MAX_CONCURRENT_CHECKPOINTS);
        config.set(CheckpointingOptions.TOLERABLE_FAILURE_NUMBER, TOLERABLE_FAILED_CHECKPOINTS);

        // A deploy here means cancel-and-resubmit with no savepoint (run-job.sh passes none), so a
        // checkpoint retained past cancellation would just leak forever. Matches the project's
        // standing decision to always restart at `latest` and re-baseline from a fresh snapshot
        // rather than resume from a possibly-stale one — DELETE_ON_CANCELLATION governs
        // cancellation only; a job that FAILS still keeps its checkpoint and restores from it.
        config.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.DELETE_ON_CANCELLATION);

        // Unaligned checkpoints trade complexity for faster checkpointing under sustained
        // back-pressure. Nothing observed on this pipeline shows sustained back-pressure today
        // (see monitoring's backPressuredTimeMsPerSecond rule); revisit if that alert fires, not on
        // a schedule.
        config.set(CheckpointingOptions.ENABLE_UNALIGNED, false);

        // hashmap keeps every book on heap, which is appropriate while state is a handful of small
        // per-market MapStates/ValueStates. Incremental checkpointing is RocksDB-only and does not
        // apply here — explicitly off rather than left to a default, so the choice is visible.
        // Move to rocksdb (and incremental) only if heap utilization actually demands it.
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, false);

        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, CHECKPOINT_DIR);

        env.configure(config, CheckpointingConfigurer.class.getClassLoader());
        env.setMaxParallelism(MAX_PARALLELISM);

        // Deliberately NOT set here: any restart-strategy key. The cluster-wide
        // restart-strategy.exponential-delay policy in the compose files must win — a prior
        // version of this class set a job-level fixed-delay strategy that silently overrode it.
    }
}
