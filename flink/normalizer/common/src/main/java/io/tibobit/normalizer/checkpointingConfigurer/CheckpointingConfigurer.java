package io.tibobit.normalizer.checkpointingConfigurer;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class CheckpointingConfigurer {

    /**
     * How many consecutive checkpoint failures a job absorbs before it fails itself.
     * Flink's default is 0 — one failed checkpoint kills the job — which is how a
     * root-owned checkpoint volume turned into a permanent 6-job restart loop on the
     * first live run of this feature. A small budget makes a transient failure (a
     * blocked write, a slow disk) an alert instead of an outage, while still failing
     * the job on a genuine, persistent problem. FlinkCheckpointsFailing fires on the
     * FIRST failure regardless, so nothing is hidden by this.
     */
    private static final int TOLERABLE_CHECKPOINT_FAILURES = 3;

    private CheckpointingConfigurer() {
    }

    public static void configure(StreamExecutionEnvironment env) {
        long intervalMs = Long.parseLong(getEnv("CHECKPOINT_INTERVAL_MS", "10000"));
        String checkpointDir = getEnv("CHECKPOINT_DIR", "file:///opt/flink/checkpoints");

        env.enableCheckpointing(intervalMs);

        // Storage goes through env.configure(), NOT getCheckpointConfig().configure().
        // CheckpointConfig has no setCheckpointStorage(CheckpointStorage) in Flink 2.x
        // (removed along with the StateBackend/CheckpointStorage fluent API), so storage
        // is config-key driven — but CheckpointConfig.configure() reads
        // `execution.checkpointing.dir` and ignores `execution.checkpointing.storage`
        // entirely (verified against flink-runtime 2.2.0). env.configure() copies every
        // key into the environment's own configuration, which is what
        // CheckpointStorageLoader actually reads. Passing only the directory does work —
        // the loader falls back to filesystem whenever one is set — but it logs a warning
        // asking for the value to be explicit, and an undocumented fallback is not
        // something a pipeline should depend on.
        Configuration storageConfig = new Configuration();
        storageConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        storageConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
        env.configure(storageConfig);

        CheckpointConfig config = env.getCheckpointConfig();
        config.setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
        config.setMinPauseBetweenCheckpoints(intervalMs / 5);
        config.setCheckpointTimeout(120_000);
        config.setMaxConcurrentCheckpoints(1);
        config.setTolerableCheckpointFailureNumber(TOLERABLE_CHECKPOINT_FAILURES);
        // DELETE_ON_CANCELLATION, not RETAIN: nothing here can consume a retained
        // checkpoint. run-job.sh submits with no savepointPath, and run-all-jobs /
        // prod-deploy cancel all 8 jobs before resubmitting — so retention would strand
        // 8 directories on the shared volume per deploy, forever (num-retained prunes
        // only a RUNNING job's own history). That is unbounded growth on a named volume
        // with no disk alert, and the volume filling up is itself a checkpoint failure.
        // Retention also contradicts the standing decision to restart from `latest` and
        // re-baseline rather than resume possibly-wrong state (S1). Note this governs
        // CANCELLATION only: a job that FAILS still keeps its checkpoint, and automatic
        // restarts restore from it as normal — the actual point of the feature.
        config.setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.DELETE_ON_CANCELLATION);
        // Restart strategy is intentionally left to the cluster-wide config
        // (docker-compose.yml sets restart-strategy.type: exponential-delay with
        // unlimited retries) rather than overridden here.
    }

    private static String getEnv(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
