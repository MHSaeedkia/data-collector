package io.tibobit.normalizer.checkpoint;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins every value {@link CheckpointingConfigurer} applies. These are production settings with a
 * documented incident history (2026-08-31 root-owned volume, 2026-09-05 producer-id-churn broker
 * OOM) — a silent regression here (e.g. tolerance dropping back to Flink's default 0, or storage
 * silently not applying because a future edit switches back to
 * {@code getCheckpointConfig().configure()}) reproduces a known outage, not a new bug.
 */
class CheckpointingConfigurerTest {

    private final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    @Test
    @DisplayName("enables EXACTLY_ONCE checkpointing at the documented interval")
    void enablesCheckpointing() {
        CheckpointingConfigurer.configure(env);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        assertThat(checkpointConfig.isCheckpointingEnabled()).isTrue();
        assertThat(checkpointConfig.getCheckpointInterval())
                .isEqualTo(CheckpointingConfigurer.CHECKPOINT_INTERVAL_MS);
        assertThat(checkpointConfig.getCheckpointingConsistencyMode()).isEqualTo(CheckpointingMode.EXACTLY_ONCE);
        assertThat(checkpointConfig.getCheckpointTimeout()).isEqualTo(CheckpointingConfigurer.CHECKPOINT_TIMEOUT_MS);
        assertThat(checkpointConfig.getMinPauseBetweenCheckpoints())
                .isEqualTo(CheckpointingConfigurer.MIN_PAUSE_BETWEEN_CHECKPOINTS_MS);
        assertThat(checkpointConfig.getMaxConcurrentCheckpoints())
                .isEqualTo(CheckpointingConfigurer.MAX_CONCURRENT_CHECKPOINTS);
    }

    @Test
    @DisplayName("raises tolerable failures above Flink's default-fails-on-first-failure 0")
    void toleratesTransientFailures() {
        CheckpointingConfigurer.configure(env);

        assertThat(env.getCheckpointConfig().getTolerableCheckpointFailureNumber())
                .isEqualTo(CheckpointingConfigurer.TOLERABLE_FAILED_CHECKPOINTS)
                .isGreaterThan(0);
    }

    @Test
    @DisplayName("deletes externalized checkpoints on cancellation, not retains")
    void deletesOnCancellation() {
        CheckpointingConfigurer.configure(env);

        assertThat(env.getCheckpointConfig().getExternalizedCheckpointRetention())
                .isEqualTo(ExternalizedCheckpointRetention.DELETE_ON_CANCELLATION);
    }

    @Test
    @DisplayName("sets filesystem checkpoint storage and directory on the environment's own config")
    void setsFilesystemStorage() {
        // Regression guard for the 2026-09-02 fix: CheckpointConfig.configure() alone silently
        // drops CHECKPOINT_STORAGE (only CHECKPOINTS_DIRECTORY has a setter it calls), so this
        // must be readable back from the ENVIRONMENT's configuration, not only CheckpointConfig.
        CheckpointingConfigurer.configure(env);

        assertThat(env.getConfiguration().get(CheckpointingOptions.CHECKPOINT_STORAGE)).isEqualTo("filesystem");
        assertThat(env.getConfiguration().get(CheckpointingOptions.CHECKPOINTS_DIRECTORY))
                .isEqualTo(CheckpointingConfigurer.CHECKPOINT_DIR);
    }

    @Test
    @DisplayName("uses the hashmap state backend, explicitly, with incremental checkpoints off")
    void usesHashMapBackend() {
        CheckpointingConfigurer.configure(env);

        assertThat(env.getConfiguration().get(StateBackendOptions.STATE_BACKEND)).isEqualTo("hashmap");
        assertThat(env.getConfiguration().get(CheckpointingOptions.INCREMENTAL_CHECKPOINTS)).isFalse();
    }

    @Test
    @DisplayName("leaves unaligned checkpoints off")
    void unalignedCheckpointsOff() {
        CheckpointingConfigurer.configure(env);

        assertThat(env.getConfiguration().get(CheckpointingOptions.ENABLE_UNALIGNED)).isFalse();
    }

    @Test
    @DisplayName("raises max parallelism above the current parallelism-1 floor")
    void raisesMaxParallelism() {
        CheckpointingConfigurer.configure(env);

        assertThat(env.getMaxParallelism()).isEqualTo(CheckpointingConfigurer.MAX_PARALLELISM);
    }

    @Test
    @DisplayName("never touches the cluster-wide restart strategy")
    void leavesRestartStrategyAlone() {
        // A prior version of this class set a job-level fixed-delay restart strategy that
        // silently overrode the cluster's exponential-delay policy (docker-compose*.yml). Guard
        // against that regression: after configure(), the environment's own restart-strategy
        // config keys must still be whatever they were before (i.e. absent — nothing set them).
        CheckpointingConfigurer.configure(env);

        assertThat(env.getConfiguration().toMap()).doesNotContainKey("restart-strategy.type");
    }
}
