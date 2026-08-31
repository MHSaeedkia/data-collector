package io.tibobit.normalizer.checkpointingConfigurer;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class CheckpointingConfigurer {

    private CheckpointingConfigurer() {
    }

    public static void configure(StreamExecutionEnvironment env) {
        long intervalMs = Long.parseLong(getEnv("CHECKPOINT_INTERVAL_MS", "10000"));
        String checkpointDir = getEnv("CHECKPOINT_DIR", "file:///opt/flink/checkpoints");

        env.enableCheckpointing(intervalMs);

        CheckpointConfig config = env.getCheckpointConfig();
        config.setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
        // CheckpointConfig has no setCheckpointStorage(CheckpointStorage) in Flink 2.x
        // (removed along with the StateBackend/CheckpointStorage fluent API) — storage
        // is config-key driven now.
        Configuration storageConfig = new Configuration();
        storageConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        storageConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
        config.configure(storageConfig);
        config.setMinPauseBetweenCheckpoints(intervalMs / 5);
        config.setCheckpointTimeout(120_000);      
        config.setMaxConcurrentCheckpoints(1);
        config.setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        // Restart strategy is intentionally left to the cluster-wide config
        // (docker-compose.yml sets restart-strategy.type: exponential-delay with
        // unlimited retries) rather than overridden here.
    }

    private static String getEnv(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
