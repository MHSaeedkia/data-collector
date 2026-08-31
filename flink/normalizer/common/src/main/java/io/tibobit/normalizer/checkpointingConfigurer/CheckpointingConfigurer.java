package io.tibobit.normalizer.checkpointingConfigurer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public final class CheckpointingConfigurer {

    private CheckpointingConfigurer() {
    }

    public static void configure(StreamExecutionEnvironment env) {
        long intervalMs = Long.parseLong(getEnv("CHECKPOINT_INTERVAL_MS", "10000"));
        String checkpointDir = getEnv("CHECKPOINT_DIR", "file:///opt/flink/checkpoints");

        env.enableCheckpointing(intervalMs);

        CheckpointConfig config = env.getCheckpointConfig();
        config.setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
        config.setCheckpointStorage(new FileSystemCheckpointStorage(checkpointDir));
        config.setMinPauseBetweenCheckpoints(intervalMs / 5);
        config.setCheckpointTimeout(120_000);      
        config.setMaxConcurrentCheckpoints(1);
        config.setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        Configuration restartConfig = new Configuration();
        restartConfig.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        restartConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 5);
        restartConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10));
        env.getConfig().configure(restartConfig, null);
    }

    private static String getEnv(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}