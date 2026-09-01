package io.tibobit.normalizer.typevalidate;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.TopicSelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.tibobit.normalizer.lookup.RefreshingLookup;
import io.tibobit.normalizer.model.ControlCommand;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import io.tibobit.normalizer.model.RejectedOrderBookEvent;
import io.tibobit.normalizer.serde.ControlCommandSerializer;
import io.tibobit.normalizer.serde.RawOrderBookEventDeserializer;
import io.tibobit.normalizer.serde.RawOrderBookEventSerializer;
import io.tibobit.normalizer.serde.RejectedOrderBookEventSerializer;

/**
 * Job 2 entry point: type validation.
 *
 * Pipeline: Kafka input ex{id}-p{id}-raw-flink (RawOrderBookEvent, subject
 * raw-order-book-event) -> source (regex) -> keyBy(exchange_id, pair_id) ->
 * TypeValidateFunction (snapshot/update sequence rules) -> main Kafka output
 * ex{id}-p{id}-type-validated-raw-flink (subject raw-order-book-event) ->
 * reject Kafka output ex{id}-p{id}-rejected-flink (subject
 * rejected-order-book-event)
 */
public class TypeValidatorJob {

    private static final Pattern INPUT_TOPIC_PATTERN = Pattern.compile("ex[0-9]+-p[0-9]+-raw-flink");

    public static void main(String[] args) throws Exception {
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String groupId = getEnv("KAFKA_GROUP_ID", "normalizer-type-validator");
        String schemaRegistryUrl = getEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8082");
        // How long to wait before re-asking NiFi for a snapshot that never arrived. One
        // request per episode is not enough on its own: if the command is lost or nothing
        // is consuming control-plane, the market stays dark until the job restarts.
        long snapshotRetryMs = Long.parseLong(getEnv("SNAPSHOT_RETRY_MS",
                String.valueOf(TypeValidateFunction.DEFAULT_SNAPSHOT_RETRY_MS)));

        // Staleness watch. The thresholds themselves are per market and live in
        // exchange_markets.staleness_threshold_seconds — these three only say how to
        // reach the DB, and REFRESH_INTERVAL_MS how often to re-read the watch list so a
        // threshold edit or a new subscription lands without resubmitting the job.
        String postgresUrl = getEnv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/markets");
        String postgresUser = getEnv("POSTGRES_USER", "postgres");
        String postgresPassword = getEnv("POSTGRES_PASSWORD", "postgres");
        long refreshIntervalMs = Long.parseLong(getEnv("REFRESH_INTERVAL_MS", "60000"));
        // How often every watched market is re-checked. Must be well under the smallest
        // staleness_threshold_seconds (default 60) or a market stays stale for up to one
        // extra poll before anyone notices.
        long stalenessPollMs = Long.parseLong(getEnv("STALENESS_POLL_MS", "10000"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<RawOrderBookEvent> source = KafkaSource.<RawOrderBookEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(INPUT_TOPIC_PATTERN)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new RawOrderBookEventDeserializer(schemaRegistryUrl))
                .build();

        // The staleness clock. A rate-limited generator supplies bare pulses and the
        // fan-out turns each into one tick per subscribed market, so the CLOCK is fixed
        // at submit time while the WATCH LIST is re-read from Postgres as the job runs.
        //
        // Parallelism 1 on both, deliberately: each parallel instance would hold the same
        // full watch list and emit the same ticks, multiplying every market's re-ask rate
        // by the parallelism.
        DataGeneratorSource<Long> pulses = new DataGeneratorSource<>(
                (GeneratorFunction<Long, Long>) index -> index,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1000.0 / stalenessPollMs),
                Types.LONG);

        DataStream<StalenessTick> ticks = env
                .fromSource(pulses, WatermarkStrategy.noWatermarks(), "staleness-pulse")
                .setParallelism(1)
                .flatMap(new StalenessTickFanOut(new RefreshingLookup<>(
                        new StalenessThresholdLoader(postgresUrl, postgresUser,
                                postgresPassword),
                        refreshIntervalMs)))
                .name("staleness-ticks")
                .setParallelism(1);

        // Both streams keyed the SAME way, so a tick lands on the state of the market it
        // is asking about. If these two key selectors ever diverge, connect() pairs ticks
        // with the wrong market's state and every verdict is silently wrong.
        SingleOutputStreamOperator<RawOrderBookEvent> validated = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "raw-flink-source")
                .keyBy(new ExchangePairKey())
                .connect(ticks.keyBy(StalenessTick::key))
                .process(new TypeValidateFunction(snapshotRetryMs))
                .name("type-validate");

        // Valid events -> ex{id}-p{id}-type-validated-raw-flink (same shared
        // raw-order-book-event schema).
        validated.sinkTo(KafkaSink.<RawOrderBookEvent>builder()
                .setBootstrapServers(bootstrapServers)
                // Without checkpointing, DeliveryGuarantee is NONE and a broker-side
                // failure drops records silently. Idempotence is the load-bearing one:
                // plain retries can reorder writes, which corrupts the book downstream.
                .setProperty("acks", "all")
                .setProperty("enable.idempotence", "true")
                .setProperty("retries", "2147483647")
                .setProperty("delivery.timeout.ms", "120000")
                .setRecordSerializer(KafkaRecordSerializationSchema.<RawOrderBookEvent>builder()
                        .setTopicSelector((TopicSelector<RawOrderBookEvent>) event -> "ex"
                        + event.getExchangeId() + "-p" + event.getPairId()
                        + "-type-validated-raw-flink")
                        .setValueSerializationSchema(
                                new RawOrderBookEventSerializer(schemaRegistryUrl))
                        .build())
                .build())
                .name("type-validated-sink");

        // Rejects -> dead-letter ex{id}-p{id}-rejected-flink (subject
        // rejected-order-book-event).
        DataStream<RejectedOrderBookEvent> rejected = validated.getSideOutput(TypeValidateFunction.REJECTED);
        rejected.sinkTo(KafkaSink.<RejectedOrderBookEvent>builder()
                .setBootstrapServers(bootstrapServers)
                // Without checkpointing, DeliveryGuarantee is NONE and a broker-side
                // failure drops records silently. Idempotence is the load-bearing one:
                // plain retries can reorder writes, which corrupts the book downstream.
                .setProperty("acks", "all")
                .setProperty("enable.idempotence", "true")
                .setProperty("retries", "2147483647")
                .setProperty("delivery.timeout.ms", "120000")
                .setRecordSerializer(KafkaRecordSerializationSchema.<RejectedOrderBookEvent>builder()
                        .setTopicSelector(
                                (TopicSelector<RejectedOrderBookEvent>) rejection -> "ex"
                                + rejection.getEvent().getExchangeId()
                                + "-p"
                                + rejection.getEvent().getPairId()
                                + "-rejected-flink")
                        .setValueSerializationSchema(
                                new RejectedOrderBookEventSerializer(schemaRegistryUrl))
                        .build())
                .build())
                .name("rejected-sink");

        // Control-plane -> shared control-plane topic, consumed by NiFi to trigger a
        // fresh snapshot.
        DataStream<ControlCommand> controlCommands = validated.getSideOutput(TypeValidateFunction.CONTROL);
        controlCommands.sinkTo(KafkaSink.<ControlCommand>builder()
                .setBootstrapServers(bootstrapServers)
                // Without checkpointing, DeliveryGuarantee is NONE and a broker-side
                // failure drops records silently. Idempotence is the load-bearing one:
                // plain retries can reorder writes, which corrupts the book downstream.
                .setProperty("acks", "all")
                .setProperty("enable.idempotence", "true")
                .setProperty("retries", "2147483647")
                .setProperty("delivery.timeout.ms", "120000")
                .setRecordSerializer(KafkaRecordSerializationSchema.<ControlCommand>builder()
                        .setTopic("control-plane")
                        .setKeySerializationSchema(cmd
                                -> (cmd.getExchangeId() + "|" + cmd.getPairId())
                                .getBytes(StandardCharsets.UTF_8))
                        .setValueSerializationSchema(new ControlCommandSerializer(schemaRegistryUrl))
                        .build())
                .build())
                .name("control-plane-sink");

        env.execute("normalizer-type-validator");
    }

    /**
     * keyBy (exchange_id, pair_id) — named class, not a lambda (Flink key-type
     * inference).
     */
    private static final class ExchangePairKey implements KeySelector<RawOrderBookEvent, String> {

        @Override
        public String getKey(RawOrderBookEvent event) {
            return event.getExchangeId() + "|" + event.getPairId();
        }
    }

    private static String getEnv(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
