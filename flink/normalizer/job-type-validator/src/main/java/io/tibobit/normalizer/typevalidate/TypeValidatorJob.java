package io.tibobit.normalizer.typevalidate;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.TopicSelector;
import org.apache.flink.connector.kafka.sink.TransactionNamingStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.tibobit.normalizer.checkpointingConfigurer.CheckpointingConfigurer;
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

        // Staleness watch list. The thresholds themselves are per market and live in
        // exchange_markets.staleness_threshold_seconds — these three only say how to
        // reach the DB, and REFRESH_INTERVAL_MS how often to re-read the list so a
        // threshold edit or an unsubscribe lands without resubmitting the job.
        //
        // REFRESH_INTERVAL_MS must stay WELL BELOW the smallest
        // staleness_threshold_seconds in the table. Both clocks start at the same
        // moment on an unsubscribe — the feed stops, and the silence deadline is one
        // threshold away — so at 60s against a 60s threshold it is a coin flip whether
        // the timer finds the market still listed. Losing that race is not fatal (the
        // stale branch empties the book too, and NiFi must ignore a request for an
        // unsubscribed market), but it sends a snapshot_request nobody should act on.
        // A quarter of the smallest threshold keeps the unsubscribe on the clean path.
        String postgresUrl = getEnv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/markets");
        String postgresUser = getEnv("POSTGRES_USER", "postgres");
        String postgresPassword = getEnv("POSTGRES_PASSWORD", "postgres");
        long refreshIntervalMs = Long.parseLong(getEnv("REFRESH_INTERVAL_MS", "15000"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointingConfigurer.configure(env);

        KafkaSource<RawOrderBookEvent> source = KafkaSource.<RawOrderBookEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(INPUT_TOPIC_PATTERN)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                // Job 1's sink writes transactionally (EXACTLY_ONCE); without this a
                // read_uncommitted consumer would see records from transactions that
                // later abort.
                .setProperty("isolation.level", "read_committed")
                .setValueOnlyDeserializer(new RawOrderBookEventDeserializer(schemaRegistryUrl))
                .build();

        // The watch list rides along inside the operator: it is only ever read by key,
        // so there is no second stream and no extra shuffle. A market missing from it is
        // simply not watched for silence.
        RefreshingLookup<String, WatchedMarket> watched = new RefreshingLookup<>(
                new StalenessThresholdLoader(postgresUrl, postgresUser, postgresPassword),
                refreshIntervalMs);

        SingleOutputStreamOperator<RawOrderBookEvent> validated = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "raw-flink-source")
                .keyBy(new ExchangePairKey())
                .process(new TypeValidateFunction(snapshotRetryMs, watched))
                .name("type-validate");

        // Valid events -> ex{id}-p{id}-type-validated-raw-flink (same shared
        // raw-order-book-event schema).
        validated.sinkTo(KafkaSink.<RawOrderBookEvent>builder()
                .setBootstrapServers(bootstrapServers)
                // EXACTLY_ONCE below commits transactionally on checkpoint completion
                // (CheckpointingConfigurer). acks=all + idempotence are required for
                // transactional Kafka writes; unlimited retries absorb transient
                // broker hiccups without failing the transaction.
                .setProperty("acks", "all")
                .setProperty("enable.idempotence", "true")
                // KafkaSink defaults transaction.timeout.ms to 1h, which exceeds
                // the broker's transaction.max.timeout.ms (15m default) and fails
                // InitProducerId. Keep this comfortably under that ceiling.
                .setProperty("transaction.timeout.ms", "600000")
                .setProperty("retries", "2147483647")
                .setProperty("delivery.timeout.ms", "120000")
                .setRecordSerializer(KafkaRecordSerializationSchema.<RawOrderBookEvent>builder()
                        .setTopicSelector((TopicSelector<RawOrderBookEvent>) event -> "ex"
                        + event.getExchangeId() + "-p" + event.getPairId()
                        + "-type-validated-raw-flink")
                        .setValueSerializationSchema(
                                new RawOrderBookEventSerializer(schemaRegistryUrl))
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("job2-type-validated")
                // POOLING rather than the INCREMENTING default. INCREMENTING mints a NEW
                // transactional.id — and so a NEW producer id — on EVERY checkpoint, and the
                // broker holds each dead id's state for transactional.id.expiration.ms plus an
                // entry in the producer state of every partition that transaction touched. At a
                // 10s interval over ~3000 partitions that is what OOM'd the 1 GB broker daily
                // (2026-09-02..04). POOLING reuses a small fixed pool of ids instead.
                // One-way switch: INCREMENTING -> POOLING is supported, the reverse is not.
                .setTransactionNamingStrategy(TransactionNamingStrategy.POOLING)
                .build())
                .name("type-validated-sink");

        // Rejects -> dead-letter ex{id}-p{id}-rejected-flink (subject
        // rejected-order-book-event).
        DataStream<RejectedOrderBookEvent> rejected = validated.getSideOutput(TypeValidateFunction.REJECTED);
        rejected.sinkTo(KafkaSink.<RejectedOrderBookEvent>builder()
                .setBootstrapServers(bootstrapServers)
                // EXACTLY_ONCE below commits transactionally on checkpoint completion
                // (CheckpointingConfigurer). acks=all + idempotence are required for
                // transactional Kafka writes; unlimited retries absorb transient
                // broker hiccups without failing the transaction.
                .setProperty("acks", "all")
                .setProperty("enable.idempotence", "true")
                // KafkaSink defaults transaction.timeout.ms to 1h, which exceeds
                // the broker's transaction.max.timeout.ms (15m default) and fails
                // InitProducerId. Keep this comfortably under that ceiling.
                .setProperty("transaction.timeout.ms", "600000")
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
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("job2-rejected")
                // POOLING rather than the INCREMENTING default. INCREMENTING mints a NEW
                // transactional.id — and so a NEW producer id — on EVERY checkpoint, and the
                // broker holds each dead id's state for transactional.id.expiration.ms plus an
                // entry in the producer state of every partition that transaction touched. At a
                // 10s interval over ~3000 partitions that is what OOM'd the 1 GB broker daily
                // (2026-09-02..04). POOLING reuses a small fixed pool of ids instead.
                // One-way switch: INCREMENTING -> POOLING is supported, the reverse is not.
                .setTransactionNamingStrategy(TransactionNamingStrategy.POOLING)
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
