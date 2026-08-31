package io.tibobit.normalizer.rebase;

import io.tibobit.normalizer.checkpointingConfigurer.CheckpointingConfigurer;
import io.tibobit.normalizer.lookup.RefreshingLookup;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import io.tibobit.normalizer.model.RejectedOrderBookEvent;
import io.tibobit.normalizer.serde.RawOrderBookEventDeserializer;
import io.tibobit.normalizer.serde.RawOrderBookEventSerializer;
import io.tibobit.normalizer.serde.RejectedOrderBookEventSerializer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.TopicSelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.regex.Pattern;

/**
 * Job 3 entry point: rebase.
 *
 * Pipeline:
 *   Kafka input  ex{id}-p{id}-type-validated-raw-flink  (RawOrderBookEvent, subject raw-order-book-event)
 *     -> source (regex) -> RebaseFunction (price/quantity x 10^rebase, exponents from exchange_markets)
 *     -> main   Kafka output  ex{id}-p{id}-rebased-flink   (subject raw-order-book-event)
 *     -> reject Kafka output  ex{id}-p{id}-rejected-flink  (subject rejected-order-book-event)
 *
 * Stateless and unkeyed — the rebase of one event never depends on another.
 */
public class RebaserJob {

    private static final Pattern INPUT_TOPIC_PATTERN =
            Pattern.compile("ex[0-9]+-p[0-9]+-type-validated-raw-flink");

    public static void main(String[] args) throws Exception {
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String groupId = getEnv("KAFKA_GROUP_ID", "normalizer-rebaser");
        String schemaRegistryUrl = getEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8082");
        String postgresUrl = getEnv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/markets");
        String postgresUser = getEnv("POSTGRES_USER", "postgres");
        String postgresPassword = getEnv("POSTGRES_PASSWORD", "postgres");
        long refreshIntervalMs = Long.parseLong(getEnv("REFRESH_INTERVAL_MS", "60000"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointingConfigurer.configure(env); 
        KafkaSource<RawOrderBookEvent> source = KafkaSource.<RawOrderBookEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(INPUT_TOPIC_PATTERN)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                // Job 2's sinks write transactionally (EXACTLY_ONCE); without this a
                // read_uncommitted consumer would see records from transactions that
                // later abort.
                .setProperty("isolation.level", "read_committed")
                .setValueOnlyDeserializer(new RawOrderBookEventDeserializer(schemaRegistryUrl))
                .build();

        RefreshingLookup<String, RebaseFactors> factors = new RefreshingLookup<>(
                new RebaseFactorsLoader(postgresUrl, postgresUser, postgresPassword),
                refreshIntervalMs);

        SingleOutputStreamOperator<RawOrderBookEvent> rebased = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "type-validated-source")
                .process(new RebaseFunction(factors))
                .name("rebase");

        // Rebased events -> ex{id}-p{id}-rebased-flink (same shared raw-order-book-event schema).
        rebased.sinkTo(KafkaSink.<RawOrderBookEvent>builder()
                        .setBootstrapServers(bootstrapServers)
                        // EXACTLY_ONCE below commits transactionally on checkpoint completion
                        // (CheckpointingConfigurer). acks=all + idempotence are required for
                        // transactional Kafka writes; unlimited retries absorb transient
                        // broker hiccups without failing the transaction.
                        .setProperty("acks", "all")
                        .setProperty("enable.idempotence", "true")
                        .setProperty("retries", "2147483647")
                        .setProperty("delivery.timeout.ms", "120000")
                        .setRecordSerializer(KafkaRecordSerializationSchema.<RawOrderBookEvent>builder()
                                .setTopicSelector((TopicSelector<RawOrderBookEvent>) event ->
                                        "ex" + event.getExchangeId() + "-p" + event.getPairId()
                                                + "-rebased-flink")
                                .setValueSerializationSchema(new RawOrderBookEventSerializer(schemaRegistryUrl))
                                .build())
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setTransactionalIdPrefix("job3-rebased")
                        .build())
                .name("rebased-sink");

        // Missing exchange_markets row -> the SAME dead-letter topic job 2 writes.
        DataStream<RejectedOrderBookEvent> rejected = rebased.getSideOutput(RebaseFunction.REJECTED);
        rejected.sinkTo(KafkaSink.<RejectedOrderBookEvent>builder()
                        .setBootstrapServers(bootstrapServers)
                        // EXACTLY_ONCE below commits transactionally on checkpoint completion
                        // (CheckpointingConfigurer). acks=all + idempotence are required for
                        // transactional Kafka writes; unlimited retries absorb transient
                        // broker hiccups without failing the transaction.
                        .setProperty("acks", "all")
                        .setProperty("enable.idempotence", "true")
                        .setProperty("retries", "2147483647")
                        .setProperty("delivery.timeout.ms", "120000")
                        .setRecordSerializer(KafkaRecordSerializationSchema.<RejectedOrderBookEvent>builder()
                                .setTopicSelector((TopicSelector<RejectedOrderBookEvent>) rejection ->
                                        "ex" + rejection.getEvent().getExchangeId()
                                                + "-p" + rejection.getEvent().getPairId() + "-rejected-flink")
                                .setValueSerializationSchema(new RejectedOrderBookEventSerializer(schemaRegistryUrl))
                                .build())
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setTransactionalIdPrefix("job3-rejected")
                        .build())
                .name("rejected-sink");

        env.execute("normalizer-rebaser");
    }

    private static String getEnv(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
