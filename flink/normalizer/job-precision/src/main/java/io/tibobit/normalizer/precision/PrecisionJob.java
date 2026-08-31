package io.tibobit.normalizer.precision;

import io.tibobit.normalizer.lookup.RefreshingLookup;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import io.tibobit.normalizer.serde.RawOrderBookEventDeserializer;
import io.tibobit.normalizer.serde.RawOrderBookEventSerializer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.TopicSelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.regex.Pattern;

/**
 * Job 4 entry point: precision.
 *
 * Pipeline:
 *   Kafka input  ex{id}-p{id}-rebased-flink  (RawOrderBookEvent, subject raw-order-book-event)
 *     -> source (regex) -> PrecisionFunction (truncate DOWN to markets.{price,quantity}_precision)
 *     -> Kafka output  ex{id}-p{id}-applied-precision-flink  (subject raw-order-book-event)
 *
 * Stateless and unkeyed — one event's truncation never depends on another. No dead-letter: an
 * unconfigured precision is a passthrough, not a rejection (see PrecisionFunction).
 */
public class PrecisionJob {

    private static final Pattern INPUT_TOPIC_PATTERN =
            Pattern.compile("ex[0-9]+-p[0-9]+-rebased-flink");

    public static void main(String[] args) throws Exception {
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String groupId = getEnv("KAFKA_GROUP_ID", "normalizer-precision");
        String schemaRegistryUrl = getEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8082");
        String postgresUrl = getEnv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/markets");
        String postgresUser = getEnv("POSTGRES_USER", "postgres");
        String postgresPassword = getEnv("POSTGRES_PASSWORD", "postgres");
        long refreshIntervalMs = Long.parseLong(getEnv("REFRESH_INTERVAL_MS", "60000"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<RawOrderBookEvent> source = KafkaSource.<RawOrderBookEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(INPUT_TOPIC_PATTERN)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new RawOrderBookEventDeserializer(schemaRegistryUrl))
                .build();

        RefreshingLookup<Integer, MarketPrecision> precisions = new RefreshingLookup<>(
                new MarketPrecisionLoader(postgresUrl, postgresUser, postgresPassword),
                refreshIntervalMs);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "rebased-source")
                .map(new PrecisionFunction(precisions))
                .name("apply-precision")
                .sinkTo(KafkaSink.<RawOrderBookEvent>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setRecordSerializer(KafkaRecordSerializationSchema.<RawOrderBookEvent>builder()
                                .setTopicSelector((TopicSelector<RawOrderBookEvent>) event ->
                                        "ex" + event.getExchangeId() + "-p" + event.getPairId()
                                                + "-applied-precision-flink")
                                .setValueSerializationSchema(new RawOrderBookEventSerializer(schemaRegistryUrl))
                                .build())
                        .build())
                .name("applied-precision-sink");

        env.execute("normalizer-precision");
    }

    private static String getEnv(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
