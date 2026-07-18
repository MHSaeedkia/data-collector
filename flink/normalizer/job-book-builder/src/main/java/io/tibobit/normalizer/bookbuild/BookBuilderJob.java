package io.tibobit.normalizer.bookbuild;

import io.tibobit.normalizer.model.OrderBookSnapshot;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import io.tibobit.normalizer.serde.OrderBookSnapshotSerializer;
import io.tibobit.normalizer.serde.RawOrderBookEventDeserializer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.TopicSelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.regex.Pattern;

/**
 * Job 5 entry point: book builder.
 *
 * Pipeline:
 *   Kafka input  ex{id}-p{id}-applied-precision-flink  (RawOrderBookEvent, subject raw-order-book-event)
 *     -> source (regex) -> keyBy(exchange_id, pair_id)
 *     -> BookBuildFunction (apply snapshot/update to keyed state, emit the full book)
 *     -> Kafka output  ex{id}-p{id}-orderbook-snapshot-flink  (subject order-book-snapshot)
 *
 * No dead-letter: everything arriving here was already validated by job 2.
 */
public class BookBuilderJob {

    private static final Pattern INPUT_TOPIC_PATTERN =
            Pattern.compile("ex[0-9]+-p[0-9]+-applied-precision-flink");

    public static void main(String[] args) throws Exception {
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String groupId = getEnv("KAFKA_GROUP_ID", "normalizer-book-builder");
        String schemaRegistryUrl = getEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8082");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<RawOrderBookEvent> source = KafkaSource.<RawOrderBookEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(INPUT_TOPIC_PATTERN)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new RawOrderBookEventDeserializer(schemaRegistryUrl))
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "applied-precision-source")
                .keyBy(new ExchangePairKey())
                .process(new BookBuildFunction())
                .name("build-book")
                .sinkTo(KafkaSink.<OrderBookSnapshot>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setRecordSerializer(KafkaRecordSerializationSchema.<OrderBookSnapshot>builder()
                                .setTopicSelector((TopicSelector<OrderBookSnapshot>) book ->
                                        "ex" + book.getExchangeId() + "-p" + book.getPairId()
                                                + "-orderbook-snapshot-flink")
                                .setValueSerializationSchema(new OrderBookSnapshotSerializer(schemaRegistryUrl))
                                .build())
                        .build())
                .name("orderbook-snapshot-sink");

        env.execute("normalizer-book-builder");
    }

    /** keyBy (exchange_id, pair_id) — named class, not a lambda (Flink key-type inference). */
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
