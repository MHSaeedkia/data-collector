package io.tibobit.normalizer.aggregate;

import io.tibobit.normalizer.model.OrderBookSnapshot;
import io.tibobit.normalizer.serde.OrderBookSnapshotDeserializer;

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
 * Terminal job: cross-exchange aggregator.
 *
 * Pipeline:
 *   Kafka input  ex{id}-p{id}-orderbook-snapshot-flink  (OrderBookSnapshot, subject order-book-snapshot)
 *     -> source (regex)
 *     -> flatMap SnapshotSplitter (one snapshot -> asks + bids ExchangeBook)
 *     -> keyBy(pair_id, side) -> CrossExchangeAggregator (union across exchanges, sort)
 *     -> Kafka output  p{id}-{side}  (subject aggregated-order-book-event — the frozen web contract)
 *
 * Consumes job 5's full per-exchange books directly. Job 2's reset marker becomes an empty book
 * in job 5, which drops that exchange from the union here. No dead-letter: everything arriving
 * here is valid.
 */
public class AggregatorJob {

    private static final Pattern INPUT_TOPIC_PATTERN =
            Pattern.compile("ex[0-9]+-p[0-9]+-orderbook-snapshot-flink");

    public static void main(String[] args) throws Exception {
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String groupId = getEnv("KAFKA_GROUP_ID", "normalizer-aggregator");
        String schemaRegistryUrl = getEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8082");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<OrderBookSnapshot> source = KafkaSource.<OrderBookSnapshot>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(INPUT_TOPIC_PATTERN)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new OrderBookSnapshotDeserializer(schemaRegistryUrl))
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "orderbook-snapshot-source")
                .flatMap(new SnapshotSplitter())
                .name("split-sides")
                .keyBy(new PairSideKey())
                .process(new CrossExchangeAggregator())
                .name("aggregate")
                .sinkTo(KafkaSink.<AggregatedOrderBook>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setRecordSerializer(KafkaRecordSerializationSchema.<AggregatedOrderBook>builder()
                                // Route each record to p{pair_id}-{side} (e.g. p1-asks).
                                .setTopicSelector((TopicSelector<AggregatedOrderBook>) book ->
                                        "p" + book.getPairId() + "-" + book.getSide())
                                .setValueSerializationSchema(new AggregatedOrderBookSerializer(schemaRegistryUrl))
                                .build())
                        .build())
                .name("aggregated-order-book-sink");

        env.execute("normalizer-aggregator");
    }

    /** keyBy (pair_id, side) — named class, not a lambda (Flink key-type inference). */
    private static final class PairSideKey implements KeySelector<ExchangeBook, String> {
        @Override
        public String getKey(ExchangeBook book) {
            return book.getPairId() + "|" + book.getSide();
        }
    }

    private static String getEnv(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
