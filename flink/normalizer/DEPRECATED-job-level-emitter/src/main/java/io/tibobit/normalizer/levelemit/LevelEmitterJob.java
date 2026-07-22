package io.tibobit.normalizer.levelemit;

import io.tibobit.normalizer.model.OrderBookSnapshot;
import io.tibobit.normalizer.model.PriceLevelEvent;
import io.tibobit.normalizer.serde.OrderBookSnapshotDeserializer;
import io.tibobit.normalizer.serde.PriceLevelEventSerializer;

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
 * Job 6 entry point: level emitter — the last job of the raw pipeline.
 *
 * Pipeline:
 *   Kafka input  ex{id}-p{id}-orderbook-snapshot-flink  (OrderBookSnapshot, subject order-book-snapshot)
 *     -> source (regex) -> keyBy(exchange_id, pair_id)
 *     -> LevelDiffFunction (diff against the last book we emitted)
 *     -> Kafka output  ex{id}-p{id}-{side}  (subject price-level-event)
 *
 * The output topics and subject are the EXISTING ones the consolidator already consumes — no
 * "-flink" suffix here, unlike every intermediate topic in this pipeline. That is deliberate: this
 * is the seam where the new pipeline replaces NiFi's normalized output.
 */
public class LevelEmitterJob {

    private static final Pattern INPUT_TOPIC_PATTERN =
            Pattern.compile("ex[0-9]+-p[0-9]+-orderbook-snapshot-flink");

    public static void main(String[] args) throws Exception {
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String groupId = getEnv("KAFKA_GROUP_ID", "normalizer-level-emitter");
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
                .keyBy(new ExchangePairKey())
                .process(new LevelDiffFunction())
                .name("emit-changed-levels")
                .sinkTo(KafkaSink.<PriceLevelEvent>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setRecordSerializer(KafkaRecordSerializationSchema.<PriceLevelEvent>builder()
                                .setTopicSelector((TopicSelector<PriceLevelEvent>) event ->
                                        "ex" + event.getExchangeId() + "-p" + event.getPairId()
                                                + "-" + event.getSide())
                                .setValueSerializationSchema(new PriceLevelEventSerializer(schemaRegistryUrl))
                                .build())
                        .build())
                .name("price-level-sink");

        env.execute("normalizer-level-emitter");
    }

    /** keyBy (exchange_id, pair_id) — named class, not a lambda (Flink key-type inference). */
    private static final class ExchangePairKey implements KeySelector<OrderBookSnapshot, String> {
        @Override
        public String getKey(OrderBookSnapshot book) {
            return book.getExchangeId() + "|" + book.getPairId();
        }
    }

    private static String getEnv(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
