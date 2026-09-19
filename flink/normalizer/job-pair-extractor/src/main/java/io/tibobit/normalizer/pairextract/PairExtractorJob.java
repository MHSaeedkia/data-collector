package io.tibobit.normalizer.pairextract;

import io.tibobit.normalizer.lookup.RefreshingLookup;
import io.tibobit.normalizer.model.ParsedBookEvent;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import io.tibobit.normalizer.serde.ParsedBookEventDeserializer;
import io.tibobit.normalizer.serde.RawOrderBookEventSerializer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.TopicSelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.regex.Pattern;

/**
 * Job 2 entry point: pair extraction.
 *
 * Pipeline (stateless — no keying needed):
 *   Kafka input topics  ex{id}-parsed-flink  (ParsedBookEvent, subject parsed-book-event,
 *        one topic per exchange)
 *     -> source (regex)
 *     -> PairExtractFunction (market → pair_id via exchange_markets)
 *     -> Kafka output topic  ex{exchange_id}-p{pair_id}-raw-flink  (per-record routing,
 *        subject raw-order-book-event)
 *
 * <p>Reads a value-only deserializer, unlike the job-parser source it used to be fused with:
 * exchange_id is ON the record here, so there is no need to parse it out of the topic name.
 */
public class PairExtractorJob {

    private static final Pattern INPUT_TOPIC_PATTERN = Pattern.compile("ex[0-9]+-parsed-flink");

    public static void main(String[] args) throws Exception {
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String groupId = getEnv("KAFKA_GROUP_ID", "normalizer-pair-extractor");
        String schemaRegistryUrl = getEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8082");
        String postgresUrl = getEnv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/markets");
        String postgresUser = getEnv("POSTGRES_USER", "postgres");
        String postgresPassword = getEnv("POSTGRES_PASSWORD", "postgres");
        long refreshIntervalMs = Long.parseLong(getEnv("REFRESH_INTERVAL_MS", "60000"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<ParsedBookEvent> source = KafkaSource.<ParsedBookEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(INPUT_TOPIC_PATTERN)
                .setGroupId(groupId)
                // Start at the tip: we want the live feed, not historical replay.
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new ParsedBookEventDeserializer(schemaRegistryUrl))
                .build();

        RefreshingLookup<String, Integer> markets = new RefreshingLookup<>(
                new ExchangeMarketsLoader(postgresUrl, postgresUser, postgresPassword),
                refreshIntervalMs);

        DataStream<RawOrderBookEvent> events = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "parsed-flink-source")
                .flatMap(new PairExtractFunction(markets))
                .name("pair-extract");

        events.sinkTo(KafkaSink.<RawOrderBookEvent>builder()
                        .setBootstrapServers(bootstrapServers)
                        // Without checkpointing, DeliveryGuarantee is NONE and a broker-side
                        // failure drops records silently. Idempotence is the load-bearing one:
                        // plain retries can reorder writes, which corrupts the book downstream.
                        .setProperty("acks", "all")
                        .setProperty("enable.idempotence", "true")
                        .setProperty("retries", "2147483647")
                        .setProperty("delivery.timeout.ms", "120000")
                        .setRecordSerializer(KafkaRecordSerializationSchema.<RawOrderBookEvent>builder()
                                .setTopicSelector((TopicSelector<RawOrderBookEvent>) event ->
                                        "ex" + event.getExchangeId() + "-p" + event.getPairId() + "-raw-flink")
                                .setValueSerializationSchema(new RawOrderBookEventSerializer(schemaRegistryUrl))
                                .build())
                        .build())
                .name("raw-flink-sink");

        env.execute("normalizer-pair-extractor");
    }

    private static String getEnv(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
