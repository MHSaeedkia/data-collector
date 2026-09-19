package io.tibobit.normalizer.parse;

import io.tibobit.normalizer.model.ParsedBookEvent;
import io.tibobit.normalizer.parse.parser.Parsers;
import io.tibobit.normalizer.serde.ParsedBookEventSerializer;

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
 * Job 1 entry point: parsing.
 *
 * Pipeline (stateless — no keying needed):
 *   Kafka input topics  ex{id}-raw        (verbatim exchange payloads, one topic per exchange)
 *     -> source (regex, topic name captured for exchange_id — RawTopicDeserializer)
 *     -> ParseFunction (per-exchange parse; market string left unresolved)
 *     -> Kafka output topic  ex{exchange_id}-parsed-flink  (per-record routing,
 *        subject parsed-book-event)
 *
 * <p>Split out of the pair-extractor on 2026-09-19. The output topic is per EXCHANGE, not per
 * (exchange, pair), because pair_id is exactly the thing this job does not know — and it keeps the
 * ordering guarantee unchanged, since one partition per exchange is already what ex{id}-raw gives.
 * The output name deliberately does not match RAW_TOPIC_PATTERN, so this job cannot consume its
 * own output.
 */
public class ParserJob {

    // Deliberately matches every ex{n}-raw topic, including exchanges with no parser yet:
    // scope lives in Parsers.byExchangeId(), and ParseFunction drops unparsered exchanges
    // with a counter. Landing ex7 (2026-08-24) and ex9 (2026-08-25) both needed no change here,
    // and with ex9 the map now covers every seeded exchange.
    private static final Pattern RAW_TOPIC_PATTERN = Pattern.compile("ex[0-9]+-raw");

    public static void main(String[] args) throws Exception {
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String groupId = getEnv("KAFKA_GROUP_ID", "normalizer-parser");
        String schemaRegistryUrl = getEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8082");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<RawExchangeMessage> source = KafkaSource.<RawExchangeMessage>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(RAW_TOPIC_PATTERN)
                .setGroupId(groupId)
                // Start at the tip: we want the live feed, not historical replay.
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new RawTopicDeserializer())
                .build();

        DataStream<ParsedBookEvent> events = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "raw-source")
                .flatMap(new ParseFunction(Parsers.byExchangeId()))
                .name("parse");

        events.sinkTo(KafkaSink.<ParsedBookEvent>builder()
                        .setBootstrapServers(bootstrapServers)
                        // Without checkpointing, DeliveryGuarantee is NONE and a broker-side
                        // failure drops records silently. Idempotence is the load-bearing one:
                        // plain retries can reorder writes, which corrupts the book downstream.
                        .setProperty("acks", "all")
                        .setProperty("enable.idempotence", "true")
                        .setProperty("retries", "2147483647")
                        .setProperty("delivery.timeout.ms", "120000")
                        .setRecordSerializer(KafkaRecordSerializationSchema.<ParsedBookEvent>builder()
                                .setTopicSelector((TopicSelector<ParsedBookEvent>) parsed ->
                                        "ex" + parsed.getEvent().getExchangeId() + "-parsed-flink")
                                .setValueSerializationSchema(new ParsedBookEventSerializer(schemaRegistryUrl))
                                .build())
                        .build())
                .name("parsed-flink-sink");

        env.execute("normalizer-parser");
    }

    private static String getEnv(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
