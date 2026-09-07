package io.tibobit.normalizer.pairextract;

import io.tibobit.normalizer.checkpoint.CheckpointingConfigurer;
import io.tibobit.normalizer.kafka.PatternTopicSelector;
import io.tibobit.normalizer.lookup.RefreshingLookup;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import io.tibobit.normalizer.pairextract.parser.Parsers;
import io.tibobit.normalizer.serde.RawOrderBookEventSerializer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.TransactionNamingStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.regex.Pattern;

/**
 * Job 1 entry point: pair extraction.
 *
 * Pipeline (stateless — no keying needed):
 *   Kafka input topics  ex{id}-raw        (verbatim exchange payloads, one topic per exchange)
 *     -> source (regex, topic name captured for exchange_id — RawTopicDeserializer)
 *     -> PairExtractFunction (per-exchange parse + market → pair_id via exchange_markets)
 *     -> Kafka output topic  ex{exchange_id}-p{pair_id}-raw-flink  (per-record routing,
 *        subject raw-order-book-event)
 */
public class PairExtractorJob {

    // Deliberately matches every ex{n}-raw topic, including exchanges with no parser yet:
    // scope lives in Parsers.byExchangeId(), and PairExtractFunction drops unparsered exchanges
    // with a counter. Landing ex7 (2026-08-24) and ex9 (2026-08-25) both needed no change here,
    // and with ex9 the map now covers every seeded exchange.
    private static final Pattern RAW_TOPIC_PATTERN = Pattern.compile("ex[0-9]+-raw");
    private static final Pattern OUTPUT_TOPIC_PATTERN = Pattern.compile("ex[0-9]+-p[0-9]+-raw-flink");

    public static void main(String[] args) throws Exception {
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String groupId = getEnv("KAFKA_GROUP_ID", "normalizer-pair-extractor");
        String schemaRegistryUrl = getEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8082");
        String postgresUrl = getEnv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/markets");
        String postgresUser = getEnv("POSTGRES_USER", "postgres");
        String postgresPassword = getEnv("POSTGRES_PASSWORD", "postgres");
        long refreshIntervalMs = Long.parseLong(getEnv("REFRESH_INTERVAL_MS", "60000"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointingConfigurer.configure(env);

        // Reads NiFi's raw topic directly — never written transactionally, so no
        // isolation.level override needed here (contrast every job downstream of job 6... i.e.
        // every OTHER job in this pipeline, which reads another job's now-transactional output).
        KafkaSource<RawExchangeMessage> source = KafkaSource.<RawExchangeMessage>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(RAW_TOPIC_PATTERN)
                .setGroupId(groupId)
                // Start at the tip: we want the live feed, not historical replay.
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new RawTopicDeserializer())
                .build();

        RefreshingLookup<String, Integer> markets = new RefreshingLookup<>(
                new ExchangeMarketsLoader(postgresUrl, postgresUser, postgresPassword),
                refreshIntervalMs);

        DataStream<RawOrderBookEvent> events = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "raw-source")
                .uid("raw-source")
                .flatMap(new PairExtractFunction(Parsers.byExchangeId(), markets))
                .name("pair-extract")
                .uid("pair-extract");

        events.sinkTo(KafkaSink.<RawOrderBookEvent>builder()
                        .setBootstrapServers(bootstrapServers)
                        // EXACTLY_ONCE: a record is only visible downstream once this job's
                        // checkpoint commits the Kafka transaction it was written in — see
                        // CheckpointingConfigurer for the latency this costs across the chain.
                        // POOLING (not the default INCREMENTING) reuses a small fixed pool of
                        // transactional ids instead of minting one per checkpoint; INCREMENTING is
                        // what flooded the broker's __transaction_state topic and OOM'd it in the
                        // round this replaces. acks/idempotence/retries are otherwise implied by a
                        // transactional producer but kept explicit.
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setTransactionalIdPrefix("job1-raw-flink")
                        .setTransactionNamingStrategy(TransactionNamingStrategy.POOLING)
                        .setProperty("acks", "all")
                        .setProperty("enable.idempotence", "true")
                        .setProperty("retries", "2147483647")
                        .setProperty("delivery.timeout.ms", "120000")
                        // Under the broker's transaction.max.timeout.ms ceiling (15 min default).
                        .setProperty("transaction.timeout.ms", "600000")
                        .setRecordSerializer(KafkaRecordSerializationSchema.<RawOrderBookEvent>builder()
                                // A plain lambda here cannot implement KafkaDatasetIdentifierProvider,
                                // which POOLING's LISTING abort strategy requires to enumerate this
                                // sink's target topics — see PatternTopicSelector.
                                .setTopicSelector(new PatternTopicSelector<RawOrderBookEvent>(OUTPUT_TOPIC_PATTERN) {
                                    @Override
                                    public String apply(RawOrderBookEvent event) {
                                        return "ex" + event.getExchangeId() + "-p" + event.getPairId() + "-raw-flink";
                                    }
                                })
                                .setValueSerializationSchema(new RawOrderBookEventSerializer(schemaRegistryUrl))
                                .build())
                        .build())
                .name("raw-flink-sink")
                .uid("raw-flink-sink");

        env.execute("normalizer-pair-extractor");
    }

    private static String getEnv(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
