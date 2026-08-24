package io.tibobit.adjustment;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.TopicSelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.regex.Pattern;

/**
 * Order book adjustment.
 *
 * Pipeline:
 *   Kafka input  p{id}-{side}            (AggregatedOrderBookEvent, subject aggregated-order-book-event)
 *     -> source (regex)
 *     -> map BuySellCommissionFunction   (no-op for now)
 *     -> map OurProfitFunction           (no-op for now)
 *     -> map SlippageFunction            (no-op for now)
 *     -> Kafka output  p{id}-{side}-adjusted   (same subject)
 *
 * This is NOT a stage of the raw-normalization pipeline and does not live in flink/normalizer/. It
 * reads that pipeline's finished output and publishes a parallel view of it, exactly as
 * flink/merger does — job 6's output is untouched and every view is a separate topic consumers
 * choose between.
 *
 * <p><b>The three adjustment stages exist but do nothing yet.</b> They are chained in the order the
 * user specified — commission, then profit, then slippage — and each currently returns its input
 * unchanged, so the record still reaches the output topic verbatim. The order is part of the
 * contract, not an arrangement of convenience: the stages compose, so each one sees prices the
 * ones above it have already moved.
 *
 * <p>Every stage is {@code .name()}d, which is what makes the chain readable in the Flink web UI —
 * the cheapest way to confirm a deployed job is wired the way this file says.
 *
 * <p>The round trip is therefore the only thing that can go wrong here, and it is not free: the
 * record is decoded into {@link AggregatedOrderBook} and re-encoded, so a field this job's model
 * omits would be dropped and silently replaced by its schema default. {@code losslessRoundTrip}
 * in the test suite is what holds that shut.
 */
public class AdjustmentJob {

    /**
     * Anchored, and it matters more here than anywhere: unanchored, this would match this job's own
     * {@code p1-asks-adjusted} output AND the merger's {@code p1-asks-merged}, feeding the job its
     * own records forever and mixing in a different record type that would fail to decode. (Kafka's
     * pattern subscription uses full-match semantics, so the anchors are belt-and-braces — but the
     * next person to widen this regex should have to notice.)
     */
    private static final Pattern INPUT_TOPIC_PATTERN = Pattern.compile("^p[0-9]+-(asks|bids)$");

    private static final String OUTPUT_TOPIC_SUFFIX = "-adjusted";

    public static void main(String[] args) throws Exception {
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String groupId = getEnv("KAFKA_GROUP_ID", "orderbook-adjustment");
        String schemaRegistryUrl = getEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8082");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<AggregatedOrderBook> source = KafkaSource.<AggregatedOrderBook>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(INPUT_TOPIC_PATTERN)
                .setGroupId(groupId)
                // Live feed, no replay — the same choice every job on this platform makes. A topic
                // created after the job starts is discovered late and whatever was produced in the
                // gap is lost, which is why warmup.sh pre-creates the adjusted family.
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new AggregatedOrderBookDeserializer(schemaRegistryUrl))
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "aggregated-order-book-source")
                .map(new BuySellCommissionFunction())
                .name("buy-sell-commission")
                .map(new OurProfitFunction())
                .name("our-profit")
                .map(new SlippageFunction())
                .name("slippage")
                .sinkTo(KafkaSink.<AggregatedOrderBook>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setRecordSerializer(KafkaRecordSerializationSchema.<AggregatedOrderBook>builder()
                                // Route each record to p{pair_id}-{side}-adjusted (e.g. p1-asks-adjusted).
                                .setTopicSelector((TopicSelector<AggregatedOrderBook>) book ->
                                        "p" + book.getPairId() + "-" + book.getSide() + OUTPUT_TOPIC_SUFFIX)
                                .setValueSerializationSchema(new AggregatedOrderBookSerializer(schemaRegistryUrl))
                                .build())
                        .build())
                .name("adjusted-order-book-sink");

        env.execute("orderbook-adjustment");
    }

    private static String getEnv(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
