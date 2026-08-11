package io.tibobit.merger;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.TopicSelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.regex.Pattern;

/**
 * Cross-exchange price-merging order book.
 *
 * Pipeline:
 *   Kafka input  p{id}-{side}         (AggregatedOrderBookEvent, subject aggregated-order-book-event)
 *     -> source (regex)
 *     -> map PriceMergeFunction       (sum quantities at equal price, collect the contributing ids)
 *     -> Kafka output  p{id}-{side}-merged  (subject merged-order-book-event)
 *
 * This is NOT a stage of the raw-normalization pipeline and does not live in flink/normalizer/. It
 * reads that pipeline's finished output and publishes a second, parallel view of it. Job 6's
 * union-never-sum rule is a deliberate business decision and is untouched; both topics are live and
 * consumers choose.
 *
 * Reading job 6's output rather than job 5's per-exchange books costs one Kafka hop of latency and
 * buys a stateless job: job 6 has already unioned every exchange, so no MapState, no splitter and
 * no gap/reset handling are needed here. An exchange that job 6 dropped is already absent.
 */
public class MergerJob {

    /**
     * Anchored, and it matters: unanchored, this would also match this job's own
     * {@code p1-asks-merged} output and feed the job its own records forever. (Kafka's pattern
     * subscription uses full-match semantics, so the anchors are belt-and-braces — but the next
     * person to widen this regex should have to notice.)
     */
    private static final Pattern INPUT_TOPIC_PATTERN = Pattern.compile("^p[0-9]+-(asks|bids)$");

    private static final String OUTPUT_TOPIC_SUFFIX = "-merged";

    public static void main(String[] args) throws Exception {
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String groupId = getEnv("KAFKA_GROUP_ID", "orderbook-merger");
        String schemaRegistryUrl = getEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8082");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<AggregatedOrderBook> source = KafkaSource.<AggregatedOrderBook>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(INPUT_TOPIC_PATTERN)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new AggregatedOrderBookDeserializer(schemaRegistryUrl))
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "aggregated-order-book-source")
                .map(new PriceMergeFunction())
                .name("merge-prices")
                .sinkTo(KafkaSink.<MergedOrderBook>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setRecordSerializer(KafkaRecordSerializationSchema.<MergedOrderBook>builder()
                                // Route each record to p{pair_id}-{side}-merged (e.g. p1-asks-merged).
                                .setTopicSelector((TopicSelector<MergedOrderBook>) book ->
                                        "p" + book.getPairId() + "-" + book.getSide() + OUTPUT_TOPIC_SUFFIX)
                                .setValueSerializationSchema(new MergedOrderBookSerializer(schemaRegistryUrl))
                                .build())
                        .build())
                .name("merged-order-book-sink");

        env.execute("orderbook-merger");
    }

    private static String getEnv(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
