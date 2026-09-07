package io.tibobit.normalizer.aggregate;

import io.tibobit.normalizer.checkpoint.CheckpointingConfigurer;
import io.tibobit.normalizer.kafka.PatternTopicSelector;
import io.tibobit.normalizer.model.OrderBookSnapshot;
import io.tibobit.normalizer.serde.OrderBookSnapshotDeserializer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.TransactionNamingStrategy;
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
    // Anchored, like every consumer of this job's output (merger, adjustment, web, e2e) — a loose
    // pattern here would make the dataset-identifier used by POOLING's LISTING abort strategy
    // match topics this sink does not itself write to.
    private static final Pattern OUTPUT_TOPIC_PATTERN = Pattern.compile("^p[0-9]+-(asks|bids)$");

    public static void main(String[] args) throws Exception {
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String groupId = getEnv("KAFKA_GROUP_ID", "normalizer-aggregator");
        String schemaRegistryUrl = getEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8082");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointingConfigurer.configure(env);

        KafkaSource<OrderBookSnapshot> source = KafkaSource.<OrderBookSnapshot>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(INPUT_TOPIC_PATTERN)
                // Direct-memory guard, added 2026-09-06 after orderbook-merger died of
                // `OutOfMemoryError: Direct buffer memory` (48 MB requested, 252 MB already
                // allocated, 287 MB limit). Nothing here is a leak — it is sizing. This source
                // subscribes by PATTERN across every `ex{id}-p{id}-orderbook-snapshot-flink`
                // topic — one per subscribed (exchange, pair), the widest fan-in on the
                // platform — and the consumer defaults are fetch.max.bytes 50 MB with
                // max.partition.fetch.bytes 1 MB, so the broker will fill a single response to
                // ~50 MB of DIRECT buffers.
                //
                // The budget it has to fit in is small and SHARED. Flink derives
                // MaxDirectMemorySize from the TaskManager sizing: at docker-compose.yml's
                // `process.size: 2g` that is framework.off-heap 128m + task.off-heap 0 + network
                // 158.7m = 286.7m — exactly the limit in the crash — and Flink's own network
                // memory has first claim on 158.7m of it. The dev stack runs ONE TaskManager
                // with 8 slots, so all 8 jobs share what is left. One 50 MB fetch does not fit.
                //
                // 8 MB per response, 512 KB per partition. Records here are order books of a few
                // tens of KB, so this costs round trips, not throughput, and a record larger than
                // the per-partition cap is still returned whole (KIP-74) rather than stalling the
                // consumer. Raise `taskmanager.memory.task.off-heap.size` instead of these only
                // if a measurement says the fetches are genuinely too small.
                .setProperty("fetch.max.bytes", "8388608")
                .setProperty("max.partition.fetch.bytes", "524288")
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                // job 5's sink is EXACTLY_ONCE/transactional; read_committed avoids seeing
                // records from a transaction that later aborts.
                .setProperty("isolation.level", "read_committed")
                .setValueOnlyDeserializer(new OrderBookSnapshotDeserializer(schemaRegistryUrl))
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "orderbook-snapshot-source")
                .uid("orderbook-snapshot-source")
                .flatMap(new SnapshotSplitter())
                .name("split-sides")
                .uid("split-sides")
                .keyBy(new PairSideKey())
                .process(new CrossExchangeAggregator())
                .name("aggregate")
                .uid("aggregate")
                .sinkTo(KafkaSink.<AggregatedOrderBook>builder()
                        .setBootstrapServers(bootstrapServers)
                        // EXACTLY_ONCE, POOLING and the pattern-aware topic selector: see
                        // PairExtractorJob's sink comment for why (same reasoning, every sink).
                        // This is the terminal, web-facing hop — merger, adjustment, web and e2e
                        // all read this topic family and all now need read_committed too (done in
                        // their own modules; see the checkpointing writeup).
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setTransactionalIdPrefix("job6-aggregated")
                        .setTransactionNamingStrategy(TransactionNamingStrategy.POOLING)
                        .setProperty("acks", "all")
                        .setProperty("enable.idempotence", "true")
                        .setProperty("retries", "2147483647")
                        .setProperty("delivery.timeout.ms", "120000")
                        .setProperty("transaction.timeout.ms", "600000")
                        .setRecordSerializer(KafkaRecordSerializationSchema.<AggregatedOrderBook>builder()
                                // Route each record to p{pair_id}-{side} (e.g. p1-asks).
                                .setTopicSelector(new PatternTopicSelector<AggregatedOrderBook>(OUTPUT_TOPIC_PATTERN) {
                                    @Override
                                    public String apply(AggregatedOrderBook book) {
                                        return "p" + book.getPairId() + "-" + book.getSide();
                                    }
                                })
                                .setValueSerializationSchema(new AggregatedOrderBookSerializer(schemaRegistryUrl))
                                .build())
                        .build())
                .name("aggregated-order-book-sink")
                .uid("aggregated-order-book-sink");

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
