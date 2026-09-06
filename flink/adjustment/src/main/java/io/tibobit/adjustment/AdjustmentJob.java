package io.tibobit.adjustment;

import java.util.regex.Pattern;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.TopicSelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Order book adjustment.
 *
 * Pipeline: Kafka input p{id}-{side} (AggregatedOrderBookEvent, subject
 * aggregated-order-book-event) -> source (regex) -> map AdjustedOrderBook::from
 * (same book, nothing adjusted yet) -> map BuySellCommissionFunction (+/-
 * buy_sell_commission_percent of that original, PER LEVEL) -> map
 * OurProfitFunction (+/- our_profit_percent of that original, PER LEVEL) -> map
 * SlippageFunction (+/- slippage_percent of that original, PER LEVEL) -> Kafka
 * output p{id}-{side}-adjusted (subject adjusted-order-book-event)
 *
 * <p>
 * All three rates come from {@code exchange_markets} (2026-08-25) via a
 * {@link RefreshingLookup} keyed {@code (exchange_id, pair_id)} — PER LEVEL,
 * not per record, because a book unions levels from multiple exchanges and the
 * user confirmed all three genuinely vary by exchange for the same market.
 *
 * This is NOT a stage of the raw-normalization pipeline and does not live in
 * flink/normalizer/. It reads that pipeline's finished output and publishes a
 * parallel view of it, exactly as flink/merger does — job 6's output is
 * untouched and every view is a separate topic consumers choose between.
 *
 * <p>
 * <b>Every stage sizes its amount off the price the level ARRIVED with, not off
 * the running price</b> (user, 2026-08-24, correcting the first
 * implementation). So the three rates ADD: an ask ends at base x <b>1.0145</b>,
 * not the 1.0035 x 1.001 x 1.01 = 1.014548535 that compounding would give. Each
 * level keeps its untouched arrival price in {@code AdjustedLevel.basePrice}
 * for exactly this reason.
 *
 * <p>
 * A consequence worth knowing: <b>the chain order no longer affects the
 * result</b>, because addition commutes. The stages still run commission →
 * profit → slippage and that is what the Flink UI shows, but reordering them
 * would produce identical prices — which was NOT true of the compounding
 * version this replaced.
 *
 * <p>
 * Each stage also writes the rate it applied onto the record, so the published
 * event says what was charged and not merely what the answer was. That is why
 * the output has a schema of its own ({@code adjusted-order-book-event}) rather
 * than reusing job 6's.
 *
 * <p>
 * Every stage is {@code .name()}d, which is what makes the chain readable in
 * the Flink web UI — the cheapest way to confirm a deployed job is wired the
 * way this file says.
 *
 * <p>
 * The round trip is therefore the only thing that can go wrong here, and it is
 * not free: the record is decoded into {@link AggregatedOrderBook} and
 * re-encoded, so a field this job's model omits would be dropped and silently
 * replaced by its schema default. {@code losslessRoundTrip} in the test suite
 * is what holds that shut.
 */
public class AdjustmentJob {

    /**
     * Anchored, and it matters more here than anywhere: unanchored, this would
     * match this job's own {@code p1-asks-adjusted} output AND the merger's
     * {@code p1-asks-merged}, feeding the job its own records forever and
     * mixing in a different record type that would fail to decode. (Kafka's
     * pattern subscription uses full-match semantics, so the anchors are
     * belt-and-braces — but the next person to widen this regex should have to
     * notice.)
     */
    private static final Pattern INPUT_TOPIC_PATTERN = Pattern.compile("^p[0-9]+-(asks|bids)$");

    private static final String OUTPUT_TOPIC_SUFFIX = "-adjusted";

    public static void main(String[] args) throws Exception {
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String groupId = getEnv("KAFKA_GROUP_ID", "orderbook-adjustment");
        String schemaRegistryUrl = getEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8082");
        String postgresUrl = getEnv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/markets");
        String postgresUser = getEnv("POSTGRES_USER", "postgres");
        String postgresPassword = getEnv("POSTGRES_PASSWORD", "postgres");
        long refreshIntervalMs = Long.parseLong(getEnv("REFRESH_INTERVAL_MS", "60000"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // One lookup per stage, same as job 3/4's per-operator ownership of a RefreshingLookup —
        // each stage polls exchange_markets independently rather than sharing one instance across
        // operators, which is what job 3 does too.
        RefreshingLookup<String, AdjustmentFactors> commissionFactors = new RefreshingLookup<>(
                new AdjustmentFactorsLoader(postgresUrl, postgresUser, postgresPassword), refreshIntervalMs);
        RefreshingLookup<String, AdjustmentFactors> profitFactors = new RefreshingLookup<>(
                new AdjustmentFactorsLoader(postgresUrl, postgresUser, postgresPassword), refreshIntervalMs);
        RefreshingLookup<String, AdjustmentFactors> slippageFactors = new RefreshingLookup<>(
                new AdjustmentFactorsLoader(postgresUrl, postgresUser, postgresPassword), refreshIntervalMs);

        KafkaSource<AggregatedOrderBook> source = KafkaSource.<AggregatedOrderBook>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(INPUT_TOPIC_PATTERN)
                // Direct-memory guard, added 2026-09-06 after orderbook-merger died of
                // `OutOfMemoryError: Direct buffer memory` (48 MB requested, 252 MB already
                // allocated, 287 MB limit). Nothing here is a leak — it is sizing. This source
                // subscribes by PATTERN across every `p{id}-{side}` topic — two per subscribed
                // pair — and the consumer defaults are fetch.max.bytes 50 MB with
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
                // Live feed, no replay — the same choice every job on this platform makes. A topic
                // created after the job starts is discovered late and whatever was produced in the
                // gap is lost, which is why warmup.sh pre-creates the adjusted family.
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new AggregatedOrderBookDeserializer(schemaRegistryUrl))
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "aggregated-order-book-source")
                .map(AdjustedOrderBook::from)
                .name("to-adjusted")
                .map(new BuySellCommissionFunction(commissionFactors))
                .name("buy-sell-commission")
                .map(new OurProfitFunction(profitFactors))
                .name("our-profit")
                .map(new SlippageFunction(slippageFactors))
                .name("slippage")
                .sinkTo(KafkaSink.<AdjustedOrderBook>builder()
                        .setBootstrapServers(bootstrapServers)
                        // Without checkpointing, DeliveryGuarantee is NONE and a broker-side
                        // failure drops records silently. Idempotence is the load-bearing one:
                        // plain retries can reorder writes, which corrupts the book downstream.
                        .setProperty("acks", "all")
                        .setProperty("enable.idempotence", "true")
                        .setProperty("retries", "2147483647")
                        .setProperty("delivery.timeout.ms", "120000")
                        .setRecordSerializer(KafkaRecordSerializationSchema.<AdjustedOrderBook>builder()
                                // Route each record to p{pair_id}-{side}-adjusted (e.g. p1-asks-adjusted).
                                .setTopicSelector((TopicSelector<AdjustedOrderBook>) book
                                        -> "p" + book.getPairId() + "-" + book.getSide() + OUTPUT_TOPIC_SUFFIX)
                                .setValueSerializationSchema(new AdjustedOrderBookSerializer(schemaRegistryUrl))
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
