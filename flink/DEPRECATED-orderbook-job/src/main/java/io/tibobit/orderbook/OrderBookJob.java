package io.tibobit.orderbook;

import io.tibobit.orderbook.aggregation.OrderBookMerger;
import io.tibobit.orderbook.model.ConsolidatedOrderBook;
import io.tibobit.orderbook.model.OrderBookEvent;
import io.tibobit.orderbook.sink.OrderBookSinkFactory;
import io.tibobit.orderbook.source.OrderBookSourceFactory;
import io.tibobit.orderbook.source.PairsLoader;
import io.tibobit.orderbook.source.PairsLoader.Pair;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * Job entry point. Builds the streaming topology and submits it to the cluster.
 *
 * Pipeline, per subscribed pair and per side (asks/bids):
 *   Kafka input topics  {side}-p{pair_id}-ex{exchange_id}   (one per exchange)
 *     -> source         (regex-subscribes to all exchanges for that pair+side)
 *     -> keyBy(pairId)  -> OrderBookMerger (latest snapshot per exchange, merged + sorted)
 *     -> Kafka output topic  {side}-p{pair_id}   (consolidated book, exchanges merged)
 *        + print() to stdout for verification.
 *
 * The set of pairs is read once from postgres at startup (see {@link PairsLoader});
 * each pair produces two independent streams (asks and bids).
 */
public class OrderBookJob {

    public static void main(String[] args) throws Exception {
        // Kafka config — defaults target the docker-compose network; override via env
        // vars.
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String groupId = getEnv("KAFKA_GROUP_ID", "orderbook-flink");

        String jdbcUrl = getEnv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/markets");
        String dbUser = getEnv("POSTGRES_USER", "postgres");
        String dbPassword = getEnv("POSTGRES_PASSWORD", "postgres");

        // Pairs are resolved once at submit time; adding/removing a subscription
        // requires restarting the job (the topology is fixed after env.execute).
        List<Pair> pairs = new PairsLoader(jdbcUrl, dbUser, dbPassword).load();
        if (pairs.isEmpty()) {
            throw new IllegalStateException(
                    "No subscribed pairs found in postgres; nothing to consume.");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // One independent source->merge->sink stream per pair and per side.
        for (Pair pair : pairs) {
            addStream(env, bootstrapServers, groupId, pair, "asks");
            addStream(env, bootstrapServers, groupId, pair, "bids");
        }

        env.execute("orderbook-job");
    }

    private static void addStream(
            StreamExecutionEnvironment env,
            String bootstrapServers,
            String groupId,
            Pair pair,
            String side) {

        KafkaSource<OrderBookEvent> source = OrderBookSourceFactory.create(bootstrapServers, groupId, pair.id(), side);

        // Operator and topic names both use IDs ({side}-p{pair_id}).
        String name = side + "-p" + pair.id();
        // No watermarks: the merge is event-driven (latest-wins per exchange), not windowed,
        // so it has no use for event-time progress.
        DataStream<OrderBookEvent> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), name + "-source");

        // keyBy(pairId) is what makes the merger's MapState per-pair. This stream already
        // carries a single pairId (one source per pair+side), so every record lands on the
        // same key — the keyBy is what gives the operator keyed state, not a fan-out.
        DataStream<ConsolidatedOrderBook> consolidated = stream
                .keyBy(OrderBookEvent::getPairId)
                .process(new OrderBookMerger(side))
                .name(name + "-merger");

        // Publish the merged book to its own topic ({side}-p{pair_id}, e.g.
        // asks-p2).
        consolidated.sinkTo(OrderBookSinkFactory.create(bootstrapServers, name)).name(name + "-sink");

        // Also print to stdout for verification.
        consolidated.print(name);
    }

    private static String getEnv(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
