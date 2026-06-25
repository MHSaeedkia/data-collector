package io.tibobit.orderbook;

import io.tibobit.orderbook.aggregation.OrderBookMerger;
import io.tibobit.orderbook.model.ConsolidatedOrderBook;
import io.tibobit.orderbook.model.OrderBookEvent;
import io.tibobit.orderbook.sink.OrderBookSinkFactory;
import io.tibobit.orderbook.source.OrderBookSourceFactory;
import io.tibobit.orderbook.source.PairsLoader;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class OrderBookJob {

    public static void main(String[] args) throws Exception {
        // Kafka config — defaults target the docker-compose network; override via env
        // vars.
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String groupId = getEnv("KAFKA_GROUP_ID", "orderbook-flink");

        String jdbcUrl = getEnv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/markets");
        String dbUser = getEnv("POSTGRES_USER", "postgres");
        String dbPassword = getEnv("POSTGRES_PASSWORD", "postgres");

        List<String> pairs = new PairsLoader(jdbcUrl, dbUser, dbPassword).load();
        if (pairs.isEmpty()) {
            throw new IllegalStateException(
                    "No subscribed pairs found in postgres; nothing to consume.");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        for (String pair : pairs) {
            addStream(env, bootstrapServers, groupId, pair, "asks");
            addStream(env, bootstrapServers, groupId, pair, "bids");
        }

        env.execute("orderbook-job");
    }

    private static void addStream(
            StreamExecutionEnvironment env,
            String bootstrapServers,
            String groupId,
            String pair,
            String side) {

        KafkaSource<OrderBookEvent> source = OrderBookSourceFactory.create(bootstrapServers, groupId, pair, side);

        String name = pair + "-" + side;
        DataStream<OrderBookEvent> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), name + "-source");

        DataStream<ConsolidatedOrderBook> consolidated = stream
                .keyBy(OrderBookEvent::getPair)
                .process(new OrderBookMerger(side))
                .name(name + "-merger");

        // Publish the merged book to its own topic ({pair}-{side}, e.g. BTC-USDT-asks).
        consolidated.sinkTo(OrderBookSinkFactory.create(bootstrapServers, name)).name(name + "-sink");

        // Also print to stdout for verification.
        consolidated.print(name);
    }

    private static String getEnv(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
