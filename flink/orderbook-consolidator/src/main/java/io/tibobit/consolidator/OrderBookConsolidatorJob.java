package io.tibobit.consolidator;

import io.tibobit.consolidator.model.ConsolidatedOrderBook;
import io.tibobit.consolidator.model.ExchangeBook;
import io.tibobit.consolidator.model.PriceLevelEvent;
import io.tibobit.consolidator.operator.CrossExchangeConsolidator;
import io.tibobit.consolidator.operator.PerExchangeBookBuilder;
import io.tibobit.consolidator.sink.ConsolidatedOrderBookSinkFactory;
import io.tibobit.consolidator.source.PriceLevelSourceFactory;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Job entry point for the order book consolidator.
 *
 * Pipeline (one unified topology — every pair/exchange/side flows through a single stream):
 *   Kafka input topics  {side}-p{pair_id}-ex{exchange_id}   (every exchange, every pair)
 *     -> source                             (one regex subscription — PriceLevelSourceFactory)
 *     -> keyBy(pair_id, exchange_id, side)  -> PerExchangeBookBuilder    (stage 1: R1/R2 per exchange)
 *     -> keyBy(pair_id, side)               -> CrossExchangeConsolidator (stage 2: R4 union / R5 sort)
 *     -> Kafka output topic  {side}-p{pair_id}   (R6 dynamic per-record routing)
 *        + print() to stdout for verification.
 *
 * Unlike orderbook-job there is no Postgres/PairsLoader: the source subscribes to every input
 * topic and the operators key off each event's own pair_id/exchange_id/side, so new
 * pairs/exchanges are picked up automatically.
 */
public class OrderBookConsolidatorJob {

    public static void main(String[] args) throws Exception {
        // Kafka config — defaults target the docker-compose network; override via env vars.
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String groupId = getEnv("KAFKA_GROUP_ID", "orderbook-consolidator-flink");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<PriceLevelEvent> source = PriceLevelSourceFactory.create(bootstrapServers, groupId);

        // No watermarks: latest-wins is driven by each event's own event_time compared in state,
        // not by event-time windows.
        DataStream<PriceLevelEvent> events =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "price-level-source");

        // Stage 1: key by (pair_id, exchange_id, side) so each keyed instance holds one exchange's
        // book. Anonymous KeySelector (not a lambda) so Flink can extract the String key type.
        DataStream<ExchangeBook> perExchange = events
                .keyBy(new KeySelector<PriceLevelEvent, String>() {
                    @Override
                    public String getKey(PriceLevelEvent e) {
                        return e.getPairId() + "|" + e.getExchangeId() + "|" + e.getSide();
                    }
                })
                .process(new PerExchangeBookBuilder())
                .name("per-exchange-book");

        // Stage 2: re-key by (pair_id, side) to union across exchanges.
        DataStream<ConsolidatedOrderBook> consolidated = perExchange
                .keyBy(new KeySelector<ExchangeBook, String>() {
                    @Override
                    public String getKey(ExchangeBook b) {
                        return b.getPairId() + "|" + b.getSide();
                    }
                })
                .process(new CrossExchangeConsolidator())
                .name("consolidated-book");

        // R6: single sink, output topic chosen per record ({side}-p{pair_id}).
        consolidated.sinkTo(ConsolidatedOrderBookSinkFactory.create(bootstrapServers)).name("consolidated-sink");

        // Also print to stdout for verification.
        consolidated.print("consolidated");

        env.execute("orderbook-consolidator");
    }

    private static String getEnv(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
