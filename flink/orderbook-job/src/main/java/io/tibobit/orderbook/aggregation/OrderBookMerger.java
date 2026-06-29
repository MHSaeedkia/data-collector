package io.tibobit.orderbook.aggregation;

import io.tibobit.orderbook.model.ConsolidatedLevel;
import io.tibobit.orderbook.model.ConsolidatedOrderBook;
import io.tibobit.orderbook.model.OrderBookEvent;
import io.tibobit.orderbook.model.PriceLevel;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Merges the latest order book snapshot of every exchange for one
 * pair+side
 * into a single
 * price-sorted book. Quantities are NOT summed — each level keeps its own
 * exchange, so
 * equal-price levels from different exchanges remain separate, adjacent
 * entries.
 *
 * Sort: price ascending for asks, descending for bids; tie-break by larger
 * quantity first.
 */
public class OrderBookMerger
        extends KeyedProcessFunction<Integer, OrderBookEvent, ConsolidatedOrderBook> {

    private final String side;
    private final boolean priceAscending; // asks ascending, bids descending

    // Latest snapshot per exchange (carries both levels and event_time), keyed by exchange_id.
    private transient MapState<Integer, OrderBookEvent> snapshotsByExchange;
    private transient Comparator<ConsolidatedLevel> comparator;

    public OrderBookMerger(String side) {
        this.side = side;
        this.priceAscending = "asks".equals(side);
    }

    // State and comparator are built in open() (not the constructor) because they belong to
    // the runtime: MapState is provided per keyed instance, and Comparator isn't Serializable
    // so it can't be a field shipped with the operator. open(OpenContext) is the Flink 2.x
    // signature; the 1.x open(Configuration) was removed in 2.0.
    @Override
    public void open(OpenContext openContext) {
        MapStateDescriptor<Integer, OrderBookEvent> descriptor = new MapStateDescriptor<>(
                "snapshotsByExchange",
                TypeInformation.of(Integer.class),
                TypeInformation.of(OrderBookEvent.class));
        snapshotsByExchange = getRuntimeContext().getMapState(descriptor);

        Comparator<ConsolidatedLevel> byPrice = Comparator.comparing(level -> new BigDecimal(level.getPrice()));
        if (!priceAscending) {
            byPrice = byPrice.reversed();
        }
        // Secondary: larger quantity first, regardless of side.
        Comparator<ConsolidatedLevel> byQuantityDesc = Comparator.<ConsolidatedLevel, BigDecimal>comparing(
                level -> new BigDecimal(level.getQuantity())).reversed();
        comparator = byPrice.thenComparing(byQuantityDesc);
    }

    @Override
    public void processElement(
            OrderBookEvent event,
            Context ctx,
            Collector<ConsolidatedOrderBook> out) throws Exception {

        // Replace this exchange's latest snapshot.
        snapshotsByExchange.put(event.getExchangeId(), event);

        // Rebuild the whole consolidated book from scratch on every event: flatten every
        // exchange's stored snapshot into one list, tagging each level with its exchange_id.
        // (Cheap enough — book depth is small — and avoids incremental-merge state bugs.)
        List<ConsolidatedLevel> merged = new ArrayList<>();
        long maxEventTime = Long.MIN_VALUE;
        for (OrderBookEvent snapshot : snapshotsByExchange.values()) {
            // Output event_time = newest contributing snapshot, so freshness reflects the
            // most recent exchange update, not whichever one happened to trigger this rebuild.
            maxEventTime = Math.max(maxEventTime, snapshot.getEventTime());
            if (snapshot.getLevels() == null) {
                continue;
            }
            for (PriceLevel level : snapshot.getLevels()) {
                merged.add(new ConsolidatedLevel(
                        snapshot.getExchangeId(), level.getPrice(), level.getQuantity()));
            }
        }

        // Sort the union (price asc/desc by side, then quantity desc); levels are NOT summed,
        // so equal-price levels from different exchanges stay as separate adjacent entries.
        merged.sort(comparator);
        out.collect(new ConsolidatedOrderBook(event.getPairId(), side, merged, maxEventTime));
    }
}
