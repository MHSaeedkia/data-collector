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
 * Merges the latest order book snapshot of every exchange for one pair+side into a single
 * price-sorted book. Quantities are NOT summed — each level keeps its own exchange, so
 * equal-price levels from different exchanges remain separate, adjacent entries.
 *
 * Sort: price ascending for asks, descending for bids; tie-break by larger quantity first.
 */
public class OrderBookMerger
        extends KeyedProcessFunction<String, OrderBookEvent, ConsolidatedOrderBook> {

    private final String side;
    private final boolean priceAscending; // asks ascending, bids descending

    // Latest snapshot per exchange (carries both levels and event_time).
    private transient MapState<String, OrderBookEvent> snapshotsByExchange;
    private transient Comparator<ConsolidatedLevel> comparator;

    public OrderBookMerger(String side) {
        this.side = side;
        this.priceAscending = "asks".equals(side);
    }

    @Override
    public void open(OpenContext openContext) {
        MapStateDescriptor<String, OrderBookEvent> descriptor = new MapStateDescriptor<>(
                "snapshotsByExchange",
                TypeInformation.of(String.class),
                TypeInformation.of(OrderBookEvent.class));
        snapshotsByExchange = getRuntimeContext().getMapState(descriptor);

        Comparator<ConsolidatedLevel> byPrice =
                Comparator.comparing(level -> new BigDecimal(level.getPrice()));
        if (!priceAscending) {
            byPrice = byPrice.reversed();
        }
        // Secondary: larger quantity first, regardless of side.
        Comparator<ConsolidatedLevel> byQuantityDesc =
                Comparator.<ConsolidatedLevel, BigDecimal>comparing(
                        level -> new BigDecimal(level.getQuantity())).reversed();
        comparator = byPrice.thenComparing(byQuantityDesc);
    }

    @Override
    public void processElement(
            OrderBookEvent event,
            Context ctx,
            Collector<ConsolidatedOrderBook> out) throws Exception {

        // Replace this exchange's latest snapshot.
        snapshotsByExchange.put(event.getExchange(), event);

        // Flatten every exchange's levels into one tagged list.
        List<ConsolidatedLevel> merged = new ArrayList<>();
        long maxEventTime = Long.MIN_VALUE;
        for (OrderBookEvent snapshot : snapshotsByExchange.values()) {
            maxEventTime = Math.max(maxEventTime, snapshot.getEventTime());
            if (snapshot.getLevels() == null) {
                continue;
            }
            for (PriceLevel level : snapshot.getLevels()) {
                merged.add(new ConsolidatedLevel(
                        snapshot.getExchange(), level.getPrice(), level.getQuantity()));
            }
        }

        merged.sort(comparator);
        out.collect(new ConsolidatedOrderBook(event.getPair(), side, merged, maxEventTime));
    }
}
