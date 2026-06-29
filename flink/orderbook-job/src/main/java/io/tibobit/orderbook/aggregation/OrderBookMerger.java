package io.tibobit.orderbook.aggregation;

import io.tibobit.orderbook.model.ConsolidatedLevel;
import io.tibobit.orderbook.model.ConsolidatedOrderBook;
import io.tibobit.orderbook.model.ExchangeBook;
import io.tibobit.orderbook.model.OrderBookEvent;
import io.tibobit.orderbook.model.OrderBookEventType;
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
import java.util.Map;

/**
 * Maintains a running order book per exchange for one pair+side and merges them into a
 * single price-sorted book. A {@code snapshot} event replaces that exchange's book; an
 * {@code update} event mutates it level-by-level (see {@link ExchangeBook}). Stale or
 * duplicate events are dropped via {@code sequence_id}. Quantities are NOT summed — each
 * level keeps its own exchange, so equal-price levels from different exchanges remain
 * separate, adjacent entries.
 *
 * Sort: price ascending for asks, descending for bids; tie-break by larger
 * quantity first.
 */
public class OrderBookMerger
        extends KeyedProcessFunction<Integer, OrderBookEvent, ConsolidatedOrderBook> {

    private final String side;
    private final boolean priceAscending; // asks ascending, bids descending

    // Maintained running book per exchange (levels + event_time + last sequence_id),
    // keyed by exchange_id.
    private transient MapState<Integer, ExchangeBook> booksByExchange;
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
        MapStateDescriptor<Integer, ExchangeBook> descriptor = new MapStateDescriptor<>(
                "booksByExchange",
                TypeInformation.of(Integer.class),
                TypeInformation.of(ExchangeBook.class));
        booksByExchange = getRuntimeContext().getMapState(descriptor);

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

        int exchangeId = event.getExchangeId();
        ExchangeBook book = booksByExchange.get(exchangeId);

        // Drop stale/duplicate events. Only meaningful once a book already exists for this
        // exchange; the first event for an exchange is always accepted. (Gap handling —
        // seq > lastSeq + 1 — is deferred; see todo.md.)
        if (book != null && event.getSequenceId() <= book.getLastSeq()) {
            return;
        }

        if (event.getType() == OrderBookEventType.SNAPSHOT) {
            // Snapshot: replace this exchange's book wholesale.
            book = new ExchangeBook();
            if (event.getLevels() != null) {
                for (PriceLevel level : event.getLevels()) {
                    book.getLevels().put(new BigDecimal(level.getPrice()), level.getQuantity());
                }
            }
        } else {
            // Update: apply deltas on top of the existing book (empty if none yet — the
            // cold-start assumption is that the first event per exchange is a snapshot).
            if (book == null) {
                book = new ExchangeBook();
            }
            if (event.getLevels() != null) {
                for (PriceLevel level : event.getLevels()) {
                    BigDecimal price = new BigDecimal(level.getPrice());
                    if (BigDecimal.ZERO.compareTo(new BigDecimal(level.getQuantity())) == 0) {
                        book.getLevels().remove(price); // quantity 0 → delete level
                    } else {
                        book.getLevels().put(price, level.getQuantity()); // upsert
                    }
                }
            }
        }
        book.setEventTime(event.getEventTime());
        book.setLastSeq(event.getSequenceId());
        booksByExchange.put(exchangeId, book);

        // Rebuild the whole consolidated book from scratch on every event: flatten every
        // exchange's maintained book into one list, tagging each level with its exchange_id.
        // (Cheap enough — book depth is small — and avoids incremental-merge state bugs.)
        List<ConsolidatedLevel> merged = new ArrayList<>();
        long maxEventTime = Long.MIN_VALUE;
        for (Map.Entry<Integer, ExchangeBook> entry : booksByExchange.entries()) {
            ExchangeBook exchangeBook = entry.getValue();
            // Output event_time = newest contributing book, so freshness reflects the
            // most recent exchange update, not whichever one happened to trigger this rebuild.
            maxEventTime = Math.max(maxEventTime, exchangeBook.getEventTime());
            for (Map.Entry<BigDecimal, String> level : exchangeBook.getLevels().entrySet()) {
                merged.add(new ConsolidatedLevel(
                        entry.getKey(), level.getKey().toPlainString(), level.getValue()));
            }
        }

        // Sort the union (price asc/desc by side, then quantity desc); levels are NOT summed,
        // so equal-price levels from different exchanges stay as separate adjacent entries.
        merged.sort(comparator);
        out.collect(new ConsolidatedOrderBook(event.getPairId(), side, merged, maxEventTime));
    }
}
