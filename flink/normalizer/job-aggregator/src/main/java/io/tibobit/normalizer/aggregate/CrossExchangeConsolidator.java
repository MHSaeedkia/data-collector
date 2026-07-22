package io.tibobit.normalizer.aggregate;

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
 * Unions every exchange's book for a pair+side into one consolidated book. Keyed by
 * {@code (pair_id, side)} (see {@link AggregatorJob}); holds the latest {@link ExchangeBook} per
 * exchange in {@code MapState<exchange_id, ExchangeBook>}.
 *
 * <p>On each incoming ExchangeBook it replaces that exchange's entry, then rebuilds the union:
 * <ul>
 *   <li>concat all exchanges' levels — quantities are NEVER summed, so equal prices from different
 *       exchanges stay as separate adjacent entries.</li>
 *   <li>sort by price (asks ascending, bids descending), tie-broken by larger quantity first,
 *       comparing as BigDecimal (see memory/project_bigdecimal_rules.md).</li>
 * </ul>
 * {@code event_time} on the output = max across the contributing exchange books. An empty
 * ExchangeBook (job 5's reset ⇒ empty book) replaces the exchange's entry and contributes no
 * levels, so that exchange drops out of the consolidated book.
 *
 * <p>The sort direction is chosen per record from {@code side} (not the constructor) because one
 * operator instance serves both asks and bids keys. Ported from the deprecated orderbook-
 * consolidator's stage-2 operator — fed full books directly instead of per-level diffs.
 */
public class CrossExchangeConsolidator
        extends KeyedProcessFunction<String, ExchangeBook, ConsolidatedOrderBook> {

    private transient MapState<Integer, ExchangeBook> booksByExchange;
    private transient Comparator<ConsolidatedLevel> asksComparator; // price ascending
    private transient Comparator<ConsolidatedLevel> bidsComparator; // price descending

    // State and comparators are built in open() (not the constructor): MapState is provided per
    // keyed instance, and Comparator isn't Serializable so it can't be a shipped field.
    @Override
    public void open(OpenContext openContext) {
        MapStateDescriptor<Integer, ExchangeBook> descriptor = new MapStateDescriptor<>(
                "booksByExchange",
                TypeInformation.of(Integer.class),
                TypeInformation.of(ExchangeBook.class));
        booksByExchange = getRuntimeContext().getMapState(descriptor);

        Comparator<ConsolidatedLevel> byPriceAsc =
                Comparator.comparing(level -> new BigDecimal(level.getPrice()));
        // Secondary: larger quantity first, regardless of side.
        Comparator<ConsolidatedLevel> byQuantityDesc = Comparator.<ConsolidatedLevel, BigDecimal>comparing(
                level -> new BigDecimal(level.getQuantity())).reversed();
        asksComparator = byPriceAsc.thenComparing(byQuantityDesc);
        bidsComparator = byPriceAsc.reversed().thenComparing(byQuantityDesc);
    }

    @Override
    public void processElement(
            ExchangeBook book,
            Context ctx,
            Collector<ConsolidatedOrderBook> out) throws Exception {

        booksByExchange.put(book.getExchangeId(), book);

        // Union every exchange's levels (already stamped with their exchange_id); never summed.
        List<ConsolidatedLevel> merged = new ArrayList<>();
        long maxEventTime = Long.MIN_VALUE;
        for (Map.Entry<Integer, ExchangeBook> entry : booksByExchange.entries()) {
            ExchangeBook exchangeBook = entry.getValue();
            maxEventTime = Math.max(maxEventTime, exchangeBook.getEventTime());
            if (exchangeBook.getLevels() != null) {
                merged.addAll(exchangeBook.getLevels());
            }
        }

        // Sort the union by side; equal-price levels from different exchanges stay separate.
        merged.sort("asks".equals(book.getSide()) ? asksComparator : bidsComparator);
        out.collect(new ConsolidatedOrderBook(book.getPairId(), book.getSide(), merged, maxEventTime));
    }
}
