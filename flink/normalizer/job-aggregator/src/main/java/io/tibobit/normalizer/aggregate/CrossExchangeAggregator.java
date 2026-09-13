package io.tibobit.normalizer.aggregate;

import io.tibobit.normalizer.model.Lineage;

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
 * Unions every exchange's book for a pair+side into one aggregated book. Keyed by
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
 * levels, so that exchange drops out of the aggregated book.
 *
 * <p>The sort direction is chosen per record from {@code side} (not the constructor) because one
 * operator instance serves both asks and bids keys. Fed job 5's full per-exchange books directly,
 * keyed by (pair_id, side).
 */
public class CrossExchangeAggregator
        extends KeyedProcessFunction<String, ExchangeBook, AggregatedOrderBook> {

    // Secondary: larger quantity first, regardless of side.
    private static final Comparator<ParsedLevel> BY_QUANTITY_DESC =
            Comparator.<ParsedLevel, BigDecimal>comparing(parsed -> parsed.quantity).reversed();
    private static final Comparator<ParsedLevel> ASKS =
            Comparator.<ParsedLevel, BigDecimal>comparing(parsed -> parsed.price).thenComparing(BY_QUANTITY_DESC);
    private static final Comparator<ParsedLevel> BIDS =
            Comparator.<ParsedLevel, BigDecimal>comparing(parsed -> parsed.price).reversed().thenComparing(BY_QUANTITY_DESC);

    private transient MapState<Integer, ExchangeBook> booksByExchange;

    // MapState is provided per keyed instance, so it is built in open(), not the constructor.
    @Override
    public void open(OpenContext openContext) {
        MapStateDescriptor<Integer, ExchangeBook> descriptor = new MapStateDescriptor<>(
                "booksByExchange",
                TypeInformation.of(Integer.class),
                TypeInformation.of(ExchangeBook.class));
        booksByExchange = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(
            ExchangeBook book,
            Context ctx,
            Collector<AggregatedOrderBook> out) throws Exception {

        booksByExchange.put(book.getExchangeId(), book);

        // Union every exchange's levels (already stamped with their exchange_id); never summed.
        // Each price and quantity is parsed ONCE here. Parsing inside the comparator did it twice per
        // comparison (~n log n times for a ~750-level book), which was 27% of the live operator thread.
        List<ParsedLevel> union = new ArrayList<>();
        long maxEventTime = Long.MIN_VALUE;
        for (Map.Entry<Integer, ExchangeBook> entry : booksByExchange.entries()) {
            ExchangeBook exchangeBook = entry.getValue();
            maxEventTime = Math.max(maxEventTime, exchangeBook.getEventTime());
            if (exchangeBook.getLevels() != null) {
                for (AggregatedLevel level : exchangeBook.getLevels()) {
                    union.add(new ParsedLevel(level));
                }
            }
        }

        // Sort the union by side; equal-price levels from different exchanges stay separate.
        union.sort("asks".equals(book.getSide()) ? ASKS : BIDS);
        List<AggregatedLevel> merged = new ArrayList<>(union.size());
        for (ParsedLevel parsed : union) {
            merged.add(parsed.level);
        }

        AggregatedOrderBook aggregated =
                new AggregatedOrderBook(book.getPairId(), book.getSide(), merged, maxEventTime);
        // Each level already carries its own source_id, stamped back at the split — nothing to
        // gather here, only this record's own id to mint.
        aggregated.setId(Lineage.newId());
        out.collect(aggregated);
    }

    /** A level with its sort keys parsed as BigDecimal (see memory/project_bigdecimal_rules.md). */
    private static final class ParsedLevel {
        private final AggregatedLevel level;
        private final BigDecimal price;
        private final BigDecimal quantity;

        ParsedLevel(AggregatedLevel level) {
            this.level = level;
            this.price = new BigDecimal(level.getPrice());
            this.quantity = new BigDecimal(level.getQuantity());
        }
    }
}
