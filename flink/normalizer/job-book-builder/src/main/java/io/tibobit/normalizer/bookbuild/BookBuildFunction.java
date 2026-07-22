package io.tibobit.normalizer.bookbuild;

import io.tibobit.normalizer.decimal.Decimals;
import io.tibobit.normalizer.model.OrderBookSnapshot;
import io.tibobit.normalizer.model.PriceLevel;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Job 5 — book builder, keyed by {@code (exchange_id, pair_id)}. Holds the live book of each
 * market in {@link MapState} (one map per side, price → quantity) and emits the WHOLE book as an
 * {@link OrderBookSnapshot} on every accepted event. Job 2 already enforced the sequence rules and
 * the topics are single-partition, so nothing is re-validated here.
 *
 * <p><b>Snapshot vs update.</b> A snapshot replaces a side wholesale, an update merges into it.
 * The difference is only whether the side is cleared first — after that both apply the same
 * per-level rule, so a zero quantity means "no level here" in either kind of event.
 *
 * <p><b>Null side is not an empty side (ex3 wallex).</b> Wallex sends a full snapshot ONE side per
 * message, with the other side null. Replacing "wholesale" therefore means replacing only the
 * sides actually present: a null side leaves that side's state exactly as it was, while an empty
 * array is a real report of "this side has no liquidity" and clears it. This is the one place
 * wallex's two messages become a single two-sided book.
 *
 * <p><b>Prices are canonicalized before use as map keys</b> ({@code stripTrailingZeros}), because
 * MapState is hash-based: without it "10.50" and "10.5" would be two different levels for the same
 * price (a lesson from the consolidator).
 *
 * <p>No checkpointing is configured anywhere on this platform yet, so after a restart a book is
 * empty until the next snapshot re-seeds it. Known, shared, not solved here.
 */
public class BookBuildFunction
        extends KeyedProcessFunction<String, RawOrderBookEvent, OrderBookSnapshot> {

    private transient MapState<String, String> asks;
    private transient MapState<String, String> bids;

    @Override
    public void open(OpenContext openContext) {
        asks = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("asks", String.class, String.class));
        bids = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("bids", String.class, String.class));
    }

    @Override
    public void processElement(RawOrderBookEvent event, Context ctx,
                               Collector<OrderBookSnapshot> out) throws Exception {
        event.getPipelineTimings().setBookBuildIn(System.currentTimeMillis());

        if ("reset".equals(event.getType())) {
            // Job 2 emits a reset marker on a sequence gap (see plans/aggregator-gap-drop.md): clear
            // the whole book so the emitted snapshot is empty and the exchange drops out downstream,
            // rather than serving its pre-gap diverged book until the next real snapshot.
            asks.clear();
            bids.clear();
        } else {
            boolean replace = "snapshot".equals(event.getType());
            applySide(asks, event.getAsks(), replace);
            applySide(bids, event.getBids(), replace);
        }

        OrderBookSnapshot book = new OrderBookSnapshot(
                event.getExchangeId(), event.getPairId(), event.getEventTime(),
                event.getSequenceId(), sorted(asks, ASCENDING), sorted(bids, DESCENDING));
        book.setPipelineTimings(event.getPipelineTimings());

        book.getPipelineTimings().setBookBuildOut(System.currentTimeMillis());
        out.collect(book);
    }

    /**
     * Applies one side of an event to its state. {@code levels == null} means the event carried no
     * report for this side (ex3's absent half) — leave the state untouched, including on a
     * snapshot. {@code replace} clears the side first, turning a merge into a wholesale replace.
     */
    private static void applySide(MapState<String, String> side, List<PriceLevel> levels,
                                  boolean replace) throws Exception {
        if (levels == null) {
            return;
        }
        if (replace) {
            side.clear();
        }
        for (PriceLevel level : levels) {
            String price = Decimals.canonicalize(new BigDecimal(level.getPrice()));
            BigDecimal quantity = new BigDecimal(level.getQuantity());
            if (quantity.signum() == 0) {
                // Delete. NOTE: a zero quantity here does NOT mean the exchange sent a delete —
                // job 4 also emits "0" for any nonzero size that truncates away at the market's
                // quantity_precision (see [[precision]]), so dust arrives as a delete too. That is
                // intentional: a size below the lot precision is not representable liquidity and
                // must not rest in the book. Don't "fix" a delete you can't find in the raw feed.
                side.remove(price);
            } else {
                side.put(price, Decimals.canonicalize(quantity));
            }
        }
    }

    private static final Comparator<PriceLevel> ASCENDING =
            Comparator.comparing(level -> new BigDecimal(level.getPrice()));
    private static final Comparator<PriceLevel> DESCENDING = ASCENDING.reversed();

    /**
     * MapState iteration order is undefined, so the book is sorted on the way out — asks ascending,
     * bids descending, the platform's convention — to keep the emitted snapshot deterministic.
     */
    private static List<PriceLevel> sorted(MapState<String, String> side,
                                           Comparator<PriceLevel> order) throws Exception {
        List<PriceLevel> levels = new ArrayList<>();
        for (Map.Entry<String, String> entry : side.entries()) {
            levels.add(new PriceLevel(entry.getKey(), entry.getValue()));
        }
        levels.sort(order);
        return levels;
    }
}
