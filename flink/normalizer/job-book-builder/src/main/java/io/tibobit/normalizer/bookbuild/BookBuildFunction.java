package io.tibobit.normalizer.bookbuild;

import io.tibobit.normalizer.decimal.Decimals;
import io.tibobit.normalizer.model.Lineage;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 * price (a lesson from the aggregation stage).
 *
 * <p><b>Lineage is the one thing here that fans in.</b> Every other step has exactly one parent, but
 * a book is state accumulated over many events, so the emitted snapshot's {@code source_ids} lists
 * the id of every event still holding a resting level — which is why each level's originating
 * event is tracked in the state itself ({@link RestingLevel}). The triggering event is always
 * included too, even when it left nothing resting: an event that only deletes levels, or a reset
 * that empties the book, still caused this record and must not vanish from the chain.
 *
 * <p>No checkpointing is configured anywhere on this platform yet, so after a restart a book is
 * empty until the next snapshot re-seeds it. Known, shared, not solved here.
 */
public class BookBuildFunction
        extends KeyedProcessFunction<String, RawOrderBookEvent, OrderBookSnapshot> {

    private transient MapState<String, RestingLevel> asks;
    private transient MapState<String, RestingLevel> bids;

    @Override
    public void open(OpenContext openContext) {
        asks = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("asks", String.class, RestingLevel.class));
        bids = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("bids", String.class, RestingLevel.class));
    }

    @Override
    public void processElement(RawOrderBookEvent event, Context ctx,
                               Collector<OrderBookSnapshot> out) throws Exception {
        event.getPipelineTimings().setBookBuildIn(System.currentTimeMillis());

        if ("reset".equals(event.getType())) {
            // Job 2 emits a reset marker on a sequence gap: clear
            // the whole book so the emitted snapshot is empty and the exchange drops out downstream,
            // rather than serving its pre-gap diverged book until the next real snapshot.
            asks.clear();
            bids.clear();
        } else {
            boolean replace = "snapshot".equals(event.getType());
            applySide(asks, event.getAsks(), replace, event.getId());
            applySide(bids, event.getBids(), replace, event.getId());
        }

        List<RestingLevel> restingAsks = sorted(asks, ASCENDING);
        List<RestingLevel> restingBids = sorted(bids, DESCENDING);

        OrderBookSnapshot book = new OrderBookSnapshot(
                event.getExchangeId(), event.getPairId(), event.getEventTime(),
                event.getSequenceId(), priceLevels(restingAsks), priceLevels(restingBids));
        // The book is state built from many events, but simulation is a property of the feed, not of
        // a level — so the emitted book carries the flag of the event that produced it. Kept out of
        // MapState deliberately: it is not per-price, and a feed does not switch mid-stream.
        book.setSimulation(event.getSimulation());
        book.setSourceIds(restingSources(event.getId(), restingAsks, restingBids));
        book.setId(Lineage.newId());
        book.setPipelineTimings(event.getPipelineTimings());

        book.getPipelineTimings().setBookBuildOut(System.currentTimeMillis());
        out.collect(book);
    }

    /**
     * Applies one side of an event to its state. {@code levels == null} means the event carried no
     * report for this side (ex3's absent half) — leave the state untouched, including on a
     * snapshot. {@code replace} clears the side first, turning a merge into a wholesale replace.
     */
    private static void applySide(MapState<String, RestingLevel> side, List<PriceLevel> levels,
                                  boolean replace, String id) throws Exception {
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
                // The level's origin is the event that last SET it, so this overwrites the previous
                // owner — a level updated by a later event belongs to that event, not the first one.
                side.put(price, new RestingLevel(
                        price, Decimals.canonicalize(quantity), id));
            }
        }
    }

    private static final Comparator<RestingLevel> ASCENDING =
            Comparator.comparing(level -> new BigDecimal(level.getPrice()));
    private static final Comparator<RestingLevel> DESCENDING = ASCENDING.reversed();

    /**
     * MapState iteration order is undefined, so the book is sorted on the way out — asks ascending,
     * bids descending, the platform's convention — to keep the emitted snapshot deterministic. That
     * determinism is what also makes {@code source_ids} stable across runs, since it is collected in
     * this order.
     */
    private static List<RestingLevel> sorted(MapState<String, RestingLevel> side,
                                             Comparator<RestingLevel> order) throws Exception {
        List<RestingLevel> levels = new ArrayList<>();
        for (Map.Entry<String, RestingLevel> entry : side.entries()) {
            levels.add(entry.getValue());
        }
        levels.sort(order);
        return levels;
    }

    /** Drops the lineage back off, leaving the wire shape of the book unchanged. */
    private static List<PriceLevel> priceLevels(List<RestingLevel> resting) {
        List<PriceLevel> levels = new ArrayList<>(resting.size());
        for (RestingLevel level : resting) {
            levels.add(new PriceLevel(level.getPrice(), level.getQuantity()));
        }
        return levels;
    }

    /**
     * Every event this book is made of: the one that triggered the emit, then the distinct events
     * still holding a resting level, asks before bids.
     *
     * <p>The triggering event goes first and unconditionally. Without that, an event that only
     * deleted levels — or a reset, which empties the book entirely — would produce a record whose
     * source_ids do not mention the event that caused it, silently breaking the chain at exactly the
     * moments worth tracing. Deduplicated because one event routinely sets many levels.
     */
    private static List<String> restingSources(String triggeringId,
                                               List<RestingLevel> asks, List<RestingLevel> bids) {
        Set<String> sources = new LinkedHashSet<>();
        sources.add(triggeringId);
        for (RestingLevel level : asks) {
            sources.add(level.getId());
        }
        for (RestingLevel level : bids) {
            sources.add(level.getId());
        }
        return new ArrayList<>(sources);
    }
}
