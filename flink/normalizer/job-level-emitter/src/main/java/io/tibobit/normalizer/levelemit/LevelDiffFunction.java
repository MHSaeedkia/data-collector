package io.tibobit.normalizer.levelemit;

import io.tibobit.normalizer.decimal.Decimals;
import io.tibobit.normalizer.model.OrderBookSnapshot;
import io.tibobit.normalizer.model.PriceLevel;
import io.tibobit.normalizer.model.PriceLevelEvent;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Job 6 — level emitter, keyed by {@code (exchange_id, pair_id)}. Job 5 emits the WHOLE book on
 * every event; the consolidator wants one message per CHANGED level. This function is the
 * translation: it keeps the last book it emitted (one {@link MapState} per side, canonicalized
 * price → quantity) and publishes only the difference.
 *
 * <p><b>The diff.</b> A price whose quantity changed, or that is new, is emitted as an upsert. A
 * price that was in the last book and is absent from this one is emitted with {@code quantity =
 * "0"} — the consolidator's removal signal (see PerExchangeBookBuilder). An unchanged book emits
 * nothing at all, which is the whole point: job 5 re-emits the full book even when a single level
 * moved, and without this the consolidator would be rewritten wholesale on every tick.
 *
 * <p><b>Why hold the book again, when job 5 already holds it?</b> Because job 5's state is what
 * the book IS and this state is what we have already TOLD the consolidator — they diverge on
 * restart, and it is the second one that decides what to send. Keeping them in one job would have
 * conflated the two.
 *
 * <p>The output subject {@code price-level-event} is frozen: the consolidator has consumed it
 * since before this pipeline existed, and NiFi's current normalized output writes it today. This
 * job's records must be indistinguishable from those, which is what makes the M8 cutover a switch
 * rather than a migration.
 *
 * <p>Same cold-start gap as job 5 (no checkpointing anywhere on this platform): after a restart
 * this state is empty, so the first book re-emits every level. That is harmless — the levels are
 * upserts and the consolidator converges — but stale levels that vanished during the downtime are
 * NOT deleted, since a book we never saw cannot be diffed against.
 */
public class LevelDiffFunction
        extends KeyedProcessFunction<String, OrderBookSnapshot, PriceLevelEvent> {

    private transient MapState<String, String> lastAsks;
    private transient MapState<String, String> lastBids;

    @Override
    public void open(OpenContext openContext) {
        lastAsks = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("last-asks", String.class, String.class));
        lastBids = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("last-bids", String.class, String.class));
    }

    @Override
    public void processElement(OrderBookSnapshot book, Context ctx,
                               Collector<PriceLevelEvent> out) throws Exception {
        book.getPipelineTimings().setLevelEmitIn(System.currentTimeMillis());

        // Buffered rather than collected inline so level_emit_out is stamped once the diff is
        // actually done — the timings are only worth carrying if they measure real work.
        List<PriceLevelEvent> events = new ArrayList<>();
        diffSide(book, "asks", book.getAsks(), lastAsks, events);
        diffSide(book, "bids", book.getBids(), lastBids, events);

        book.getPipelineTimings().setLevelEmitOut(System.currentTimeMillis());
        events.forEach(out::collect);
    }

    private static void diffSide(OrderBookSnapshot book, String side, List<PriceLevel> levels,
                                 MapState<String, String> last, List<PriceLevelEvent> out)
            throws Exception {
        Map<String, String> current = canonicalize(levels);

        for (Map.Entry<String, String> level : current.entrySet()) {
            if (!level.getValue().equals(last.get(level.getKey()))) {
                out.add(event(book, side, level.getKey(), level.getValue()));
            }
        }

        // Deletes: whatever we last told the consolidator about but this book no longer has.
        for (String price : keysOf(last)) {
            if (!current.containsKey(price)) {
                out.add(event(book, side, price, "0"));
            }
        }

        last.clear();
        last.putAll(current);
    }

    /**
     * Prices AND quantities are canonicalized ({@code stripTrailingZeros}) before comparison. Job 5
     * already canonicalizes both, so in practice this is belt-and-braces — but the comparison is a
     * string equality on decimals, and "1.0" vs "1" differing would emit an upsert for a level that
     * did not move.
     */
    private static Map<String, String> canonicalize(List<PriceLevel> levels) {
        Map<String, String> canonical = new LinkedHashMap<>();
        for (PriceLevel level : levels) {
            canonical.put(Decimals.canonicalize(new BigDecimal(level.getPrice())),
                    Decimals.canonicalize(new BigDecimal(level.getQuantity())));
        }
        return canonical;
    }

    /** Materialized because {@code last} is cleared and rewritten right after being iterated. */
    private static List<String> keysOf(MapState<String, String> last) throws Exception {
        List<String> keys = new ArrayList<>();
        for (String key : last.keys()) {
            keys.add(key);
        }
        return keys;
    }

    private static PriceLevelEvent event(OrderBookSnapshot book, String side,
                                         String price, String quantity) {
        PriceLevelEvent event = new PriceLevelEvent(
                book.getExchangeId(), book.getPairId(), side, book.getEventTime(), price, quantity);
        event.setPipelineTimings(book.getPipelineTimings());
        return event;
    }
}
