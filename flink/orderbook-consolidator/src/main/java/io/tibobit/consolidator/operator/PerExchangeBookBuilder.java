package io.tibobit.consolidator.operator;

import io.tibobit.consolidator.model.ConsolidatedLevel;
import io.tibobit.consolidator.model.ExchangeBook;
import io.tibobit.consolidator.model.PriceLevelEvent;
import io.tibobit.consolidator.model.StoredLevel;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Stage 1 — maintains one exchange's book for a pair+side. Keyed by
 * {@code (pair_id, exchange_id, side)} (see OrderBookConsolidatorJob), so each keyed instance
 * sees a single exchange's stream of single-level events and holds only that exchange's levels
 * in {@code MapState<price, StoredLevel>}.
 *
 * Per event:
 *  - R1 upsert-latest-by-event_time: a newer (or equal) {@code event_time} for a price wins;
 *    an older one is stale and dropped.
 *  - R2 remove: {@code quantity == 0} deletes that price.
 * After a change it emits that exchange's whole book as an {@link ExchangeBook} whose levels are
 * {@link ConsolidatedLevel}s stamped with this exchange_id (so stage 2's union is a straight
 * concat), and whose {@code event_time} is the max across the remaining levels.
 *
 * Prices are keyed by a canonical decimal string (BigDecimal, trailing zeros stripped) so equal
 * prices in different formats collapse to one level — MapState is hash-based, so unlike a TreeMap
 * it would not collapse them for us (see memory/project_bigdecimal_rules.md).
 */
public class PerExchangeBookBuilder
        extends KeyedProcessFunction<String, PriceLevelEvent, ExchangeBook> {

    private transient MapState<String, StoredLevel> levels;

    // State is built in open() (not the constructor) because MapState is provided per keyed
    // instance by the runtime. open(OpenContext) is the Flink 2.x signature.
    @Override
    public void open(OpenContext openContext) {
        MapStateDescriptor<String, StoredLevel> descriptor = new MapStateDescriptor<>(
                "levels",
                TypeInformation.of(String.class),
                TypeInformation.of(StoredLevel.class));
        levels = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(
            PriceLevelEvent event,
            Context ctx,
            Collector<ExchangeBook> out) throws Exception {

        String price = new BigDecimal(event.getPrice()).stripTrailingZeros().toPlainString();
        StoredLevel existing = levels.get(price);

        // R1: ignore a stale event (older event_time than what we already hold for this price).
        if (existing != null && event.getEventTime() < existing.getEventTime()) {
            return;
        }

        if (new BigDecimal(event.getQuantity()).signum() == 0) {
            // R2: quantity 0 removes the level. Nothing stored → nothing changed, skip the emit.
            if (existing == null) {
                return;
            }
            levels.remove(price);
        } else {
            // R1: upsert the latest quantity for this price.
            levels.put(price, new StoredLevel(event.getQuantity(), event.getEventTime()));
        }

        out.collect(buildBook(event));
    }

    // Flatten the maintained levels into this exchange's book. event_time = max across the
    // remaining levels; when the book is now empty, fall back to the triggering event's time.
    private ExchangeBook buildBook(PriceLevelEvent event) throws Exception {
        List<ConsolidatedLevel> bookLevels = new ArrayList<>();
        long maxEventTime = Long.MIN_VALUE;
        for (Map.Entry<String, StoredLevel> entry : levels.entries()) {
            StoredLevel level = entry.getValue();
            maxEventTime = Math.max(maxEventTime, level.getEventTime());
            bookLevels.add(new ConsolidatedLevel(
                    event.getExchangeId(), entry.getKey(), level.getQuantity()));
        }
        if (bookLevels.isEmpty()) {
            maxEventTime = event.getEventTime();
        }
        return new ExchangeBook(
                event.getPairId(), event.getExchangeId(), event.getSide(), bookLevels, maxEventTime);
    }
}
