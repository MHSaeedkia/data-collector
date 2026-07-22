package io.tibobit.normalizer.aggregate;

import io.tibobit.normalizer.model.OrderBookSnapshot;
import io.tibobit.normalizer.model.PriceLevel;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits each job-5 {@link OrderBookSnapshot} into two per-side {@link ExchangeBook}s (asks, bids)
 * so the ported {@link CrossExchangeConsolidator} (keyed {@code (pair_id, side)}) can be reused
 * near-verbatim. Each level is stamped with the snapshot's {@code exchange_id}.
 *
 * <p>An emitted book always carries both sides; on job 5's reset both sides are empty, which
 * produces two empty ExchangeBooks and drops that exchange from the union. A null side (defensive —
 * job 5 emits both) is treated as empty.
 */
public class SnapshotSplitter implements FlatMapFunction<OrderBookSnapshot, ExchangeBook> {

    @Override
    public void flatMap(OrderBookSnapshot snapshot, Collector<ExchangeBook> out) {
        out.collect(toExchangeBook(snapshot, "asks", snapshot.getAsks()));
        out.collect(toExchangeBook(snapshot, "bids", snapshot.getBids()));
    }

    private static ExchangeBook toExchangeBook(OrderBookSnapshot snapshot, String side,
                                               List<PriceLevel> levels) {
        List<ConsolidatedLevel> consolidated = new ArrayList<>();
        if (levels != null) {
            for (PriceLevel level : levels) {
                consolidated.add(new ConsolidatedLevel(
                        snapshot.getExchangeId(), level.getPrice(), level.getQuantity()));
            }
        }
        return new ExchangeBook(snapshot.getPairId(), snapshot.getExchangeId(), side,
                consolidated, snapshot.getEventTime());
    }
}
