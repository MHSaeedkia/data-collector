package io.tibobit.normalizer.aggregate;

import io.tibobit.normalizer.model.OrderBookSnapshot;
import io.tibobit.normalizer.model.PriceLevel;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits each job-5 {@link OrderBookSnapshot} into two per-side {@link ExchangeBook}s (asks, bids)
 * so the ported {@link CrossExchangeAggregator} (keyed {@code (pair_id, side)}) can be reused
 * near-verbatim. Each level is stamped with the snapshot's {@code exchange_id}, its
 * {@code simulation} flag and its {@code id} (as the level's {@code source_id}) — the union
 * mixes exchanges, so none of the three mean anything except per level. Stamping the source here,
 * at the split, is what lets the aggregator stay a pure union: by the time levels are merged, each
 * already knows where it came from.
 *
 * <p><b>The snapshot's own per-level {@code source_id} is deliberately NOT copied through</b>, even
 * though it would shorten a trace by one hop. It names a job-4 event, which this record never read;
 * stamping it here would make the aggregated record point straight past job 5 at a record it has no
 * relationship with, and the job-5 hop would then appear nowhere in the lineage at all. A level
 * names the snapshot; the snapshot's level of the same price names the event. Each hop, one step.
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
        List<AggregatedLevel> aggregated = new ArrayList<>();
        if (levels != null) {
            for (PriceLevel level : levels) {
                aggregated.add(new AggregatedLevel(
                        snapshot.getExchangeId(), snapshot.getSimulation(), snapshot.getId(),
                        level.getPrice(), level.getQuantity()));
            }
        }
        return new ExchangeBook(snapshot.getPairId(), snapshot.getExchangeId(), side,
                aggregated, snapshot.getEventTime());
    }
}
