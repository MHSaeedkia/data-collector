package io.tibobit.merger;

import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Sums job 6's unioned levels into one level per price.
 *
 * <pre>
 *   in  (aggregated, union)         out (merged, summed)
 *   {ex:1, price:10, qty:3, src:A}
 *   {ex:2, price:10, qty:4, src:B}  {price:10, qty:7, exchange_ids:[1,2], source_ids:[A,B]}
 *   {ex:1, price:11, qty:5, src:A}  {price:11, qty:5, exchange_ids:[1],   source_ids:[A]}
 * </pre>
 *
 * <p>Stateless, one record in / one record out: job 6 has already done the cross-exchange fan-in,
 * so a single aggregated record is the complete book for that pair+side and the merge is a pure
 * function of it. That is the entire reason this job needs no keyed state, no per-exchange MapState
 * and no reset handling — an exchange that job 6 dropped is simply already absent from the input.
 */
public class PriceMergeFunction implements MapFunction<AggregatedOrderBook, MergedOrderBook> {

    @Override
    public MergedOrderBook map(AggregatedOrderBook book) {
        return new MergedOrderBook(
                book.getPairId(),
                book.getSide(),
                UUID.randomUUID().toString(),
                book.getId(),
                book.getEventTime(),
                merge(book.getLevels(), book.getSide()));
    }

    static List<MergedLevel> merge(List<AggregatedLevel> levels, String side) {
        if (levels == null || levels.isEmpty()) {
            return List.of();
        }

        // Grouped by (canonical price, simulation) — never by the raw wire string, which carries no
        // formatting guarantee, so "10.00" and "10" are one price (memory/project_bigdecimal_rules.md).
        Map<String, Group> groups = new LinkedHashMap<>();
        for (AggregatedLevel level : levels) {
            String price = Decimals.canonicalize(new BigDecimal(level.getPrice()));
            groups.computeIfAbsent(price + "|" + level.getSimulation(),
                            key -> new Group(price, level.getSimulation()))
                    .add(level);
        }

        List<MergedLevel> merged = new ArrayList<>(groups.size());
        for (Group group : groups.values()) {
            merged.add(group.toLevel());
        }
        merged.sort(ordering(side));
        return merged;
    }

    /**
     * Asks ascending, bids descending — the same convention as every other book in the pipeline.
     * The tie is between a live and a simulated level at one price (nothing else can collide after
     * grouping), and live sorts first. Input order would very nearly do here, since job 6 already
     * sorts by price, but its tie-break is quantity — which says nothing about where the simulated
     * twin lands. Sorting explicitly is what makes the output deterministic.
     */
    private static Comparator<MergedLevel> ordering(String side) {
        Comparator<MergedLevel> byPrice = Comparator.comparing(level -> new BigDecimal(level.getPrice()));
        if ("bids".equals(side)) {
            byPrice = byPrice.reversed();
        }
        return byPrice.thenComparingInt(MergedLevel::getSimulation);
    }

    /** One (price, simulation) bucket, accumulating quantity and the lineage of its contributors. */
    private static final class Group {
        private final String price;
        private final int simulation;
        private final List<Integer> exchangeIds = new ArrayList<>();
        private final List<String> sourceIds = new ArrayList<>();
        private BigDecimal quantity = BigDecimal.ZERO;

        Group(String price, int simulation) {
            this.price = price;
            this.simulation = simulation;
        }

        void add(AggregatedLevel level) {
            quantity = quantity.add(new BigDecimal(level.getQuantity()));
            exchangeIds.add(level.getExchangeId());
            sourceIds.add(level.getSourceId());
        }

        MergedLevel toLevel() {
            return new MergedLevel(simulation, exchangeIds, sourceIds, price, Decimals.canonicalize(quantity));
        }
    }
}
