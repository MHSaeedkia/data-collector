package io.tibobit.normalizer.rebase;

import io.tibobit.normalizer.decimal.Decimals;
import io.tibobit.normalizer.lookup.RefreshingLookup;
import io.tibobit.normalizer.model.PriceLevel;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import io.tibobit.normalizer.model.RejectedOrderBookEvent;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Job 3 — rebase. Stateless: every level's price is shifted by {@code price_amount_rebase} and
 * every quantity by {@code volume_amount_rebase} powers of ten (exact, {@code scaleByPowerOfTen}
 * — no double anywhere, see memory/project_bigdecimal_rules.md). Exponents come per
 * {@code (exchange_id, pair_id)} from a {@link RefreshingLookup} over exchange_markets.
 *
 * <p>An event whose (exchange, pair) has no exchange_markets row goes to the {@link #REJECTED}
 * dead-letter with reason {@code no_rebase_row} (user decision 2026-07-18). Passing it through
 * un-rebased would emit silently corrupt prices — orders of magnitude off — with nothing
 * downstream able to tell rebased from un-rebased. In practice the row is near-guaranteed to
 * exist: job 1 resolved this very event's pair_id from the same table, so only a refresh race
 * (row deleted mid-flight) reaches this branch.
 *
 * <p>A null side is NOT an empty side: ex3 wallex sends one side per message and the other stays
 * null, so null in ⇒ null out. Job 5 is where the two sides finally merge.
 */
public class RebaseFunction extends ProcessFunction<RawOrderBookEvent, RawOrderBookEvent> {

    /** Dead-letter side output. Shared by the job wiring and the tests. */
    public static final OutputTag<RejectedOrderBookEvent> REJECTED =
            new OutputTag<>("rejected") {};

    static final String NO_REBASE_ROW = "no_rebase_row";

    private final RefreshingLookup<String, RebaseFactors> factors;

    public RebaseFunction(RefreshingLookup<String, RebaseFactors> factors) {
        this.factors = factors;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        factors.open();
    }

    @Override
    public void processElement(RawOrderBookEvent event, Context ctx,
                               Collector<RawOrderBookEvent> out) {
        event.getPipelineTimings().setRebaseIn(System.currentTimeMillis());

        RebaseFactors factor =
                factors.get(RebaseFactorsLoader.key(event.getExchangeId(), event.getPairId()));
        if (factor == null) {
            // rebaseOut stays null — the event never leaves the rebaser onto the main stream.
            ctx.output(REJECTED,
                    new RejectedOrderBookEvent(event, NO_REBASE_ROW, System.currentTimeMillis()));
            return;
        }

        event.setAsks(rebaseLevels(event.getAsks(), factor));
        event.setBids(rebaseLevels(event.getBids(), factor));

        event.getPipelineTimings().setRebaseOut(System.currentTimeMillis());
        out.collect(event);
    }

    @Override
    public void close() {
        factors.close();
    }

    /** Null side (ex3's absent half) passes through as null; an empty list stays empty. */
    private static List<PriceLevel> rebaseLevels(List<PriceLevel> levels, RebaseFactors factor) {
        if (levels == null) {
            return null;
        }
        List<PriceLevel> rebased = new ArrayList<>(levels.size());
        for (PriceLevel level : levels) {
            rebased.add(new PriceLevel(
                    shift(level.getPrice(), factor.getPrice()),
                    shift(level.getQuantity(), factor.getVolume())));
        }
        return rebased;
    }

    private static String shift(String value, int rebase) {
        return Decimals.canonicalize(Decimals.rebase(new BigDecimal(value), rebase));
    }
}
