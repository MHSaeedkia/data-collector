package io.tibobit.adjustment;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.math.BigDecimal;

/**
 * Our profit — our own margin, applied on top of whatever the exchange quoted.
 *
 * <p>Adds a per-LEVEL percent of the price that level arrived with, and records the rate it used on
 * that level so the published event says what was charged rather than only what the result was.
 * The direction — up on {@code asks}, down on {@code bids} — and the fact that the amount is sized
 * off the ORIGINAL price both live in one place, {@link Prices#applyPerLevelPercent}.
 *
 * <p>Sized off the exchange's ORIGINAL price, not off the commission-adjusted one, so this margin
 * is independent of whatever the commission stage charged.
 *
 * <p><b>The rate is read from {@code exchange_markets.our_profit_percent}</b> (2026-08-25), keyed
 * per {@code (exchange_id, pair_id)} via a {@link RefreshingLookup} — this margin is OURS, not an
 * exchange's, but the user confirmed it still varies per exchange for the same market, so it is a
 * per-LEVEL rate, not a per-record one (see memory/project_adjustment.md).
 */
public class OurProfitFunction extends RichMapFunction<AdjustedOrderBook, AdjustedOrderBook> {

    /**
     * Fallback when exchange_markets has no row for a level's (exchange, pair) — the pre-DB
     * constant. This job has no dead-letter output to raise a missing row into (unlike job 3's
     * no_rebase_row), and silently charging 0% would under-charge rather than merely go stale, so
     * it falls back to what used to be hardcoded instead of either.
     */
    static final BigDecimal DEFAULT_PERCENT = new BigDecimal("0.1");

    private final RefreshingLookup<String, AdjustmentFactors> factors;

    public OurProfitFunction(RefreshingLookup<String, AdjustmentFactors> factors) {
        this.factors = factors;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        factors.open();
    }

    @Override
    public AdjustedOrderBook map(AdjustedOrderBook book) {
        Prices.applyPerLevelPercent(book,
                (pairId, exchangeId) -> {
                    AdjustmentFactors factor = factors.get(AdjustmentFactorsLoader.key(exchangeId, pairId));
                    return factor != null ? factor.getProfitPercent() : DEFAULT_PERCENT;
                },
                AdjustedLevel::setOurProfitPercent);
        return book;
    }

    @Override
    public void close() throws Exception {
        factors.close();
    }
}
