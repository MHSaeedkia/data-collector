package io.tibobit.adjustment;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.math.BigDecimal;

/**
 * Slippage — the slippage allowance applied to the book.
 *
 * <p>Adds a per-LEVEL percent of the price that level arrived with, and records the rate it used on
 * that level so the published event says what was charged rather than only what the result was.
 * The direction — up on {@code asks}, down on {@code bids} — and the fact that the amount is sized
 * off the ORIGINAL price both live in one place, {@link Prices#applyPerLevelPercent}.
 *
 * <p>Runs last, but is sized off the original price like the other two — so being last buys it
 * nothing and costs it nothing. The chain order is presentation, not arithmetic.
 *
 * <p><b>The rate is read from {@code exchange_markets.slippage_percent}</b> (2026-08-25), keyed per
 * {@code (exchange_id, pair_id)} via a {@link RefreshingLookup} — the user confirmed slippage
 * genuinely differs by exchange for the same market (e.g. 1% on one exchange, 2% on another for the
 * same pair), which settles the granularity question flagged when this stage first shipped: flat
 * percent per exchange+market, not depth-based, and not per-market-only.
 */
public class SlippageFunction extends RichMapFunction<AdjustedOrderBook, AdjustedOrderBook> {

    /**
     * Fallback when exchange_markets has no row for a level's (exchange, pair) — the pre-DB
     * constant. Same reasoning as {@link OurProfitFunction#DEFAULT_PERCENT}: no dead-letter output
     * to raise a missing row into, and 0% would under-charge rather than merely go stale.
     */
    static final BigDecimal DEFAULT_PERCENT = new BigDecimal("1");

    private final RefreshingLookup<String, AdjustmentFactors> factors;

    public SlippageFunction(RefreshingLookup<String, AdjustmentFactors> factors) {
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
                    return factor != null ? factor.getSlippagePercent() : DEFAULT_PERCENT;
                },
                AdjustedLevel::setSlippagePercent);
        return book;
    }

    @Override
    public void close() throws Exception {
        factors.close();
    }
}
