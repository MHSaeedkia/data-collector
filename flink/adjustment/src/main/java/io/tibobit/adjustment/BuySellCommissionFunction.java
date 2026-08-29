package io.tibobit.adjustment;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.math.BigDecimal;

/**
 * Buy/sell commission — the commission charged on a buy or a sell.
 *
 * <p>
 * Adds a per-LEVEL percent of the price that level arrived with, and records
 * the rate it used on that level so the published event says what was charged
 * rather than only what the result was. The direction — up on {@code asks},
 * down on {@code bids} — and the fact that the amount is sized off the ORIGINAL
 * price both live in one place, {@link Prices#applyPerLevelPercent}.
 *
 * <p>
 * First in the chain, but that no longer means anything arithmetically: it is
 * sized off the original price exactly like the other two, so the three ADD
 * rather than compounding.
 *
 * <p>
 * <b>The rate is read from
 * {@code exchange_markets.buy_sell_commission_percent}</b>
 * (2026-08-25), keyed per {@code (exchange_id, pair_id)} via a
 * {@link RefreshingLookup} — same move already made for profit/slippage, and
 * for the identical reason: levels carry {@code exchange_id}, and one book
 * unions levels from multiple exchanges, so a commission that varies by
 * exchange can only be represented per-level.
 */
public class BuySellCommissionFunction extends RichMapFunction<AdjustedOrderBook, AdjustedOrderBook> {

    /**
     * Fallback when exchange_markets has no row for a level's (exchange, pair)
     * — the pre-DB constant. Same reasoning as
     * {@link OurProfitFunction#DEFAULT_PERCENT}: no dead-letter output to raise
     * a missing row into, and 0% would under-charge rather than merely go
     * stale.
     */
    static final BigDecimal DEFAULT_PERCENT = new BigDecimal("0.35");

    private final RefreshingLookup<String, AdjustmentFactors> factors;

    public BuySellCommissionFunction(RefreshingLookup<String, AdjustmentFactors> factors) {
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
                    return factor != null ? factor.getCommissionPercent() : DEFAULT_PERCENT;
                },
                AdjustedLevel::setBuySellCommissionPercent);
        return book;
    }

    @Override
    public void close() throws Exception {
        factors.close();
    }
}
