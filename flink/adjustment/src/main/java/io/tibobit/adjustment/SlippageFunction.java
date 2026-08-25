package io.tibobit.adjustment;

import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;

/**
 * Slippage — the slippage allowance applied to the book.
 *
 * <p>Adds {@link #PERCENT}% <b>of the price the level arrived with</b> to every level, and records
 * the rate it used on the book so the published event says what was charged rather than only what
 * the result was. The direction — up on {@code asks}, down on {@code bids} — and the fact that the
 * amount is sized off the ORIGINAL price both live in one place, {@link Prices#applyPercent}.
 *
 * <p>Runs last, but is sized off the original price like the other two — so being last buys it
 * nothing and costs it nothing. The chain order is presentation, not arithmetic.
 *
 * <p><b>The rate is a constant for now, and this field is where the database read replaces it</b>
 * (user, 2026-08-24: constants first, DB later). When it does, this becomes a
 * {@code RichMapFunction} holding a {@code RefreshingLookup}, the way jobs 3 and 4 read rebase
 * factors and precisions from postgres — and the lookup key is the open question:
 * unsettled — slippage is usually a function of DEPTH, so a flat percent may not survive contact with the real requirement.
 */
public class SlippageFunction implements MapFunction<AdjustedOrderBook, AdjustedOrderBook> {

    /** Percent, not a fraction: 1 means 1%. */
    static final BigDecimal PERCENT = new BigDecimal("1");

    @Override
    public AdjustedOrderBook map(AdjustedOrderBook book) {
        Prices.applyPercent(book, PERCENT);
        book.setSlippagePercent(PERCENT.toPlainString());
        return book;
    }
}
