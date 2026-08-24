package io.tibobit.adjustment;

import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;

/**
 * Slippage — the slippage allowance applied to the book.
 *
 * <p>Moves every level's price by {@link #PERCENT}% and records the rate it used on the book, so
 * the published event says what was charged rather than only what the result was. Direction — up on
 * {@code asks}, down on {@code bids} — is decided once, in {@link Prices#multiplier}.
 *
 * <p>Last, so it compounds on a price the other two stages have already moved. If it should instead work off the exchange's original price, the chain has to change shape rather than its arithmetic.
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
