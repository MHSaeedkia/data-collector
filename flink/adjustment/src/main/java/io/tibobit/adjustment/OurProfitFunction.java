package io.tibobit.adjustment;

import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;

/**
 * Our profit — our own margin, applied on top of whatever the exchange quoted.
 *
 * <p>Adds {@link #PERCENT}% <b>of the price the level arrived with</b> to every level, and records
 * the rate it used on the book so the published event says what was charged rather than only what
 * the result was. The direction — up on {@code asks}, down on {@code bids} — and the fact that the
 * amount is sized off the ORIGINAL price both live in one place, {@link Prices#applyPercent}.
 *
 * <p>Sized off the exchange's ORIGINAL price, not off the commission-adjusted one, so this margin
 * is independent of whatever the commission stage charged.
 *
 * <p><b>The rate is a constant for now, and this field is where the database read replaces it</b>
 * (user, 2026-08-24: constants first, DB later). When it does, this becomes a
 * {@code RichMapFunction} holding a {@code RefreshingLookup}, the way jobs 3 and 4 read rebase
 * factors and precisions from postgres — and the lookup key is the open question:
 * this margin is OURS, not an exchange's, so it is plausibly per-market or global and can stay a per-record rate.
 */
public class OurProfitFunction implements MapFunction<AdjustedOrderBook, AdjustedOrderBook> {

    /** Percent, not a fraction: 0.1 means 0.1%. */
    static final BigDecimal PERCENT = new BigDecimal("0.1");

    @Override
    public AdjustedOrderBook map(AdjustedOrderBook book) {
        Prices.applyPercent(book, PERCENT);
        book.setOurProfitPercent(PERCENT.toPlainString());
        return book;
    }
}
