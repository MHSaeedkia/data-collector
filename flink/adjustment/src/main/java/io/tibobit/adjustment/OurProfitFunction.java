package io.tibobit.adjustment;

import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;

/**
 * Our profit — our own margin, applied on top of whatever the exchange quoted.
 *
 * <p>Moves every level's price by {@link #PERCENT}% and records the rate it used on the book, so
 * the published event says what was charged rather than only what the result was. Direction — up on
 * {@code asks}, down on {@code bids} — is decided once, in {@link Prices#multiplier}.
 *
 * <p>Applied to the commission-adjusted price, not to the exchange's original — the stages compound in the order the job chains them.
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
