package io.tibobit.adjustment;

import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;

/**
 * Buy/sell commission — the commission charged on a buy or a sell.
 *
 * <p>Moves every level's price by {@link #PERCENT}% and records the rate it used on the book, so
 * the published event says what was charged rather than only what the result was. Direction — up on
 * {@code asks}, down on {@code bids} — is decided once, in {@link Prices#multiplier}.
 *
 * <p>First in the chain, so it is the only stage that sees the exchange's own price untouched. Everything after it compounds on this result.
 *
 * <p><b>The rate is a constant for now, and this field is where the database read replaces it</b>
 * (user, 2026-08-24: constants first, DB later). When it does, this becomes a
 * {@code RichMapFunction} holding a {@code RefreshingLookup}, the way jobs 3 and 4 read rebase
 * factors and precisions from postgres — and the lookup key is the open question:
 * a commission is charged by the EXCHANGE, and levels carry {@code exchange_id}, so this is plausibly per-exchange — which would make it a per-LEVEL rate rather than the per-record one the schema has today.
 */
public class BuySellCommissionFunction implements MapFunction<AdjustedOrderBook, AdjustedOrderBook> {

    /** Percent, not a fraction: 0.35 means 0.35%. */
    static final BigDecimal PERCENT = new BigDecimal("0.35");

    @Override
    public AdjustedOrderBook map(AdjustedOrderBook book) {
        Prices.applyPercent(book, PERCENT);
        book.setBuySellCommissionPercent(PERCENT.toPlainString());
        return book;
    }
}
