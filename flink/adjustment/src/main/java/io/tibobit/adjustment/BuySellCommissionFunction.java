package io.tibobit.adjustment;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * Buy/sell commission — the commission charged on a buy or a sell.
 *
 * <p><b>Deliberately a no-op right now.</b> The stage exists so the pipeline has the shape the
 * adjustments will occupy; how commission is actually calculated is defined in a later step and is
 * not guessed at here. Until then it returns the book it was handed, unchanged.
 *
 * <p>Which side is which is worth stating before the logic lands, because it decides the SIGN: {@code asks} is what a user buys at and {@code bids} is what a user sells at, so a commission normally moves the price against the user in opposite directions on the two sides. Confirm that before implementing rather than inferring it from this comment.
 *
 * <p>Plain {@link MapFunction}, not a Rich one: nothing here needs {@code open()} yet. It becomes a
 * {@code RichMapFunction} when it needs a {@code RefreshingLookup} for its parameters, the way
 * jobs 3 and 4 read their rebase factors and precisions from postgres.
 */
public class BuySellCommissionFunction implements MapFunction<AggregatedOrderBook, AggregatedOrderBook> {

    @Override
    public AggregatedOrderBook map(AggregatedOrderBook book) {
        return book;
    }
}
