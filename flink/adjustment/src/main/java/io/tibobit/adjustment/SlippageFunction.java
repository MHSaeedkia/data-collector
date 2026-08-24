package io.tibobit.adjustment;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * Slippage — the slippage allowance applied to the book.
 *
 * <p><b>Deliberately a no-op right now.</b> The stage exists so the pipeline has the shape the
 * adjustments will occupy; how slippage is actually calculated is defined in a later step and is
 * not guessed at here. Until then it returns the book it was handed, unchanged.
 *
 * <p>Last in the chain, so it sees prices the two stages above have already moved. If it should instead work off the exchange's original prices, the chain order has to change — the stages compose, they are not independent.
 *
 * <p>Plain {@link MapFunction}, not a Rich one: nothing here needs {@code open()} yet. It becomes a
 * {@code RichMapFunction} when it needs a {@code RefreshingLookup} for its parameters, the way
 * jobs 3 and 4 read their rebase factors and precisions from postgres.
 */
public class SlippageFunction implements MapFunction<AggregatedOrderBook, AggregatedOrderBook> {

    @Override
    public AggregatedOrderBook map(AggregatedOrderBook book) {
        return book;
    }
}
