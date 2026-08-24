package io.tibobit.adjustment;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * Our profit — our own margin, applied on top of whatever the exchange quoted.
 *
 * <p><b>Deliberately a no-op right now.</b> The stage exists so the pipeline has the shape the
 * adjustments will occupy; how the margin is actually calculated is defined in a later step and is
 * not guessed at here. Until then it returns the book it was handed, unchanged.
 *
 * <p>Distinct from the commission stage above: that one models a fee the EXCHANGE charges and is therefore plausibly per-exchange, whereas this is ours and is plausibly per-market or global. They are separate stages for that reason, not just for tidiness.
 *
 * <p>Plain {@link MapFunction}, not a Rich one: nothing here needs {@code open()} yet. It becomes a
 * {@code RichMapFunction} when it needs a {@code RefreshingLookup} for its parameters, the way
 * jobs 3 and 4 read their rebase factors and precisions from postgres.
 */
public class OurProfitFunction implements MapFunction<AggregatedOrderBook, AggregatedOrderBook> {

    @Override
    public AggregatedOrderBook map(AggregatedOrderBook book) {
        return book;
    }
}
