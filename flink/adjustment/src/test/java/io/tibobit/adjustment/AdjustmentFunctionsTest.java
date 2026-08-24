package io.tibobit.adjustment;

import org.apache.flink.api.common.functions.MapFunction;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The three adjustment stages are placeholders: each returns its input unchanged, and the chain of
 * all three does too.
 *
 * <p>These tests are the written-down contract of that stage of the work, and they are **meant to
 * fail** the moment real logic lands — that failure is the reminder to state the new contract here
 * rather than let a stage change prices with nothing describing what it does. Do not delete them
 * then; rewrite them.
 *
 * <p>Fields are asserted individually rather than by reference, because a future implementation
 * that mutates the book in place would still satisfy {@code assertThat(out).isSameAs(in)} while
 * having changed every price.
 */
class AdjustmentFunctionsTest {

    private static AggregatedOrderBook book() {
        return new AggregatedOrderBook(3, "asks", "agg-id-1", 1750680000000L, List.of(
                new AggregatedLevel(6, 1, "snapshot-A", "62650.00", "0.50000000"),
                new AggregatedLevel(8, 1, "snapshot-B", "62651.25", "1.25")));
    }

    private static void assertUnchanged(AggregatedOrderBook out) {
        assertThat(out.getPairId()).isEqualTo(3);
        assertThat(out.getSide()).isEqualTo("asks");
        assertThat(out.getId()).isEqualTo("agg-id-1");
        assertThat(out.getEventTime()).isEqualTo(1750680000000L);
        assertThat(out.getLevels()).hasSize(2);

        AggregatedLevel first = out.getLevels().get(0);
        assertThat(first.getExchangeId()).isEqualTo(6);
        assertThat(first.getSimulation()).isEqualTo(1);
        assertThat(first.getSourceId()).isEqualTo("snapshot-A");
        assertThat(first.getPrice()).isEqualTo("62650.00");
        assertThat(first.getQuantity()).isEqualTo("0.50000000");

        AggregatedLevel second = out.getLevels().get(1);
        assertThat(second.getExchangeId()).isEqualTo(8);
        assertThat(second.getPrice()).isEqualTo("62651.25");
        assertThat(second.getQuantity()).isEqualTo("1.25");
    }

    @Test
    void buySellCommissionPassesEveryFieldThroughUnchanged() throws Exception {
        assertUnchanged(new BuySellCommissionFunction().map(book()));
    }

    @Test
    void ourProfitPassesEveryFieldThroughUnchanged() throws Exception {
        assertUnchanged(new OurProfitFunction().map(book()));
    }

    @Test
    void slippagePassesEveryFieldThroughUnchanged() throws Exception {
        assertUnchanged(new SlippageFunction().map(book()));
    }

    /**
     * The three composed, in the order {@code AdjustmentJob} chains them. Worth its own test even
     * while every stage is a no-op: once they are not, this is where "the whole chain still
     * produces a well-formed book" is checked, and the order it runs in is part of the contract.
     */
    @Test
    void theChainOfAllThreePassesTheBookThroughUnchanged() throws Exception {
        List<MapFunction<AggregatedOrderBook, AggregatedOrderBook>> chain = List.of(
                new BuySellCommissionFunction(),
                new OurProfitFunction(),
                new SlippageFunction());

        AggregatedOrderBook out = book();
        for (MapFunction<AggregatedOrderBook, AggregatedOrderBook> stage : chain) {
            out = stage.map(out);
        }

        assertUnchanged(out);
    }

    /** An emptied book (job 6's output after a reset) must survive the chain too. */
    @Test
    void anEmptyBookSurvivesTheChain() throws Exception {
        AggregatedOrderBook empty = new AggregatedOrderBook(1, "bids", "agg-id-empty", 1L, List.of());

        AggregatedOrderBook out = new SlippageFunction()
                .map(new OurProfitFunction().map(new BuySellCommissionFunction().map(empty)));

        assertThat(out.getLevels()).isEmpty();
        assertThat(out.getId()).isEqualTo("agg-id-empty");
        assertThat(out.getSide()).isEqualTo("bids");
    }
}
