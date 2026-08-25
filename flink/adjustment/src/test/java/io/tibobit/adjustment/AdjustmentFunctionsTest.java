package io.tibobit.adjustment;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The three adjustment stages, and the arithmetic they compose into.
 *
 * <p>Expected prices are spelled out as literals rather than recomputed from the rates: a test that
 * repeats the implementation's formula agrees with the implementation by construction and proves
 * nothing. These were worked out independently and are what the job must publish.
 */
class AdjustmentFunctionsTest {

    /** 62650.00 chosen for readable arithmetic: x1.0035 lands exactly on 62869.275. */
    private static AdjustedOrderBook book(String side) {
        return AdjustedOrderBook.from(new AggregatedOrderBook(3, side, "agg-id-1", 1750680000000L, List.of(
                new AggregatedLevel(6, 1, "snapshot-A", "62650.00", "0.50000000"),
                new AggregatedLevel(8, 1, "snapshot-B", "1000", "1.25"))));
    }

    private static AdjustedOrderBook wholeChain(String side) {
        return new SlippageFunction()
                .map(new OurProfitFunction()
                        .map(new BuySellCommissionFunction().map(book(side))));
    }

    private static List<String> prices(AdjustedOrderBook book) {
        return book.getLevels().stream().map(AdjustedLevel::getPrice).toList();
    }

    // ---- direction: the convention the whole job rests on -----------------------

    @Test
    void commissionRaisesAskPricesAndLowersBidPrices() {
        // asks are what a user BUYS at, bids what they SELL at: a charge takes money from the same
        // side of the trade both times, so the two move in OPPOSITE directions.
        assertThat(prices(new BuySellCommissionFunction().map(book("asks"))))
                .containsExactly("62869.275", "1003.5");
        assertThat(prices(new BuySellCommissionFunction().map(book("bids"))))
                .containsExactly("62430.725", "996.5");
    }

    @Test
    void everyStageMovesAsksUpAndBidsDown() {
        BigDecimal start = new BigDecimal("1000");
        for (String side : List.of("asks", "bids")) {
            boolean up = side.equals("asks");
            assertThat(new BigDecimal(prices(new BuySellCommissionFunction().map(book(side))).get(1)))
                    .matches(p -> up == (p.compareTo(start) > 0), side + " commission");
            assertThat(new BigDecimal(prices(new OurProfitFunction().map(book(side))).get(1)))
                    .matches(p -> up == (p.compareTo(start) > 0), side + " profit");
            assertThat(new BigDecimal(prices(new SlippageFunction().map(book(side))).get(1)))
                    .matches(p -> up == (p.compareTo(start) > 0), side + " slippage");
        }
    }

    // ---- each stage on its own ---------------------------------------------------

    @Test
    void eachStageAppliesItsOwnRate() {
        assertThat(prices(new BuySellCommissionFunction().map(book("asks"))).get(1)).isEqualTo("1003.5");
        assertThat(prices(new OurProfitFunction().map(book("asks"))).get(1)).isEqualTo("1001");
        assertThat(prices(new SlippageFunction().map(book("asks"))).get(1)).isEqualTo("1010");
    }

    @Test
    void eachStageRecordsTheRateItApplied() {
        assertThat(new BuySellCommissionFunction().map(book("asks")).getBuySellCommissionPercent()).isEqualTo("0.35");
        assertThat(new OurProfitFunction().map(book("asks")).getOurProfitPercent()).isEqualTo("0.1");
        assertThat(new SlippageFunction().map(book("asks")).getSlippagePercent()).isEqualTo("1");
    }

    @Test
    void aStageLeavesTheOtherTwoRatesUntouched() {
        AdjustedOrderBook only = new BuySellCommissionFunction().map(book("asks"));

        assertThat(only.getBuySellCommissionPercent()).isEqualTo("0.35");
        assertThat(only.getOurProfitPercent()).isEqualTo("0");
        assertThat(only.getSlippagePercent()).isEqualTo("0");
    }

    // ---- the chain ---------------------------------------------------------------

    @Test
    void theThreeStagesAddRatherThanCompound() {
        // Every stage sizes its amount off the price the level ARRIVED with, so they SUM:
        // 1000 + 3.5 + 1 + 10 = 1014.5, NOT 1000 x 1.0035 x 1.001 x 1.01 = 1014.548535.
        assertThat(prices(wholeChain("asks"))).containsExactly("63558.425", "1014.5");
        assertThat(prices(wholeChain("bids"))).containsExactly("61741.575", "985.5");
    }

    /**
     * The direct consequence of sizing every amount off the original: no stage sees another's
     * output, so addition commutes and the chain order cannot change the numbers. It did under the
     * first (compounding) implementation, which is why this is worth pinning — if it ever fails,
     * someone has reintroduced a dependency between the stages.
     */
    @Test
    void reorderingTheStagesCannotChangeTheResult() {
        AdjustedOrderBook forward = new SlippageFunction()
                .map(new OurProfitFunction().map(new BuySellCommissionFunction().map(book("asks"))));
        AdjustedOrderBook reversed = new BuySellCommissionFunction()
                .map(new OurProfitFunction().map(new SlippageFunction().map(book("asks"))));

        assertThat(prices(forward)).isEqualTo(prices(reversed));
    }

    /** Each stage's contribution is a fixed money amount off the base, not a share of the running price. */
    @Test
    void eachStagesAmountIsSizedOffTheOriginalPrice() {
        AdjustedOrderBook afterFirst = new BuySellCommissionFunction().map(book("asks"));
        assertThat(afterFirst.getLevels().get(1).getPrice()).isEqualTo("1003.5");   // 1000 + 3.5

        AdjustedOrderBook afterSecond = new OurProfitFunction().map(afterFirst);
        // +1, which is 0.1% of the ORIGINAL 1000 — not 0.1% of 1003.5 (which would be 1.0035).
        assertThat(afterSecond.getLevels().get(1).getPrice()).isEqualTo("1004.5");
    }

    @Test
    void theChainReportsAllThreeRates() {
        AdjustedOrderBook out = wholeChain("asks");

        assertThat(out.getBuySellCommissionPercent()).isEqualTo("0.35");
        assertThat(out.getOurProfitPercent()).isEqualTo("0.1");
        assertThat(out.getSlippagePercent()).isEqualTo("1");
    }

    // ---- what must NOT change ----------------------------------------------------

    @Test
    void everythingExceptPriceIsCarriedThroughUntouched() {
        AdjustedOrderBook out = wholeChain("asks");

        assertThat(out.getPairId()).isEqualTo(3);
        assertThat(out.getSide()).isEqualTo("asks");
        assertThat(out.getId()).isEqualTo("agg-id-1");
        assertThat(out.getEventTime()).isEqualTo(1750680000000L);

        assertThat(out.getLevels()).hasSize(2);
        AdjustedLevel first = out.getLevels().get(0);
        assertThat(first.getExchangeId()).isEqualTo(6);
        assertThat(first.getSimulation()).isEqualTo(1);
        assertThat(first.getSourceId()).isEqualTo("snapshot-A");
        // Quantities are untouched: these stages move what a unit costs, not how many there are.
        assertThat(first.getQuantity()).isEqualTo("0.50000000");
        assertThat(out.getLevels().get(1).getQuantity()).isEqualTo("1.25");
    }

    /**
     * {@code AdjustedOrderBook.from} copies the levels, so the stages mutating in place cannot
     * write through to the record the deserializer produced. Sharing them would corrupt the input
     * of anything that later reads the aggregated book alongside this chain.
     */
    @Test
    void adjustingDoesNotMutateTheAggregatedInput() {
        AggregatedOrderBook input = new AggregatedOrderBook(1, "asks", "id", 1L,
                List.of(new AggregatedLevel(6, 0, "s", "1000", "1")));

        new BuySellCommissionFunction().map(AdjustedOrderBook.from(input));

        assertThat(input.getLevels().get(0).getPrice()).isEqualTo("1000");
    }

    /** An emptied book (job 6's output after a reset) still reports the rates and stays empty. */
    @Test
    void anEmptyBookSurvivesTheChain() {
        AdjustedOrderBook empty = AdjustedOrderBook.from(
                new AggregatedOrderBook(1, "bids", "agg-id-empty", 1L, List.of()));

        AdjustedOrderBook out = new SlippageFunction()
                .map(new OurProfitFunction().map(new BuySellCommissionFunction().map(empty)));

        assertThat(out.getLevels()).isEmpty();
        assertThat(out.getId()).isEqualTo("agg-id-empty");
        assertThat(out.getSlippagePercent()).isEqualTo("1");
    }

    /** Exact decimal arithmetic, never a double: 0.1% of 0.07 must not drift. */
    @Test
    void arithmeticIsExactNotFloatingPoint() {
        AdjustedOrderBook book = AdjustedOrderBook.from(new AggregatedOrderBook(1, "asks", "id", 1L,
                List.of(new AggregatedLevel(6, 0, "s", "0.07", "1"))));

        // 0.07 + (0.07 x 0.001) = 0.070070 exactly. Doubles give 0.07007000000000001.
        assertThat(prices(new OurProfitFunction().map(book))).containsExactly("0.07007");
    }

    /** The base must survive every stage, or the second and third would size off a moved price. */
    @Test
    void theBasePriceIsNeverMovedByAStage() {
        AdjustedOrderBook out = wholeChain("asks");

        assertThat(out.getLevels()).extracting(AdjustedLevel::getBasePrice)
                .containsExactly("62650.00", "1000");
    }
}
