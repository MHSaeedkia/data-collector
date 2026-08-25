package io.tibobit.adjustment;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The three adjustment stages, and the arithmetic they compose into.
 *
 * <p>Expected prices are spelled out as literals rather than recomputed from the rates: a test that
 * repeats the implementation's formula agrees with the implementation by construction and proves
 * nothing. These were worked out independently and are what the job must publish.
 *
 * <p>{@link #profitFunction}/{@link #slippageFunction} open with an EMPTY lookup unless a test
 * needs specific rows — an empty map means every level falls back to {@code DEFAULT_PERCENT}
 * (0.1%/1%), which is deliberately identical to the pre-DB hardcoded constants, so every test that
 * predates the DB read keeps its original literal expectations unchanged.
 */
class AdjustmentFunctionsTest {

    /** 62650.00 chosen for readable arithmetic: x1.0035 lands exactly on 62869.275. */
    private static AdjustedOrderBook book(String side) {
        return AdjustedOrderBook.from(new AggregatedOrderBook(3, side, "agg-id-1", 1750680000000L, List.of(
                new AggregatedLevel(6, 1, "snapshot-A", "62650.00", "0.50000000"),
                new AggregatedLevel(8, 1, "snapshot-B", "1000", "1.25"))));
    }

    private static OurProfitFunction profitFunction(Map<String, AdjustmentFactors> rows) throws Exception {
        OurProfitFunction fn = new OurProfitFunction(new RefreshingLookup<>(() -> rows, 60_000L));
        fn.open(null);
        return fn;
    }

    private static SlippageFunction slippageFunction(Map<String, AdjustmentFactors> rows) throws Exception {
        SlippageFunction fn = new SlippageFunction(new RefreshingLookup<>(() -> rows, 60_000L));
        fn.open(null);
        return fn;
    }

    private static AdjustedOrderBook wholeChain(String side) throws Exception {
        return slippageFunction(Map.of())
                .map(profitFunction(Map.of())
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
    void everyStageMovesAsksUpAndBidsDown() throws Exception {
        BigDecimal start = new BigDecimal("1000");
        for (String side : List.of("asks", "bids")) {
            boolean up = side.equals("asks");
            assertThat(new BigDecimal(prices(new BuySellCommissionFunction().map(book(side))).get(1)))
                    .matches(p -> up == (p.compareTo(start) > 0), side + " commission");
            assertThat(new BigDecimal(prices(profitFunction(Map.of()).map(book(side))).get(1)))
                    .matches(p -> up == (p.compareTo(start) > 0), side + " profit");
            assertThat(new BigDecimal(prices(slippageFunction(Map.of()).map(book(side))).get(1)))
                    .matches(p -> up == (p.compareTo(start) > 0), side + " slippage");
        }
    }

    // ---- each stage on its own ---------------------------------------------------

    @Test
    void eachStageAppliesItsOwnRate() throws Exception {
        assertThat(prices(new BuySellCommissionFunction().map(book("asks"))).get(1)).isEqualTo("1003.5");
        assertThat(prices(profitFunction(Map.of()).map(book("asks"))).get(1)).isEqualTo("1001");
        assertThat(prices(slippageFunction(Map.of()).map(book("asks"))).get(1)).isEqualTo("1010");
    }

    @Test
    void eachStageRecordsTheRateItApplied() throws Exception {
        assertThat(new BuySellCommissionFunction().map(book("asks")).getBuySellCommissionPercent()).isEqualTo("0.35");
        assertThat(profitFunction(Map.of()).map(book("asks")).getLevels().get(1).getOurProfitPercent()).isEqualTo("0.1");
        assertThat(slippageFunction(Map.of()).map(book("asks")).getLevels().get(1).getSlippagePercent()).isEqualTo("1");
    }

    @Test
    void aStageLeavesTheOtherTwoRatesUntouched() {
        AdjustedOrderBook only = new BuySellCommissionFunction().map(book("asks"));

        assertThat(only.getBuySellCommissionPercent()).isEqualTo("0.35");
        // profit/slippage default "0" on every level until their own stage runs.
        assertThat(only.getLevels().get(1).getOurProfitPercent()).isEqualTo("0");
        assertThat(only.getLevels().get(1).getSlippagePercent()).isEqualTo("0");
    }

    // ---- the chain ---------------------------------------------------------------

    @Test
    void theThreeStagesAddRatherThanCompound() throws Exception {
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
    void reorderingTheStagesCannotChangeTheResult() throws Exception {
        AdjustedOrderBook forward = slippageFunction(Map.of())
                .map(profitFunction(Map.of()).map(new BuySellCommissionFunction().map(book("asks"))));
        AdjustedOrderBook reversed = new BuySellCommissionFunction()
                .map(profitFunction(Map.of()).map(slippageFunction(Map.of()).map(book("asks"))));

        assertThat(prices(forward)).isEqualTo(prices(reversed));
    }

    /** Each stage's contribution is a fixed money amount off the base, not a share of the running price. */
    @Test
    void eachStagesAmountIsSizedOffTheOriginalPrice() throws Exception {
        AdjustedOrderBook afterFirst = new BuySellCommissionFunction().map(book("asks"));
        assertThat(afterFirst.getLevels().get(1).getPrice()).isEqualTo("1003.5");   // 1000 + 3.5

        AdjustedOrderBook afterSecond = profitFunction(Map.of()).map(afterFirst);
        // +1, which is 0.1% of the ORIGINAL 1000 — not 0.1% of 1003.5 (which would be 1.0035).
        assertThat(afterSecond.getLevels().get(1).getPrice()).isEqualTo("1004.5");
    }

    @Test
    void theChainReportsAllThreeRates() throws Exception {
        AdjustedOrderBook out = wholeChain("asks");

        assertThat(out.getBuySellCommissionPercent()).isEqualTo("0.35");
        assertThat(out.getLevels().get(1).getOurProfitPercent()).isEqualTo("0.1");
        assertThat(out.getLevels().get(1).getSlippagePercent()).isEqualTo("1");
    }

    // ---- profit/slippage looked up per (exchange, pair) — the whole point of the DB move --------

    /**
     * The scenario the redesign exists for: one book, two exchanges, two different DB rows. A flat
     * record-level rate could never have produced this — exchange 6 and exchange 8 must each get
     * their OWN rate even though both levels are in the same p3-asks book.
     */
    @Test
    void differentExchangesInTheSameBookGetDifferentRates() throws Exception {
        Map<String, AdjustmentFactors> rows = Map.of(
                "6|3", new AdjustmentFactors(new BigDecimal("1"), new BigDecimal("2")),
                "8|3", new AdjustmentFactors(new BigDecimal("3"), new BigDecimal("4")));

        AdjustedOrderBook out = slippageFunction(rows).map(profitFunction(rows).map(book("asks")));

        assertThat(out.getLevels().get(0).getOurProfitPercent()).isEqualTo("1");
        assertThat(out.getLevels().get(0).getSlippagePercent()).isEqualTo("2");
        assertThat(out.getLevels().get(1).getOurProfitPercent()).isEqualTo("3");
        assertThat(out.getLevels().get(1).getSlippagePercent()).isEqualTo("4");
        // 62650.00 x (1 + 0.01 + 0.02) = 62650 + 626.5 + 1253 = 64529.5
        assertThat(out.getLevels().get(0).getPrice()).isEqualTo("64529.5");
    }

    /** A level whose (exchange, pair) has no exchange_markets row falls back to the old constant. */
    @Test
    void aMissingRowFallsBackToTheDefaultPercent() throws Exception {
        Map<String, AdjustmentFactors> rows = Map.of(
                "6|3", new AdjustmentFactors(new BigDecimal("5"), new BigDecimal("9")));
        // No "8|3" row — that level must fall back to OurProfitFunction.DEFAULT_PERCENT (0.1) /
        // SlippageFunction.DEFAULT_PERCENT (1), not silently charge 0%.

        AdjustedOrderBook out = slippageFunction(rows).map(profitFunction(rows).map(book("asks")));

        assertThat(out.getLevels().get(0).getOurProfitPercent()).isEqualTo("5");
        assertThat(out.getLevels().get(1).getOurProfitPercent()).isEqualTo(
                OurProfitFunction.DEFAULT_PERCENT.toPlainString());
        assertThat(out.getLevels().get(1).getSlippagePercent()).isEqualTo(
                SlippageFunction.DEFAULT_PERCENT.toPlainString());
    }

    // ---- what must NOT change ----------------------------------------------------

    @Test
    void everythingExceptPriceIsCarriedThroughUntouched() throws Exception {
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

    /** An emptied book (job 6's output after a reset) still reports the commission rate and stays empty. */
    @Test
    void anEmptyBookSurvivesTheChain() throws Exception {
        AdjustedOrderBook empty = AdjustedOrderBook.from(
                new AggregatedOrderBook(1, "bids", "agg-id-empty", 1L, List.of()));

        AdjustedOrderBook out = slippageFunction(Map.of())
                .map(profitFunction(Map.of()).map(new BuySellCommissionFunction().map(empty)));

        assertThat(out.getLevels()).isEmpty();
        assertThat(out.getId()).isEqualTo("agg-id-empty");
        assertThat(out.getBuySellCommissionPercent()).isEqualTo("0.35");
    }

    /** Exact decimal arithmetic, never a double: 0.1% of 0.07 must not drift. */
    @Test
    void arithmeticIsExactNotFloatingPoint() throws Exception {
        AdjustedOrderBook book = AdjustedOrderBook.from(new AggregatedOrderBook(1, "asks", "id", 1L,
                List.of(new AggregatedLevel(6, 0, "s", "0.07", "1"))));

        // 0.07 + (0.07 x 0.001) = 0.070070 exactly. Doubles give 0.07007000000000001.
        assertThat(prices(profitFunction(Map.of()).map(book))).containsExactly("0.07007");
    }

    /** The base must survive every stage, or the second and third would size off a moved price. */
    @Test
    void theBasePriceIsNeverMovedByAStage() throws Exception {
        AdjustedOrderBook out = wholeChain("asks");

        assertThat(out.getLevels()).extracting(AdjustedLevel::getBasePrice)
                .containsExactly("62650.00", "1000");
    }
}
