package io.tibobit.merger;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PriceMergeFunctionTest {

    private final PriceMergeFunction merge = new PriceMergeFunction();

    private static AggregatedLevel level(int exchangeId, String price, String quantity, String sourceId) {
        return new AggregatedLevel(exchangeId, 0, sourceId, price, quantity);
    }

    private static AggregatedLevel simLevel(int exchangeId, String price, String quantity, String sourceId) {
        return new AggregatedLevel(exchangeId, 1, sourceId, price, quantity);
    }

    private static AggregatedOrderBook book(String side, AggregatedLevel... levels) {
        return new AggregatedOrderBook(1, side, "agg-id", 1750680000000L, List.of(levels));
    }

    @Test
    void sumsQuantitiesOfEqualPricesAcrossExchanges() {
        // The requirement, verbatim: 10 x 3 from ex1 and 10 x 4 from ex2 is one level of 7.
        MergedOrderBook result = merge.map(book("asks",
                level(1, "10", "3", "A"),
                level(2, "10", "4", "B")));

        assertThat(result.getLevels()).hasSize(1);
        MergedLevel merged = result.getLevels().get(0);
        assertThat(merged.getPrice()).isEqualTo("10");
        assertThat(merged.getQuantity()).isEqualTo("7");
        assertThat(merged.getExchangeIds()).containsExactly(1, 2);
        assertThat(merged.getSourceIds()).containsExactly("A", "B");
    }

    @Test
    void keepsDifferentPricesAsSeparateLevels() {
        MergedOrderBook result = merge.map(book("asks",
                level(1, "10", "3", "A"),
                level(2, "10", "4", "B"),
                level(1, "11", "5", "A")));

        assertThat(result.getLevels()).hasSize(2);
        assertThat(result.getLevels().get(0).getQuantity()).isEqualTo("7");
        assertThat(result.getLevels().get(1).getPrice()).isEqualTo("11");
        assertThat(result.getLevels().get(1).getQuantity()).isEqualTo("5");
        assertThat(result.getLevels().get(1).getExchangeIds()).containsExactly(1);
    }

    @Test
    void mergesPricesThatDifferOnlyInScale() {
        // Wire strings carry no formatting guarantee, so "10.00" and "10" are one price.
        MergedOrderBook result = merge.map(book("asks",
                level(1, "10.00", "3", "A"),
                level(2, "10", "4", "B")));

        assertThat(result.getLevels()).hasSize(1);
        assertThat(result.getLevels().get(0).getPrice()).isEqualTo("10");
        assertThat(result.getLevels().get(0).getQuantity()).isEqualTo("7");
    }

    @Test
    void sumsExactlyWithoutBinaryFloatingPointDrift() {
        MergedOrderBook result = merge.map(book("asks",
                level(1, "0.1", "0.1", "A"),
                level(2, "0.1", "0.2", "B")));

        assertThat(result.getLevels().get(0).getQuantity()).isEqualTo("0.3");
    }

    @Test
    void neverSumsSimulatedLiquidityIntoLive() {
        MergedOrderBook result = merge.map(book("asks",
                level(1, "10", "3", "A"),
                simLevel(2, "10", "4", "B")));

        assertThat(result.getLevels()).hasSize(2);
        // Live sorts first at an equal price.
        assertThat(result.getLevels().get(0).getSimulation()).isZero();
        assertThat(result.getLevels().get(0).getQuantity()).isEqualTo("3");
        assertThat(result.getLevels().get(1).getSimulation()).isEqualTo(1);
        assertThat(result.getLevels().get(1).getQuantity()).isEqualTo("4");
    }

    @Test
    void sortsAsksAscendingByPrice() {
        MergedOrderBook result = merge.map(book("asks",
                level(1, "9", "1", "A"),
                level(2, "11", "1", "B"),
                level(3, "10", "1", "C")));

        assertThat(result.getLevels()).extracting(MergedLevel::getPrice)
                .containsExactly("9", "10", "11");
    }

    @Test
    void sortsBidsDescendingByPrice() {
        MergedOrderBook result = merge.map(book("bids",
                level(1, "9", "1", "A"),
                level(2, "11", "1", "B"),
                level(3, "10", "1", "C")));

        assertThat(result.getLevels()).extracting(MergedLevel::getPrice)
                .containsExactly("11", "10", "9");
    }

    @Test
    void sortsNumericallyNotLexicographically() {
        // "9" > "10" as strings; the whole point of comparing as BigDecimal.
        MergedOrderBook result = merge.map(book("asks",
                level(1, "100", "1", "A"),
                level(2, "9", "1", "B")));

        assertThat(result.getLevels()).extracting(MergedLevel::getPrice)
                .containsExactly("9", "100");
    }

    @Test
    void carriesPairSideEventTimeAndRecordLineage() {
        MergedOrderBook result = merge.map(book("asks", level(1, "10", "3", "A")));

        assertThat(result.getPairId()).isEqualTo(1);
        assertThat(result.getSide()).isEqualTo("asks");
        assertThat(result.getEventTime()).isEqualTo(1750680000000L);
        // source_id is the one aggregated record this was merged from; id is freshly minted here.
        assertThat(result.getSourceId()).isEqualTo("agg-id");
        assertThat(result.getId()).isNotBlank().isNotEqualTo("agg-id");
    }

    @Test
    void mintsADistinctIdPerRecord() {
        AggregatedOrderBook input = book("asks", level(1, "10", "3", "A"));

        assertThat(merge.map(input).getId()).isNotEqualTo(merge.map(input).getId());
    }

    @Test
    void emitsAnEmptyBookWhenEveryExchangeHasDroppedOut() {
        // Job 6 emits a levels-less record when every exchange has been reset; it stays empty here
        // rather than becoming a gap in the merged topic.
        MergedOrderBook result = merge.map(book("asks"));

        assertThat(result.getLevels()).isEmpty();
        assertThat(result.getPairId()).isEqualTo(1);
        assertThat(result.getSourceId()).isEqualTo("agg-id");
    }

    @Test
    void toleratesANullLevelsArray() {
        AggregatedOrderBook input = new AggregatedOrderBook(1, "asks", "agg-id", 1750680000000L, null);

        assertThat(merge.map(input).getLevels()).isEmpty();
    }
}
