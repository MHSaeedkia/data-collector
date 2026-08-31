package io.tibobit.normalizer.precision;

import io.tibobit.normalizer.lookup.RefreshingLookup;
import io.tibobit.normalizer.model.PriceLevel;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link PrecisionFunction} through Flink's operator harness so the real {@code open}/
 * {@code close} lifecycle runs, with a fake {@link RefreshingLookup.Loader} standing in for the
 * markets JDBC query.
 */
class PrecisionFunctionTest {

    private OneInputStreamOperatorTestHarness<RawOrderBookEvent, RawOrderBookEvent> harness;

    @AfterEach
    void closeHarness() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    // ---- helpers ----------------------------------------------------------------

    /** Opens a harness whose lookup holds exactly the given (pair → precision) rows. */
    private void openWith(Map<Integer, MarketPrecision> rows) throws Exception {
        RefreshingLookup<Integer, MarketPrecision> lookup =
                new RefreshingLookup<>(() -> rows, 60_000L);
        harness = new OneInputStreamOperatorTestHarness<>(
                new StreamMap<>(new PrecisionFunction(lookup)));
        harness.open();
    }

    private static RawOrderBookEvent event(int pair, List<PriceLevel> asks, List<PriceLevel> bids) {
        return new RawOrderBookEvent(8, pair, "snapshot", 1L, 0L, 1L, asks, bids);
    }

    private static List<PriceLevel> levels(String price, String quantity) {
        return List.of(new PriceLevel(price, quantity));
    }

    private RawOrderBookEvent process(RawOrderBookEvent event) throws Exception {
        harness.processElement(new StreamRecord<>(event));
        return harness.extractOutputValues().get(harness.extractOutputValues().size() - 1);
    }

    // ---- truncation -------------------------------------------------------------

    @Test
    @DisplayName("truncates DOWN — never rounds up, on either price or quantity")
    void truncatesDown() throws Exception {
        openWith(Map.of(1, new MarketPrecision(2, 8)));

        RawOrderBookEvent out = process(event(1,
                levels("62770.999", "0.1234567899"), null));

        assertThat(out.getAsks().get(0).getPrice()).isEqualTo("62770.99");
        assertThat(out.getAsks().get(0).getQuantity()).isEqualTo("0.12345678");
    }

    @Test
    @DisplayName("values already within precision are unchanged")
    void alreadyWithinPrecision() throws Exception {
        openWith(Map.of(1, new MarketPrecision(2, 8)));

        RawOrderBookEvent out = process(event(1, levels("62770.5", "0.002"), null));

        assertThat(out.getAsks().get(0).getPrice()).isEqualTo("62770.5");
        assertThat(out.getAsks().get(0).getQuantity()).isEqualTo("0.002");
    }

    @Test
    @DisplayName("precision 0 truncates to a whole number")
    void zeroPrecision() throws Exception {
        openWith(Map.of(1, new MarketPrecision(0, 0)));

        RawOrderBookEvent out = process(event(1, levels("62770.99", "5.99"), null));

        assertThat(out.getAsks().get(0).getPrice()).isEqualTo("62770");
        assertThat(out.getAsks().get(0).getQuantity()).isEqualTo("5");
    }

    @Test
    @DisplayName("truncation is exact on values wider than a double can hold")
    void exactOnWideValues() throws Exception {
        openWith(Map.of(1, new MarketPrecision(9, 9)));

        RawOrderBookEvent out = process(event(1,
                levels("12345678901234567890.1234567899", "0.9999999999"), null));

        assertThat(out.getAsks().get(0).getPrice()).isEqualTo("12345678901234567890.123456789");
        assertThat(out.getAsks().get(0).getQuantity()).isEqualTo("0.999999999");
    }

    @Test
    @DisplayName("price and quantity use their own precision columns")
    void separatePrecisions() throws Exception {
        openWith(Map.of(1, new MarketPrecision(1, 4)));

        RawOrderBookEvent out = process(event(1, levels("1.9999", "1.999999"), null));

        assertThat(out.getAsks().get(0).getPrice()).isEqualTo("1.9");
        assertThat(out.getAsks().get(0).getQuantity()).isEqualTo("1.9999");
    }

    @Test
    @DisplayName("precision is looked up per pair")
    void perPairLookup() throws Exception {
        openWith(Map.of(1, new MarketPrecision(2, 2), 2, new MarketPrecision(4, 4)));

        assertThat(process(event(1, levels("1.98765", "1.98765"), null))
                .getAsks().get(0).getPrice()).isEqualTo("1.98");
        assertThat(process(event(2, levels("1.98765", "1.98765"), null))
                .getAsks().get(0).getPrice()).isEqualTo("1.9876");
    }

    // ---- passthrough cases ------------------------------------------------------

    @Test
    @DisplayName("null precision column leaves that value untouched")
    void nullPrecisionPassesThrough() throws Exception {
        openWith(Map.of(1, new MarketPrecision(null, 2)));

        RawOrderBookEvent out = process(event(1, levels("62770.123456", "0.98765"), null));

        assertThat(out.getAsks().get(0).getPrice()).isEqualTo("62770.123456");
        assertThat(out.getAsks().get(0).getQuantity()).isEqualTo("0.98");
    }

    @Test
    @DisplayName("a pair with no markets row passes through untouched, not dead-lettered")
    void unknownPairPassesThrough() throws Exception {
        openWith(Map.of(1, new MarketPrecision(2, 2)));

        RawOrderBookEvent out = process(event(99, levels("62770.123456", "0.98765"), null));

        assertThat(out.getAsks().get(0).getPrice()).isEqualTo("62770.123456");
        assertThat(out.getAsks().get(0).getQuantity()).isEqualTo("0.98765");
    }

    @Test
    @DisplayName("a quantity that is genuinely zero on the wire survives as a delete")
    void zeroQuantityPassesThrough() throws Exception {
        openWith(Map.of(1, new MarketPrecision(2, 8)));

        RawOrderBookEvent out = process(event(1, levels("62770.5", "0"), null));

        assertThat(out.getAsks()).hasSize(1);
        assertThat(out.getAsks().get(0).getQuantity()).isEqualTo("0");
    }

    // ---- truncate-to-zero (the resolved design flag) -----------------------------

    @Test
    @DisplayName("a nonzero quantity truncating to zero is emitted as 0, keeping its level")
    void dustBecomesZero() throws Exception {
        openWith(Map.of(1, new MarketPrecision(2, 8)));

        RawOrderBookEvent out = process(event(1,
                List.of(new PriceLevel("62770.5", "0.000000001"),
                        new PriceLevel("62771.5", "0.5")),
                null));

        assertThat(out.getAsks()).hasSize(2);
        assertThat(out.getAsks().get(0).getPrice()).isEqualTo("62770.5");
        assertThat(out.getAsks().get(0).getQuantity()).isEqualTo("0");
        assertThat(out.getAsks().get(1).getQuantity()).isEqualTo("0.5");
    }

    @Test
    @DisplayName("every level surviving as dust keeps the side at full length")
    void allDustKeepsEveryLevel() throws Exception {
        openWith(Map.of(1, new MarketPrecision(2, 8)));

        RawOrderBookEvent out = process(event(1, levels("62770.5", "0.000000001"), null));

        assertThat(out.getAsks()).hasSize(1);
        assertThat(out.getAsks().get(0).getQuantity()).isEqualTo("0");
    }

    @Test
    @DisplayName("with a null quantity precision nothing can truncate to dust")
    void nullQuantityPrecisionKeepsDust() throws Exception {
        openWith(Map.of(1, new MarketPrecision(2, null)));

        RawOrderBookEvent out = process(event(1, levels("62770.5", "0.000000001"), null));

        assertThat(out.getAsks()).hasSize(1);
        assertThat(out.getAsks().get(0).getQuantity()).isEqualTo("0.000000001");
    }

    // ---- price collision merge (user decision 2026-07-20) ------------------------

    @Test
    @DisplayName("wire prices colliding into one book price merge into one level, quantities summed")
    void collidingPricesMerge() throws Exception {
        openWith(Map.of(1, new MarketPrecision(2, 8)));

        RawOrderBookEvent out = process(event(1,
                List.of(new PriceLevel("1.234", "2"), new PriceLevel("1.235", "3")),
                null));

        assertThat(out.getAsks()).hasSize(1);
        assertThat(out.getAsks().get(0).getPrice()).isEqualTo("1.23");
        assertThat(out.getAsks().get(0).getQuantity()).isEqualTo("5");
    }

    @Test
    @DisplayName("merged levels keep first-appearance order and non-colliding levels are untouched")
    void mergeKeepsOrder() throws Exception {
        openWith(Map.of(1, new MarketPrecision(2, 8)));

        RawOrderBookEvent out = process(event(1,
                List.of(new PriceLevel("1.234", "2"),
                        new PriceLevel("2.500", "1"),
                        new PriceLevel("1.239", "3")),
                null));

        assertThat(out.getAsks()).extracting(PriceLevel::getPrice).containsExactly("1.23", "2.5");
        assertThat(out.getAsks()).extracting(PriceLevel::getQuantity).containsExactly("5", "1");
    }

    @Test
    @DisplayName("quantities are summed RAW and the sum truncated once, so dust can add up to a real level")
    void sumsBeforeTruncating() throws Exception {
        openWith(Map.of(1, new MarketPrecision(2, 8)));

        RawOrderBookEvent out = process(event(1,
                List.of(new PriceLevel("1.234", "0.000000006"),
                        new PriceLevel("1.235", "0.000000006")),
                null));

        assertThat(out.getAsks()).hasSize(1);
        assertThat(out.getAsks().get(0).getQuantity()).isEqualTo("0.00000001");
    }

    @Test
    @DisplayName("colliding levels that are all dust still merge down to a single delete")
    void allDustCollisionBecomesOneDelete() throws Exception {
        openWith(Map.of(1, new MarketPrecision(2, 8)));

        RawOrderBookEvent out = process(event(1,
                List.of(new PriceLevel("1.234", "0.000000001"),
                        new PriceLevel("1.235", "0.000000002")),
                null));

        assertThat(out.getAsks()).hasSize(1);
        assertThat(out.getAsks().get(0).getQuantity()).isEqualTo("0");
    }

    @Test
    @DisplayName("both sides merge independently")
    void bothSidesMerge() throws Exception {
        openWith(Map.of(1, new MarketPrecision(2, 8)));

        RawOrderBookEvent out = process(event(1,
                List.of(new PriceLevel("1.234", "2"), new PriceLevel("1.235", "3")),
                List.of(new PriceLevel("9.991", "1"), new PriceLevel("9.999", "4"))));

        assertThat(out.getAsks()).hasSize(1);
        assertThat(out.getAsks().get(0).getQuantity()).isEqualTo("5");
        assertThat(out.getBids()).hasSize(1);
        assertThat(out.getBids().get(0).getQuantity()).isEqualTo("5");
    }

    @Test
    @DisplayName("with no price precision configured only exact wire duplicates merge")
    void nullPricePrecisionMergesOnlyExactDuplicates() throws Exception {
        openWith(Map.of(1, new MarketPrecision(null, 8)));

        RawOrderBookEvent out = process(event(1,
                List.of(new PriceLevel("1.234", "2"),
                        new PriceLevel("1.235", "3"),
                        new PriceLevel("1.234", "1")),
                null));

        assertThat(out.getAsks()).extracting(PriceLevel::getPrice).containsExactly("1.234", "1.235");
        assertThat(out.getAsks()).extracting(PriceLevel::getQuantity).containsExactly("3", "3");
    }

    // ---- side/shape handling ----------------------------------------------------

    @Test
    @DisplayName("a null side stays null and an empty side stays empty")
    void nullSideStaysNull() throws Exception {
        openWith(Map.of(1, new MarketPrecision(2, 8)));

        RawOrderBookEvent out = process(event(1, null, List.of()));

        assertThat(out.getAsks()).isNull();
        assertThat(out.getBids()).isNotNull().isEmpty();
    }

    @Test
    @DisplayName("every level of both sides is truncated")
    void bothSidesAllLevels() throws Exception {
        openWith(Map.of(1, new MarketPrecision(2, 2)));

        RawOrderBookEvent out = process(event(1,
                List.of(new PriceLevel("1.999", "1.999"), new PriceLevel("2.999", "2.999")),
                List.of(new PriceLevel("3.999", "3.999"))));

        assertThat(out.getAsks()).extracting(PriceLevel::getPrice).containsExactly("1.99", "2.99");
        assertThat(out.getBids()).extracting(PriceLevel::getPrice).containsExactly("3.99");
    }

    @Test
    @DisplayName("pipeline timings are stamped around the truncation")
    void stampsTimings() throws Exception {
        openWith(Map.of(1, new MarketPrecision(2, 8)));

        RawOrderBookEvent out = process(event(1, levels("62770.5", "0.5"), null));

        assertThat(out.getPipelineTimings().getPrecisionIn()).isNotNull();
        assertThat(out.getPipelineTimings().getPrecisionOut())
                .isNotNull()
                .isGreaterThanOrEqualTo(out.getPipelineTimings().getPrecisionIn());
    }
}
