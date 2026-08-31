package io.tibobit.normalizer.rebase;

import io.tibobit.normalizer.lookup.RefreshingLookup;
import io.tibobit.normalizer.model.PriceLevel;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import io.tibobit.normalizer.model.RejectedOrderBookEvent;

import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link RebaseFunction} through Flink's operator harness so the real {@code open}/{@code
 * close} lifecycle and side-output plumbing run, with a fake {@link RefreshingLookup.Loader}
 * standing in for the exchange_markets JDBC query.
 */
class RebaseFunctionTest {

    private OneInputStreamOperatorTestHarness<RawOrderBookEvent, RawOrderBookEvent> harness;

    @AfterEach
    void closeHarness() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    // ---- helpers ----------------------------------------------------------------

    /** Opens a harness whose lookup holds exactly the given (exchange|pair → factors) rows. */
    private void openWith(Map<String, RebaseFactors> rows) throws Exception {
        RefreshingLookup<String, RebaseFactors> lookup =
                new RefreshingLookup<>(() -> rows, 60_000L);
        harness = new OneInputStreamOperatorTestHarness<>(
                new ProcessOperator<>(new RebaseFunction(lookup)));
        harness.open();
    }

    private static RawOrderBookEvent event(int ex, int pair,
                                           List<PriceLevel> asks, List<PriceLevel> bids) {
        return new RawOrderBookEvent(ex, pair, "snapshot", 1L, 0L, 1L, asks, bids);
    }

    private static List<PriceLevel> levels(String price, String quantity) {
        return List.of(new PriceLevel(price, quantity));
    }

    private List<RawOrderBookEvent> output() {
        return harness.extractOutputValues();
    }

    private List<RejectedOrderBookEvent> rejects() {
        ConcurrentLinkedQueue<StreamRecord<RejectedOrderBookEvent>> queue =
                harness.getSideOutput(RebaseFunction.REJECTED);
        return queue == null ? List.of()
                : queue.stream().map(StreamRecord::getValue).collect(Collectors.toList());
    }

    // ---- rebase arithmetic ------------------------------------------------------

    @Test
    @DisplayName("rebase 0 is identity on the value")
    void rebaseZeroIsIdentity() throws Exception {
        openWith(Map.of("1|1", new RebaseFactors(0, 0)));

        harness.processElement(new StreamRecord<>(
                event(1, 1, levels("62770.5", "2.25"), levels("62769", "0.5"))));

        PriceLevel ask = output().get(0).getAsks().get(0);
        assertThat(ask.getPrice()).isEqualTo("62770.5");
        assertThat(ask.getQuantity()).isEqualTo("2.25");
    }

    @Test
    @DisplayName("positive exponent shifts the decimal point right, negative shifts it left")
    void positiveAndNegativeExponents() throws Exception {
        openWith(Map.of("1|1", new RebaseFactors(2, -3)));

        harness.processElement(new StreamRecord<>(
                event(1, 1, levels("1234.5", "2500"), null)));

        PriceLevel ask = output().get(0).getAsks().get(0);
        assertThat(ask.getPrice()).isEqualTo("123450");
        assertThat(ask.getQuantity()).isEqualTo("2.5");
    }

    @Test
    @DisplayName("rebase is exact — no binary-floating-point drift on values a double would mangle")
    void rebaseIsExact() throws Exception {
        openWith(Map.of("1|1", new RebaseFactors(-8, -8)));

        // 0.1 and friends have no exact double representation; scaleByPowerOfTen must be exact.
        harness.processElement(new StreamRecord<>(
                event(1, 1, levels("12345678901234567890.1", "1"), null)));

        PriceLevel ask = output().get(0).getAsks().get(0);
        assertThat(ask.getPrice()).isEqualTo("123456789012.345678901");
        assertThat(ask.getQuantity()).isEqualTo("0.00000001");
    }

    @Test
    @DisplayName("price and quantity use their own exponents")
    void priceAndQuantityUseSeparateExponents() throws Exception {
        openWith(Map.of("1|1", new RebaseFactors(1, 4)));

        harness.processElement(new StreamRecord<>(event(1, 1, levels("5", "5"), null)));

        PriceLevel ask = output().get(0).getAsks().get(0);
        assertThat(ask.getPrice()).isEqualTo("50");
        assertThat(ask.getQuantity()).isEqualTo("50000");
    }

    @Test
    @DisplayName("factors are looked up per (exchange, pair), not shared across keys")
    void factorsArePerExchangeAndPair() throws Exception {
        openWith(Map.of(
                "1|1", new RebaseFactors(1, 0),
                "2|1", new RebaseFactors(3, 0)));

        harness.processElement(new StreamRecord<>(event(1, 1, levels("5", "1"), null)));
        harness.processElement(new StreamRecord<>(event(2, 1, levels("5", "1"), null)));

        assertThat(output().get(0).getAsks().get(0).getPrice()).isEqualTo("50");
        assertThat(output().get(1).getAsks().get(0).getPrice()).isEqualTo("5000");
    }

    // ---- side shape -------------------------------------------------------------

    @Test
    @DisplayName("a null side stays null (ex3 wallex half-book), an empty side stays empty")
    void nullSideStaysNullEmptyStaysEmpty() throws Exception {
        openWith(Map.of("3|1", new RebaseFactors(1, 1)));

        harness.processElement(new StreamRecord<>(event(3, 1, null, List.of())));

        RawOrderBookEvent out = output().get(0);
        assertThat(out.getAsks()).isNull();
        assertThat(out.getBids()).isEmpty();
    }

    @Test
    @DisplayName("every level of both sides is rebased")
    void rebasesAllLevelsOfBothSides() throws Exception {
        openWith(Map.of("1|1", new RebaseFactors(1, 0)));

        harness.processElement(new StreamRecord<>(event(1, 1,
                List.of(new PriceLevel("1", "1"), new PriceLevel("2", "1")),
                List.of(new PriceLevel("3", "1")))));

        RawOrderBookEvent out = output().get(0);
        assertThat(out.getAsks()).extracting(PriceLevel::getPrice).containsExactly("10", "20");
        assertThat(out.getBids()).extracting(PriceLevel::getPrice).containsExactly("30");
    }

    // ---- missing row ------------------------------------------------------------

    @Test
    @DisplayName("no exchange_markets row -> dead-letter no_rebase_row, nothing on the main stream")
    void missingRowIsDeadLettered() throws Exception {
        openWith(Map.of("1|1", new RebaseFactors(0, 0)));

        harness.processElement(new StreamRecord<>(event(9, 9, levels("5", "1"), null)));

        assertThat(output()).isEmpty();
        assertThat(rejects()).hasSize(1);
        RejectedOrderBookEvent rejected = rejects().get(0);
        assertThat(rejected.getRejectReason()).isEqualTo(RebaseFunction.NO_REBASE_ROW);
        assertThat(rejected.getEvent().getExchangeId()).isEqualTo(9);
        // The un-rebased original is preserved verbatim for replay.
        assertThat(rejected.getEvent().getAsks().get(0).getPrice()).isEqualTo("5");
    }

    // ---- timings ----------------------------------------------------------------

    @Test
    @DisplayName("stamps rebase_in/out on the main path, leaves rebase_out null on a reject")
    void stampsPipelineTimings() throws Exception {
        openWith(Map.of("1|1", new RebaseFactors(0, 0)));

        harness.processElement(new StreamRecord<>(event(1, 1, levels("5", "1"), null)));
        harness.processElement(new StreamRecord<>(event(9, 9, levels("5", "1"), null)));

        var passed = output().get(0).getPipelineTimings();
        assertThat(passed.getRebaseIn()).isNotNull();
        assertThat(passed.getRebaseOut()).isNotNull();
        assertThat(passed.getRebaseOut()).isGreaterThanOrEqualTo(passed.getRebaseIn());

        var rejectedTimings = rejects().get(0).getEvent().getPipelineTimings();
        assertThat(rejectedTimings.getRebaseIn()).isNotNull();
        assertThat(rejectedTimings.getRebaseOut()).isNull();
    }
}
