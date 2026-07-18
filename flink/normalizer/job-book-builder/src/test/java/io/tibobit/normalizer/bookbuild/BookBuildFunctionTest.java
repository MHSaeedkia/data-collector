package io.tibobit.normalizer.bookbuild;

import io.tibobit.normalizer.model.OrderBookSnapshot;
import io.tibobit.normalizer.model.PriceLevel;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link BookBuildFunction} through Flink's keyed harness so the real keyed state backend
 * runs — MapState iteration order and per-key isolation are part of what is under test.
 */
class BookBuildFunctionTest {

    private KeyedOneInputStreamOperatorTestHarness<String, RawOrderBookEvent, OrderBookSnapshot> harness;

    @BeforeEach
    void openHarness() throws Exception {
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new BookBuildFunction()),
                event -> event.getExchangeId() + "|" + event.getPairId(),
                Types.STRING);
        harness.open();
    }

    @AfterEach
    void closeHarness() throws Exception {
        harness.close();
    }

    // ---- helpers ----------------------------------------------------------------

    private static RawOrderBookEvent event(String type, Long sequenceId,
                                           List<PriceLevel> asks, List<PriceLevel> bids) {
        return new RawOrderBookEvent(8, 1, type, sequenceId, 0L, 1_700_000_000_000L, asks, bids);
    }

    private static RawOrderBookEvent event(String type, List<PriceLevel> asks, List<PriceLevel> bids) {
        return event(type, 1L, asks, bids);
    }

    private static List<PriceLevel> levels(String... priceQuantityPairs) {
        PriceLevel[] built = new PriceLevel[priceQuantityPairs.length / 2];
        for (int i = 0; i < built.length; i++) {
            built[i] = new PriceLevel(priceQuantityPairs[i * 2], priceQuantityPairs[i * 2 + 1]);
        }
        return List.of(built);
    }

    private OrderBookSnapshot process(RawOrderBookEvent event) throws Exception {
        harness.processElement(new StreamRecord<>(event));
        List<OrderBookSnapshot> emitted = harness.extractOutputValues();
        return emitted.get(emitted.size() - 1);
    }

    private static org.assertj.core.api.ListAssert<String> pricesOf(List<PriceLevel> side) {
        return assertThat(side.stream().map(PriceLevel::getPrice).toList());
    }

    // ---- snapshot ---------------------------------------------------------------

    @Test
    @DisplayName("a snapshot replaces the book wholesale, dropping levels it does not mention")
    void snapshotReplacesBook() throws Exception {
        process(event("snapshot", levels("10", "1", "11", "2"), levels("9", "3")));

        OrderBookSnapshot out = process(event("snapshot", levels("12", "5"), levels("8", "6")));

        pricesOf(out.getAsks()).containsExactly("12");
        pricesOf(out.getBids()).containsExactly("8");
    }

    @Test
    @DisplayName("an empty side in a snapshot clears that side")
    void emptySideClearsSide() throws Exception {
        process(event("snapshot", levels("10", "1"), levels("9", "3")));

        OrderBookSnapshot out = process(event("snapshot", List.of(), levels("9", "3")));

        assertThat(out.getAsks()).isEmpty();
        pricesOf(out.getBids()).containsExactly("9");
    }

    // ---- ex3 wallex per-side snapshot merge --------------------------------------

    @Test
    @DisplayName("ex3: a null side is NOT cleared — two one-sided snapshots merge into one book")
    void nullSideKeepsOtherSideState() throws Exception {
        process(event("snapshot", null, null, levels("9", "3")));

        OrderBookSnapshot out = process(event("snapshot", null, levels("10", "1"), null));

        pricesOf(out.getAsks()).containsExactly("10");
        pricesOf(out.getBids()).containsExactly("9");
    }

    // ---- update -----------------------------------------------------------------

    @Test
    @DisplayName("an update upserts a new price and overwrites an existing one")
    void updateUpserts() throws Exception {
        process(event("snapshot", levels("10", "1"), List.of()));

        OrderBookSnapshot out = process(event("update", levels("10", "7", "11", "2"), null));

        assertThat(out.getAsks()).hasSize(2);
        assertThat(out.getAsks().get(0).getPrice()).isEqualTo("10");
        assertThat(out.getAsks().get(0).getQuantity()).isEqualTo("7");
    }

    @Test
    @DisplayName("quantity 0 in an update deletes the level")
    void zeroQuantityDeletes() throws Exception {
        process(event("snapshot", levels("10", "1", "11", "2"), List.of()));

        OrderBookSnapshot out = process(event("update", levels("10", "0"), null));

        pricesOf(out.getAsks()).containsExactly("11");
    }

    @Test
    @DisplayName("deleting a price the book never held is a no-op, not an error")
    void deleteOfUnknownPriceIsNoop() throws Exception {
        process(event("snapshot", levels("10", "1"), List.of()));

        OrderBookSnapshot out = process(event("update", levels("99", "0"), null));

        pricesOf(out.getAsks()).containsExactly("10");
    }

    @Test
    @DisplayName("quantity 0 inside a snapshot never rests in the book (job 4 dust)")
    void zeroQuantityInSnapshotIsNotStored() throws Exception {
        OrderBookSnapshot out = process(event("snapshot", levels("10", "1", "11", "0"), List.of()));

        pricesOf(out.getAsks()).containsExactly("10");
    }

    // ---- price identity ----------------------------------------------------------

    @Test
    @DisplayName("prices differing only in trailing zeros are the same level, not two")
    void pricesAreCanonicalized() throws Exception {
        process(event("snapshot", levels("10.50", "1"), List.of()));

        OrderBookSnapshot out = process(event("update", levels("10.5", "9"), null));

        assertThat(out.getAsks()).hasSize(1);
        assertThat(out.getAsks().get(0).getPrice()).isEqualTo("10.5");
        assertThat(out.getAsks().get(0).getQuantity()).isEqualTo("9");
    }

    // ---- emitted shape -----------------------------------------------------------

    @Test
    @DisplayName("asks sort ascending and bids descending by numeric price")
    void sidesAreSorted() throws Exception {
        OrderBookSnapshot out = process(event("snapshot",
                levels("11", "1", "9", "1", "10", "1"),
                levels("11", "1", "9", "1", "10", "1")));

        pricesOf(out.getAsks()).containsExactly("9", "10", "11");
        pricesOf(out.getBids()).containsExactly("11", "10", "9");
    }

    @Test
    @DisplayName("the emitted book carries identity, event_time and last_sequence_id")
    void emitsIdentityAndSequence() throws Exception {
        OrderBookSnapshot out = process(event("snapshot", 42L, levels("10", "1"), List.of()));

        assertThat(out.getExchangeId()).isEqualTo(8);
        assertThat(out.getPairId()).isEqualTo(1);
        assertThat(out.getEventTime()).isEqualTo(1_700_000_000_000L);
        assertThat(out.getLastSequenceId()).isEqualTo(42L);
    }

    @Test
    @DisplayName("a feed with no ordering field (ex3) emits a null last_sequence_id")
    void nullSequenceStaysNull() throws Exception {
        OrderBookSnapshot out = process(event("snapshot", null, levels("10", "1"), List.of()));

        assertThat(out.getLastSequenceId()).isNull();
    }

    @Test
    @DisplayName("both sides are always present in the output, never null")
    void bothSidesAlwaysPresent() throws Exception {
        OrderBookSnapshot out = process(event("snapshot", levels("10", "1"), null));

        assertThat(out.getAsks()).isNotNull();
        assertThat(out.getBids()).isNotNull().isEmpty();
    }

    @Test
    @DisplayName("every accepted event emits one book, even when nothing changed")
    void emitsOnEveryEvent() throws Exception {
        process(event("snapshot", levels("10", "1"), List.of()));
        process(event("update", List.of(), null));

        assertThat(harness.extractOutputValues()).hasSize(2);
    }

    @Test
    @DisplayName("pipeline timings are stamped around the book build")
    void stampsTimings() throws Exception {
        OrderBookSnapshot out = process(event("snapshot", levels("10", "1"), List.of()));

        assertThat(out.getPipelineTimings().getBookBuildIn()).isNotNull();
        assertThat(out.getPipelineTimings().getBookBuildOut())
                .isNotNull()
                .isGreaterThanOrEqualTo(out.getPipelineTimings().getBookBuildIn());
    }

    // ---- keying ------------------------------------------------------------------

    @Test
    @DisplayName("books of different (exchange, pair) keys do not mix")
    void booksAreIsolatedPerKey() throws Exception {
        process(event("snapshot", levels("10", "1"), List.of()));

        RawOrderBookEvent otherPair =
                new RawOrderBookEvent(8, 2, "snapshot", 1L, 0L, 1L, levels("20", "1"), List.of());
        OrderBookSnapshot out = process(otherPair);

        pricesOf(out.getAsks()).containsExactly("20");
    }
}
