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
import static org.assertj.core.api.Assertions.tuple;

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

    // ---- reset (gap-driven book drop) --------------------------------------------

    @Test
    @DisplayName("a reset empties both sides and clears state, so the exchange drops out")
    void resetEmptiesBook() throws Exception {
        process(event("snapshot", levels("10", "1", "11", "2"), levels("9", "3")));

        OrderBookSnapshot out = process(event("reset", null, null, null));

        assertThat(out.getAsks()).isEmpty();
        assertThat(out.getBids()).isEmpty();

        // state is truly cleared: a following update builds on an empty book, not the pre-gap one
        OrderBookSnapshot after = process(event("update", levels("12", "5"), null));
        pricesOf(after.getAsks()).containsExactly("12");
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

    // ---- simulation flag ---------------------------------------------------------

    @Test
    @DisplayName("the emitted book carries the producing event's simulation flag")
    void carriesSimulationFlag() throws Exception {
        RawOrderBookEvent simulated = event("snapshot", levels("10", "1"), levels("9", "3"));
        simulated.setSimulation(1);

        assertThat(process(simulated).getSimulation()).isEqualTo(1);
    }

    @Test
    @DisplayName("an unflagged event emits a live (0) book")
    void defaultsToLive() throws Exception {
        assertThat(process(event("snapshot", levels("10", "1"), List.of())).getSimulation())
                .isZero();
    }

    @Test
    @DisplayName("the flag follows the latest event, it is not sticky book state")
    void flagFollowsLatestEvent() throws Exception {
        RawOrderBookEvent simulated = event("snapshot", levels("10", "1"), List.of());
        simulated.setSimulation(1);
        assertThat(process(simulated).getSimulation()).isEqualTo(1);

        // The book is unchanged state, but the flag is a property of the feed, not of the levels.
        assertThat(process(event("update", levels("11", "2"), List.of())).getSimulation()).isZero();
    }

    @Test
    @DisplayName("a reset empties the book and still reports the reset event's flag")
    void resetKeepsFlag() throws Exception {
        RawOrderBookEvent simulated = event("snapshot", levels("10", "1"), levels("9", "3"));
        simulated.setSimulation(1);
        process(simulated);

        RawOrderBookEvent reset = event("reset", null, null, null);
        reset.setSimulation(1);
        OrderBookSnapshot out = process(reset);

        assertThat(out.getAsks()).isEmpty();
        assertThat(out.getBids()).isEmpty();
        // Job 6 drops the exchange on an empty book — but while it is still in flight the record
        // must not claim to be live data.
        assertThat(out.getSimulation()).isEqualTo(1);
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

    // ---- lineage -----------------------------------------------------------------

    /** Job 4 stamped an id on every event that reaches here; these stand in for those. */
    private static RawOrderBookEvent from(RawOrderBookEvent event, String id) {
        event.setId(id);
        return event;
    }

    /**
     * The defining case for this job: a book assembled from three separate events. The record names
     * the event that caused THIS emit; who each level belongs to is on the levels, which is why the
     * state has to remember which event set each one.
     */
    @Test
    @DisplayName("a book made of several events names the triggering one and attributes each level")
    void namesTheTriggerAndAttributesEachLevel() throws Exception {
        process(from(event("snapshot", levels("10", "1"), List.of()), "ev-a"));
        process(from(event("update", levels("11", "2"), List.of()), "ev-b"));
        OrderBookSnapshot out = process(from(event("update", List.of(), levels("9", "3")), "ev-c"));

        assertThat(out.getTriggerId()).isEqualTo("ev-c");
        assertThat(out.getAsks()).extracting(PriceLevel::getSourceId).containsExactly("ev-a", "ev-b");
        assertThat(out.getBids()).extracting(PriceLevel::getSourceId).containsExactly("ev-c");
        assertThat(out.getId()).isNotBlank();
    }

    /**
     * A level updated by a later event belongs to that event, so the earlier one stops being named
     * anywhere once nothing it set is still resting.
     */
    @Test
    @DisplayName("overwriting a level transfers it to the newer event, retiring the older one")
    void overwrittenLevelChangesOwner() throws Exception {
        process(from(event("snapshot", levels("10", "1"), List.of()), "ev-old"));
        OrderBookSnapshot out = process(from(event("update", levels("10", "5"), List.of()), "ev-new"));

        assertThat(out.getAsks()).extracting(PriceLevel::getSourceId).containsExactly("ev-new");
        assertThat(out.getTriggerId()).isEqualTo("ev-new");
    }

    /**
     * THE reason trigger_id is its own field rather than being folded in with the level ids: an
     * event that only DELETES leaves nothing resting, so it appears on no level. Without it the
     * record would name no parent that has anything to do with why it exists.
     */
    @Test
    @DisplayName("a delete-only event is the trigger even though it owns no level")
    void deleteOnlyEventIsStillTheTrigger() throws Exception {
        process(from(event("snapshot", levels("10", "1", "11", "2"), List.of()), "ev-seed"));
        OrderBookSnapshot out = process(from(event("update", levels("11", "0"), List.of()), "ev-del"));

        pricesOf(out.getAsks()).containsExactly("10");
        assertThat(out.getAsks()).extracting(PriceLevel::getSourceId).containsExactly("ev-seed");
        assertThat(out.getTriggerId()).isEqualTo("ev-del");
    }

    /** The same argument at its limit: a reset empties the book, so there is no level to carry it. */
    @Test
    @DisplayName("a reset's emptied book still names the reset marker as its trigger")
    void resetKeepsItsOwnTrigger() throws Exception {
        process(from(event("snapshot", levels("10", "1"), levels("9", "1")), "ev-seed"));
        OrderBookSnapshot out = process(from(event("reset", null, null, null), "ev-reset"));

        assertThat(out.getAsks()).isEmpty();
        assertThat(out.getBids()).isEmpty();
        assertThat(out.getTriggerId()).isEqualTo("ev-reset");
    }

    /**
     * The per-level half of the lineage: each emitted level names the ONE event that set it, which
     * is what makes a single price traceable back to its raw event. trigger_id answers a different
     * question — why this record exists — and says nothing about any particular price.
     */
    @Test
    @DisplayName("each emitted level carries the event that set it")
    void stampsEachLevelWithItsOwningEvent() throws Exception {
        process(from(event("snapshot", levels("10", "1", "11", "2"), levels("9", "3")), "ev-seed"));
        OrderBookSnapshot out = process(from(event("update", levels("11", "5"), null), "ev-upd"));

        assertThat(out.getAsks()).extracting(PriceLevel::getPrice, PriceLevel::getSourceId)
                .containsExactly(tuple("10", "ev-seed"), tuple("11", "ev-upd"));
        assertThat(out.getBids()).extracting(PriceLevel::getPrice, PriceLevel::getSourceId)
                .containsExactly(tuple("9", "ev-seed"));
    }

    /**
     * The reason the lineage has to follow the price rather than the message: an event that touches
     * one level says nothing about the rest of the book, however many events later it arrives. A
     * level untouched for 500 events still names the event that put it there.
     */
    @Test
    @DisplayName("a level untouched by later events keeps naming the event that set it")
    void untouchedLevelKeepsItsOriginalEvent() throws Exception {
        process(from(event("snapshot", levels("10", "1"), List.of()), "ev-first"));
        process(from(event("update", levels("11", "1"), List.of()), "ev-second"));
        process(from(event("update", levels("12", "1"), List.of()), "ev-third"));
        OrderBookSnapshot out = process(from(event("update", levels("13", "1"), List.of()), "ev-fourth"));

        assertThat(out.getAsks()).extracting(PriceLevel::getPrice, PriceLevel::getSourceId)
                .containsExactly(tuple("10", "ev-first"), tuple("11", "ev-second"),
                        tuple("12", "ev-third"), tuple("13", "ev-fourth"));
    }

    /**
     * A snapshot replaces the side wholesale, so every level belongs to it — including a price that
     * happened to be resting at the same value before. Nothing may survive with a stale owner.
     */
    @Test
    @DisplayName("a snapshot takes ownership of every level it re-reports")
    void snapshotOwnsEveryLevelItReports() throws Exception {
        process(from(event("snapshot", levels("10", "1", "11", "2"), List.of()), "ev-old"));
        OrderBookSnapshot out = process(from(
                event("snapshot", levels("10", "1", "12", "4"), List.of()), "ev-new"));

        assertThat(out.getAsks()).extracting(PriceLevel::getSourceId)
                .containsOnly("ev-new");
    }

    @Test
    @DisplayName("every emitted book gets its own distinct id")
    void mintsDistinctIdPerBook() throws Exception {
        process(from(event("snapshot", levels("10", "1"), List.of()), "ev-a"));
        process(from(event("update", levels("11", "2"), List.of()), "ev-b"));

        List<OrderBookSnapshot> all = harness.extractOutputValues();
        assertThat(all).hasSize(2);
        assertThat(all).extracting(OrderBookSnapshot::getId)
                .doesNotHaveDuplicates()
                .allSatisfy(id -> assertThat(id).isNotBlank());
    }
}
