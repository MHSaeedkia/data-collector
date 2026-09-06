package io.tibobit.normalizer.typevalidate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.tibobit.normalizer.lookup.RefreshingLookup;
import io.tibobit.normalizer.model.ControlCommand;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import io.tibobit.normalizer.model.RejectedOrderBookEvent;

/**
 * Tests {@link TypeValidateFunction} against its documented sequence rules,
 * driven through Flink's {@link KeyedOneInputStreamOperatorTestHarness} keyed
 * exactly as the job —  {@code (exchange_id,
 * pair_id)} — so real keyed ValueState and the {@code open(OpenContext)}
 * lifecycle run, not a mock.
 */
class TypeValidateFunctionTest {

    private KeyedOneInputStreamOperatorTestHarness<String, RawOrderBookEvent, RawOrderBookEvent> harness;

    @BeforeEach
    void openHarness() throws Exception {
        // Empty watch list by default: no market is watched for silence, so no timer is
        // ever armed and every rule test below behaves exactly as it did before the
        // staleness feature existed.
        harness = openHarness(new TypeValidateFunction(
                TypeValidateFunction.DEFAULT_SNAPSHOT_RETRY_MS, watchList()));
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, RawOrderBookEvent, RawOrderBookEvent> openHarness(TypeValidateFunction function) throws Exception {
        KeyedProcessOperator<String, RawOrderBookEvent, RawOrderBookEvent> operator
                = new KeyedProcessOperator<>(function);
        KeySelector<RawOrderBookEvent, String> byEvent = e -> e.getExchangeId() + "|" + e.getPairId();
        KeyedOneInputStreamOperatorTestHarness<String, RawOrderBookEvent, RawOrderBookEvent> opened = new KeyedOneInputStreamOperatorTestHarness<>(
                operator, byEvent, TypeInformation.of(String.class));
        opened.open();
        return opened;
    }

    /**
     * A fixed stand-in for the {@code exchange_markets} watch list. The refresh
     * interval is effectively never, so the map the test passes is the map the
     * operator reads.
     */
    private static RefreshingLookup<String, WatchedMarket> watchList(WatchedMarket... markets) {
        Map<String, WatchedMarket> byKey = java.util.Arrays.stream(markets)
                .collect(Collectors.toMap(WatchedMarket::key, m -> m));
        return new RefreshingLookup<>(() -> byKey, Long.MAX_VALUE);
    }

    @AfterEach
    void closeHarness() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    // ---- helpers ----------------------------------------------------------------
    /**
     * Snapshot-feed event (jump 0) for ex/pair with the given ordering value.
     */
    private static RawOrderBookEvent snapshotFeed(int ex, int pair, Long seq) {
        return new RawOrderBookEvent(ex, pair, "snapshot", seq, 0L, seq == null ? 0L : seq,
                List.of(), List.of());
    }

    /**
     * Null-seq snapshot (ex3 wallex / ex6 bybit REST) with an explicit event
     * time — the field the out-of-order guard orders these by, since they carry
     * no sequence id.
     */
    private static RawOrderBookEvent nullSeqSnapshot(int ex, int pair, long eventTime) {
        return new RawOrderBookEvent(ex, pair, "snapshot", null, 0L, eventTime, List.of(), List.of());
    }

    /**
     * Delta-feed message (snapshot or update) with a nonzero jump. Usually a per-exchange
     * constant (ex6=1, ex5=650), but ex7 and ex8 stamp it PER MESSAGE — see
     * {@link #dynamicJumpChainsEachMessageToItsNamedPredecessor()}.
     */
    private static RawOrderBookEvent delta(int ex, int pair, String type, long seq, long jump) {
        return new RawOrderBookEvent(ex, pair, type, seq, jump, seq, List.of(), List.of());
    }

    /**
     * ex5/bitget WS message: the sequence is a millisecond CLOCK on a VARIABLE
     * cadence, so the event carries a wide jump tolerance and job 2 checks a
     * window instead of an equality. The 650 ± 110 band is fitted to the live
     * feed (see BitgetParser); ex5's REST snapshot is null-seq and uses
     * {@link #nullSeqSnapshot} instead.
     */
    private static RawOrderBookEvent bitget(int pair, String type, long ts) {
        RawOrderBookEvent event
                = new RawOrderBookEvent(5, pair, type, ts, 650L, ts, List.of(), List.of());
        event.setSequenceJumpTolerance(110L);
        return event;
    }

    private void send(RawOrderBookEvent e) throws Exception {
        harness.processElement(new StreamRecord<>(e));
    }

    private List<RawOrderBookEvent> valid() {
        return harness.extractOutputValues();
    }

    /**
     * Main-output events excluding the synthetic reset markers a gap now emits
     * (Part A).
     */
    private List<RawOrderBookEvent> validBusiness() {
        return valid().stream()
                .filter(e -> !TypeValidateFunction.RESET.equals(e.getType()))
                .collect(Collectors.toList());
    }

    private List<RejectedOrderBookEvent> rejects() {
        ConcurrentLinkedQueue<StreamRecord<RejectedOrderBookEvent>> q = harness
                .getSideOutput(TypeValidateFunction.REJECTED);
        return q == null ? List.of()
                : q.stream().map(StreamRecord::getValue).collect(Collectors.toList());
    }

    private List<ControlCommand> controlCommands() {
        ConcurrentLinkedQueue<StreamRecord<ControlCommand>> q = harness.getSideOutput(TypeValidateFunction.CONTROL);
        return q == null ? List.of()
                : q.stream().map(StreamRecord::getValue).collect(Collectors.toList());
    }

    // ---- snapshot feeds (jump 0, out-of-order check only) -----------------------
    @Test
    @DisplayName("snapshot feed: first snapshot is accepted as the baseline")
    void firstSnapshotAccepted() throws Exception {
        send(snapshotFeed(1, 1, 100L));
        assertThat(valid()).hasSize(1);
        assertThat(rejects()).isEmpty();
    }

    @Test
    @DisplayName("snapshot feed: a strictly newer snapshot is accepted")
    void newerSnapshotAccepted() throws Exception {
        send(snapshotFeed(1, 1, 100L));
        send(snapshotFeed(1, 1, 101L));
        assertThat(valid()).hasSize(2);
        assertThat(rejects()).isEmpty();
    }

    @Test
    @DisplayName("snapshot feed: equal or lower ordering value is rejected stale_or_duplicate")
    void staleSnapshotRejected() throws Exception {
        send(snapshotFeed(1, 1, 100L));
        send(snapshotFeed(1, 1, 100L)); // duplicate
        send(snapshotFeed(1, 1, 99L)); // out of order
        assertThat(valid()).hasSize(1);
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.STALE_OR_DUPLICATE,
                        TypeValidateFunction.STALE_OR_DUPLICATE);
    }

    // ---- no ordering field (ex3 wallex) -----------------------------------------
    @Test
    @DisplayName("null sequence_id (ex3): snapshots in event-time order pass through unchecked")
    void nullSequenceInOrderPasses() throws Exception {
        send(nullSeqSnapshot(3, 1, 100L));
        send(nullSeqSnapshot(3, 1, 200L));
        assertThat(valid()).hasSize(2);
        assertThat(rejects()).isEmpty();
    }

    @Test
    @DisplayName("null sequence_id: a snapshot older than the last accepted event is rejected out_of_order")
    void nullSeqOlderSnapshotRejected() throws Exception {
        send(nullSeqSnapshot(3, 1, 200L));
        send(nullSeqSnapshot(3, 1, 100L)); // out of order by event time
        assertThat(valid()).hasSize(1);
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.OUT_OF_ORDER);
    }

    // ---- delta feeds (jump > 0, gap/jump rule) ----------------------------------
    @Test
    @DisplayName("delta feed: an update before any snapshot is rejected no_baseline")
    void updateBeforeBaselineRejected() throws Exception {
        send(delta(6, 1, "update", 5L, 1L));
        assertThat(valid()).isEmpty();
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.NO_BASELINE);
    }

    @Test
    @DisplayName("delta feed (ex6, jump 1): snapshot then contiguous updates are accepted")
    void contiguousUpdatesAcceptedJump1() throws Exception {
        send(delta(6, 1, "snapshot", 10L, 1L));
        send(delta(6, 1, "update", 11L, 1L));
        send(delta(6, 1, "update", 12L, 1L));
        assertThat(valid()).hasSize(3);
        assertThat(rejects()).isEmpty();
    }

    @Test
    @DisplayName("delta feed (fixed jump 300): update at last + 300 is accepted")
    void contiguousUpdateAcceptedJump300() throws Exception {
        send(delta(8, 1, "snapshot", 1000L, 300L));
        send(delta(8, 1, "update", 1300L, 300L));
        assertThat(valid()).hasSize(2);
        assertThat(rejects()).isEmpty();
    }

    /**
     * The DYNAMIC jump, which had no cover here at all until 2026-09-05 despite two exchanges
     * resting on it: ex7/ompfinex ({@code u - U}) and, since the okx {@code books} switch,
     * ex8 ({@code seqId - prevSeqId}). Those parsers stamp the jump PER MESSAGE from a frame that
     * names its own predecessor, which makes this function's {@code seq == last + jump} reduce
     * algebraically to <b>"the predecessor it names is the one we last accepted"</b>.
     *
     * <p>So the jump varies message to message and none of the values would pass as a constant —
     * that is the point. Job 2 needs no knowledge of any of this; it is pinned here because the
     * reduction is the whole correctness argument for both exchanges.
     */
    @Test
    @DisplayName("delta feed (dynamic jump, ex7/ex8): each message chains to the predecessor it names")
    void dynamicJumpChainsEachMessageToItsNamedPredecessor() throws Exception {
        send(delta(8, 1, "snapshot", 4429784547L, 0L)); // snapshot: ordered, never jump-checked
        send(delta(8, 1, "update", 4429784551L, 4L));   // prevSeqId 4429784547 == lastSeq
        send(delta(8, 1, "update", 4429784558L, 7L));   // prevSeqId 4429784551 == lastSeq
        send(delta(8, 1, "update", 4429784560L, 2L));   // prevSeqId 4429784558 == lastSeq
        assertThat(valid()).hasSize(4);
        assertThat(rejects()).isEmpty();
    }

    /**
     * The other half of the same contract: a message naming a predecessor we never accepted is a
     * gap, however small the jump. Here the counter really does advance by 4 — the value a healthy
     * frame might carry — but it is measured from 4429784559, which we never saw, so the frame it
     * chains to is missing and the book can no longer be trusted.
     */
    @Test
    @DisplayName("delta feed (dynamic jump): naming an unseen predecessor is a gap, not a small jump")
    void dynamicJumpRejectsAnUnseenPredecessor() throws Exception {
        send(delta(8, 1, "snapshot", 4429784547L, 0L));
        send(delta(8, 1, "update", 4429784551L, 4L)); // accepted, lastSeq = ...551
        send(delta(8, 1, "update", 4429784563L, 4L)); // prevSeqId ...559 != lastSeq ...551
        assertThat(valid()).hasSize(3); // the two good ones plus the RESET the gap emits
        assertThat(valid().get(2).getType()).isEqualTo(TypeValidateFunction.RESET);
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.SEQUENCE_GAP);
    }

    @Test
    @DisplayName("delta feed: a stale/duplicate update (seq <= last) is rejected stale_or_duplicate")
    void staleUpdateRejected() throws Exception {
        send(delta(6, 1, "snapshot", 10L, 1L));
        send(delta(6, 1, "update", 11L, 1L));
        send(delta(6, 1, "update", 11L, 1L)); // duplicate
        send(delta(6, 1, "update", 8L, 1L)); // older
        assertThat(valid()).hasSize(2);
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.STALE_OR_DUPLICATE,
                        TypeValidateFunction.STALE_OR_DUPLICATE);
    }

    @Test
    @DisplayName("tolerant jump (ex5): both edges of the last+jump±tolerance window are contiguous")
    void toleranceWindowEdgesAccepted() throws Exception {
        long t0 = 1787404282000L;
        send(bitget(1, "snapshot", t0));
        send(bitget(1, "update", t0 + 540)); // low edge  -> ok
        send(bitget(1, "update", t0 + 540 + 760)); // high edge -> ok
        send(bitget(1, "update", t0 + 540 + 760 + 650)); // dead centre -> ok

        assertThat(validBusiness()).extracting(RawOrderBookEvent::getSequenceId)
                .containsExactly(t0, t0 + 540, t0 + 1300, t0 + 1950);
        assertThat(rejects()).isEmpty();
    }

    @Test
    @DisplayName("tolerant jump (ex5): one millisecond outside the window is still a gap")
    void toleranceWindowIsNotUnbounded() throws Exception {
        long t0 = 1787404282000L;
        send(bitget(1, "snapshot", t0));
        send(bitget(1, "update", t0 + 761)); // 1 ms past the high edge -> gap

        assertThat(validBusiness()).extracting(RawOrderBookEvent::getSequenceId)
                .containsExactly(t0);
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.SEQUENCE_GAP);
    }

    /**
     * The live resync loop, reproduced and shown fixed (dev server,
     * 2026-08-23). ex5's WS feed sends NO snapshots — the REST endpoint is its
     * only baseline — and the REST {@code ts} runs on a different clock, BEHIND
     * the last WS update 57% of the time. When that body was sequenced by its
     * own {@code ts} it seeded the update window from the wrong clock, so the
     * next update gapped ~90% of the time: accept → gap → empty the book →
     * request another snapshot → repeat, 22 times a minute. Null-seq breaks the
     * cycle by never comparing the two clocks: the REST body is ordered by
     * event time, and the next update re-anchors the baseline.
     */
    @Test
    @DisplayName("ex5 resync: a REST snapshot on the other clock no longer gaps the next update")
    void bitgetRestResyncDoesNotSeedTheWindow() throws Exception {
        long t0 = 1787404282000L;
        send(bitget(1, "update", t0)); // cold start -> no_baseline, asks for a snapshot
        send(nullSeqSnapshot(5, 1, t0 - 40)); // the REST answer, ts BEHIND the update it follows
        send(bitget(1, "update", t0 + 600)); // baselinePending adopts this unconditionally
        send(bitget(1, "update", t0 + 1200)); // +600 -> inside the window
        send(bitget(1, "update", t0 + 1950)); // +750 -> the live cluster the old window rejected

        assertThat(validBusiness()).extracting(RawOrderBookEvent::getSequenceId)
                .containsExactly(null, t0 + 600, t0 + 1200, t0 + 1950);
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.NO_BASELINE);
        assertThat(controlCommands()).hasSize(1); // ONE ask, not one per snapshot
    }

    /**
     * The band is widened, not removed: a genuinely missed tick is ~2x the
     * cadence and still lands outside [540, 760]. Without this the widening
     * would have quietly disabled ex5 gap detection.
     */
    @Test
    @DisplayName("tolerant jump (ex5): a missed tick (~1200 ms) is still a gap")
    void bitgetStillDetectsAMissedTick() throws Exception {
        long t0 = 1787404282000L;
        send(bitget(1, "snapshot", t0));
        send(bitget(1, "update", t0 + 750)); // the upper live cluster -> accepted
        send(bitget(1, "update", t0 + 750 + 1200)); // one tick lost -> gap

        assertThat(validBusiness()).extracting(RawOrderBookEvent::getSequenceId)
                .containsExactly(t0, t0 + 750);
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.SEQUENCE_GAP);
    }

    @Test
    @DisplayName("tolerant jump (ex5): a backwards ts is stale_or_duplicate, not a gap")
    void toleranceWindowStillRejectsStale() throws Exception {
        long t0 = 1787404282000L;
        send(bitget(1, "snapshot", t0));
        send(bitget(1, "update", t0 + 600)); // ok
        send(bitget(1, "update", t0 + 600)); // duplicate ts
        send(bitget(1, "update", t0 + 100)); // older

        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.STALE_OR_DUPLICATE,
                        TypeValidateFunction.STALE_OR_DUPLICATE);
    }

    @Test
    @DisplayName("tolerance 0 (every other exchange) keeps the exact seq == last + jump check")
    void zeroToleranceIsTheExactCheck() throws Exception {
        send(delta(6, 1, "snapshot", 10L, 1L));
        send(delta(6, 1, "update", 11L, 1L)); // exact -> ok
        send(delta(6, 1, "update", 13L, 1L)); // +2 with tolerance 0 -> gap, not accepted

        assertThat(validBusiness()).extracting(RawOrderBookEvent::getSequenceId)
                .containsExactly(10L, 11L);
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.SEQUENCE_GAP);
    }

    // ---- a snapshot is ordered, never jump-checked -------------------------------
    //
    // The rule, restated by the user 2026-08-23 and true of every exchange: a snapshot
    // RE-ANCHORS the sequence, so the only thing worth asking is whether it is newer than
    // what we already have. `sequence_jump` describes the cadence BETWEEN deltas and says
    // nothing about the interval from an update to the snapshot that replaces it — the
    // collector chose when to send that, not the exchange's tick. Contiguity therefore has
    // exactly two legal sites, both in the update branch: snapshot -> next update, and
    // update -> update. The tests below fail the moment a jump check leaks into the
    // snapshot branch; the jump-0 snapshot-feed tests above cannot catch that, because
    // `last + 0` is trivially satisfied by nothing.
    @Test
    @DisplayName("snapshot after updates (ex6, jump 1): accepted however far past last+jump it lands")
    void snapshotAfterUpdatesIgnoresTheJump() throws Exception {
        send(delta(6, 1, "snapshot", 10L, 1L));
        send(delta(6, 1, "update", 11L, 1L));
        send(delta(6, 1, "snapshot", 5000L, 1L)); // nowhere near 11 + 1, and NOT a gap
        send(delta(6, 1, "update", 5001L, 1L)); // contiguity resumes from the snapshot

        assertThat(validBusiness()).extracting(RawOrderBookEvent::getSequenceId)
                .containsExactly(10L, 11L, 5000L, 5001L);
        assertThat(rejects()).isEmpty();
    }

    @Test
    @DisplayName("snapshot after updates (fixed jump 300): accepted even when it lands SHORT of last+jump")
    void snapshotShortOfTheJumpIsStillAccepted() throws Exception {
        send(delta(8, 1, "snapshot", 1000L, 300L));
        send(delta(8, 1, "update", 1300L, 300L));
        send(delta(8, 1, "snapshot", 1307L, 300L)); // +7: forward, but far short of +300
        send(delta(8, 1, "update", 1607L, 300L));

        assertThat(validBusiness()).extracting(RawOrderBookEvent::getSequenceId)
                .containsExactly(1000L, 1300L, 1307L, 1607L);
        assertThat(rejects()).isEmpty();
    }

    @Test
    @DisplayName("snapshot after updates (ex5): a resync ts off the 600 ms grid re-anchors the window")
    void bitgetResyncSnapshotIsNotWindowChecked() throws Exception {
        // Shape of ex5's REST depth resync, which carries the same jump/tolerance as the WS
        // snapshot (see BitgetParser) and arrives whenever the collector answered, not on a tick.
        long t0 = 1787404282000L;
        send(bitget(1, "snapshot", t0));
        send(bitget(1, "update", t0 + 650));
        send(bitget(1, "snapshot", t0 + 1500)); // 190 ms past the window end -> still accepted
        send(bitget(1, "update", t0 + 2150)); // 650 after the SNAPSHOT, not after t0 + 650

        assertThat(validBusiness()).extracting(RawOrderBookEvent::getSequenceId)
                .containsExactly(t0, t0 + 650, t0 + 1500, t0 + 2150);
        assertThat(rejects()).isEmpty();
    }

    @Test
    @DisplayName("snapshot on a delta feed: ordering is the WHOLE test — equal rejects, one forward passes")
    void snapshotOrderingBoundaryIgnoresTheJump() throws Exception {
        send(delta(8, 1, "snapshot", 1000L, 300L));
        send(delta(8, 1, "snapshot", 1000L, 300L)); // equal -> stale_or_duplicate
        send(delta(8, 1, "snapshot", 1001L, 300L)); // one forward -> accepted, jump irrelevant

        assertThat(validBusiness()).extracting(RawOrderBookEvent::getSequenceId)
                .containsExactly(1000L, 1001L);
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.STALE_OR_DUPLICATE);
    }

    @Test
    @DisplayName("delta feed: a gap rejects sequence_gap then holds every update as awaiting_snapshot until a snapshot re-syncs")
    void gapThenAwaitingSnapshotUntilResync() throws Exception {
        send(delta(6, 1, "snapshot", 10L, 1L));
        send(delta(6, 1, "update", 11L, 1L)); // ok
        send(delta(6, 1, "update", 15L, 1L)); // gap (expected 12) -> sequence_gap
        send(delta(6, 1, "update", 16L, 1L)); // still awaiting -> awaiting_snapshot
        send(delta(6, 1, "snapshot", 20L, 1L)); // re-sync -> accepted, clears awaiting
        send(delta(6, 1, "update", 21L, 1L)); // contiguous again -> ok

        assertThat(validBusiness()).extracting(RawOrderBookEvent::getSequenceId)
                .containsExactly(10L, 11L, 20L, 21L);
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.SEQUENCE_GAP,
                        TypeValidateFunction.AWAITING_SNAPSHOT);
    }

    @Test
    @DisplayName("delta feed: a gap emits exactly one reset marker on the main output AND still dead-letters the update")
    void gapEmitsResetMarkerOncePerEpisode() throws Exception {
        send(delta(6, 1, "snapshot", 10L, 1L));
        send(delta(6, 1, "update", 11L, 1L)); // ok
        send(delta(6, 1, "update", 15L, 1L)); // gap -> reset + dead-letter
        send(delta(6, 1, "update", 16L, 1L)); // still awaiting -> NO second reset

        List<RawOrderBookEvent> resets = valid().stream()
                .filter(e -> TypeValidateFunction.RESET.equals(e.getType()))
                .collect(Collectors.toList());
        assertThat(resets).hasSize(1);

        RawOrderBookEvent reset = resets.get(0);
        assertThat(reset.getExchangeId()).isEqualTo(6);
        assertThat(reset.getPairId()).isEqualTo(1);
        assertThat(reset.getSequenceId()).isNull();
        assertThat(reset.getAsks()).isNull();
        assertThat(reset.getBids()).isNull();
        assertThat(reset.getEventTime()).isEqualTo(15L); // event_time from the gap event
        assertThat(reset.getPipelineTimings().getTypeValidateOut()).isNotNull();

        // the offending update is still dead-lettered; the held update is a plain
        // awaiting reject
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.SEQUENCE_GAP,
                        TypeValidateFunction.AWAITING_SNAPSHOT);
    }

    // ---- simulation flag
    // ---------------------------------------------------------
    @Test
    @DisplayName("a passed-through event keeps its simulation flag")
    void validEventKeepsSimulationFlag() throws Exception {
        RawOrderBookEvent simulated = delta(6, 1, "snapshot", 10L, 1L);
        simulated.setSimulation(1);

        send(simulated);

        assertThat(valid()).singleElement()
                .extracting(RawOrderBookEvent::getSimulation).isEqualTo(1);
    }

    @Test
    @DisplayName("the synthetic reset marker inherits the gap event's simulation flag")
    void resetMarkerInheritsSimulationFlag() throws Exception {
        RawOrderBookEvent seed = delta(6, 1, "snapshot", 10L, 1L);
        seed.setSimulation(1);
        send(seed);

        RawOrderBookEvent gap = delta(6, 1, "update", 15L, 1L); // gap -> reset
        gap.setSimulation(1);
        send(gap);

        // The marker is built fresh rather than forwarded, so without this the reset
        // that empties a
        // simulated exchange's book would come out flagged as live data.
        RawOrderBookEvent reset = valid().stream()
                .filter(e -> TypeValidateFunction.RESET.equals(e.getType()))
                .findFirst()
                .orElseThrow();
        assertThat(reset.getSimulation()).isEqualTo(1);
    }

    // ---- ex6 bybit: null-seq REST snapshot resyncs the WS delta stream
    // (ex1/ex2 used this same shape until 2026-09-02 — see TypeValidateFunction's
    // class javadoc; both now send WS snapshots instead, so ex6 is the current
    // live example of this bootstrap)
    // ---------
    @Test
    @DisplayName("ex6: the first update after a null-seq REST snapshot adopts its offset as the baseline, then gaps are enforced")
    void restSnapshotResyncsThenGapChecks() throws Exception {
        send(nullSeqSnapshot(6, 1, 499L)); // REST snapshot: no offset, flags a resync
        send(delta(6, 1, "update", 500L, 1L)); // first WS delta -> adopts 500 as baseline
        send(delta(6, 1, "update", 501L, 1L)); // contiguous -> ok
        send(delta(6, 1, "update", 505L, 1L)); // gap (expected 502) -> sequence_gap

        assertThat(validBusiness()).extracting(RawOrderBookEvent::getSequenceId)
                .containsExactly(null, 500L, 501L);
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.SEQUENCE_GAP);
    }

    @Test
    @DisplayName("ex6: a later REST snapshot re-anchors the baseline unconditionally, recovering from awaiting_snapshot")
    void laterRestSnapshotReanchors() throws Exception {
        send(nullSeqSnapshot(6, 1, 499L)); // resync
        send(delta(6, 1, "update", 500L, 1L)); // baseline 500
        send(delta(6, 1, "update", 505L, 1L)); // gap -> sequence_gap, awaiting
        send(delta(6, 1, "update", 506L, 1L)); // still awaiting -> awaiting_snapshot
        send(nullSeqSnapshot(6, 1, 899L)); // REST re-snapshot (newer) -> clears awaiting, flags resync
        send(delta(6, 1, "update", 900L, 1L)); // adopts 900 unconditionally
        send(delta(6, 1, "update", 901L, 1L)); // contiguous -> ok

        assertThat(validBusiness()).extracting(RawOrderBookEvent::getSequenceId)
                .containsExactly(null, 500L, null, 900L, 901L);
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.SEQUENCE_GAP,
                        TypeValidateFunction.AWAITING_SNAPSHOT);
    }

    @Test
    @DisplayName("ex6: an update before any REST snapshot still rejects no_baseline (resync flag gates the bootstrap)")
    void updateBeforeAnyRestSnapshotRejected() throws Exception {
        send(delta(6, 1, "update", 500L, 1L));
        assertThat(valid()).isEmpty();
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.NO_BASELINE);
    }

    @Test
    @DisplayName("ex6: an OLD REST snapshot replayed after newer WS deltas is rejected out_of_order and does NOT re-arm the resync")
    void staleRestSnapshotAfterUpdatesRejected() throws Exception {
        send(nullSeqSnapshot(6, 1, 499L)); // resync, event time 499
        send(delta(6, 1, "update", 500L, 1L)); // adopts 500 as baseline (event time 500)
        send(delta(6, 1, "update", 501L, 1L)); // contiguous -> ok (event time 501)
        send(nullSeqSnapshot(6, 1, 499L)); // OLD snapshot replayed: 499 < 501 -> out_of_order
        send(delta(6, 1, "update", 600L, 1L)); // if the stale snapshot had wrongly re-armed the
        // resync this would be ADOPTED; instead it is a gap

        assertThat(validBusiness()).extracting(RawOrderBookEvent::getSequenceId)
                .containsExactly(null, 500L, 501L);
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.OUT_OF_ORDER,
                        TypeValidateFunction.SEQUENCE_GAP);
    }

    // ---- keying isolation -------------------------------------------------------
    @Test
    @DisplayName("state is per (exchange_id, pair_id): keys do not cross-contaminate")
    void stateIsolatedPerKey() throws Exception {
        send(delta(6, 1, "snapshot", 10L, 1L));
        send(delta(6, 2, "update", 5L, 1L)); // different pair, no baseline -> no_baseline
        send(delta(6, 1, "update", 11L, 1L)); // ex6/p1 baseline intact -> ok
        assertThat(valid()).hasSize(2);
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.NO_BASELINE);
    }

    // ---- pipeline timings -------------------------------------------------------
    @Test
    @DisplayName("timings: valid events get type_validate_in and _out; rejects get _in only")
    void timingsStamped() throws Exception {
        send(snapshotFeed(1, 1, 100L)); // valid
        send(snapshotFeed(1, 1, 99L)); // rejected

        RawOrderBookEvent ok = valid().get(0);
        assertThat(ok.getPipelineTimings().getTypeValidateIn()).isNotNull();
        assertThat(ok.getPipelineTimings().getTypeValidateOut()).isNotNull();

        RawOrderBookEvent bad = rejects().get(0).getEvent();
        assertThat(bad.getPipelineTimings().getTypeValidateIn()).isNotNull();
        assertThat(bad.getPipelineTimings().getTypeValidateOut()).isNull();
    }

    // ---- lineage ----------------------------------------------------------------
    /**
     * Every incoming event arrives with the id job 1 gave it.
     */
    private static RawOrderBookEvent from(RawOrderBookEvent event, String id) {
        event.setId(id);
        return event;
    }

    @Test
    @DisplayName("a forwarded event takes the id it arrived with as its source and mints a new one")
    void forwardedEventIsRestamped() throws Exception {
        send(from(snapshotFeed(1, 1, 100L), "job1-id"));

        RawOrderBookEvent out = valid().get(0);
        assertThat(out.getSourceIds()).containsExactly("job1-id");
        assertThat(out.getId()).isNotBlank().isNotEqualTo("job1-id");
    }

    /**
     * The dead-letter record is a record of its own, so it gets its own id and
     * names the rejected event as its parent. The nested event keeps the id it
     * came in with — it is being reported on, not forwarded, and that id is the
     * link back to the raw stream.
     */
    @Test
    @DisplayName("a rejection gets its own id and keeps the rejected event's untouched")
    void rejectionHasItsOwnLineage() throws Exception {
        send(from(snapshotFeed(1, 1, 100L), "job1-first"));
        send(from(snapshotFeed(1, 1, 99L), "job1-stale")); // out of order -> rejected

        RejectedOrderBookEvent rejection = rejects().get(0);
        assertThat(rejection.getSourceIds()).containsExactly("job1-stale");
        assertThat(rejection.getId()).isNotBlank().isNotEqualTo("job1-stale");
        assertThat(rejection.getEvent().getId()).isEqualTo("job1-stale");
    }

    /**
     * On a gap the one event produces TWO records — a reset marker on the main
     * stream and a dead-letter record — and both name it as their parent while
     * carrying distinct ids of their own. A fan-out of lineage, not a fan-in.
     */
    @Test
    @DisplayName("a gap's reset marker and dead-letter both descend from the gap event")
    void gapProducesTwoDistinctChildren() throws Exception {
        send(from(delta(6, 1, "snapshot", 10L, 1L), "job1-base"));
        send(from(delta(6, 1, "update", 20L, 1L), "job1-gap")); // gap -> reset + reject

        RawOrderBookEvent reset = valid().stream()
                .filter(e -> TypeValidateFunction.RESET.equals(e.getType()))
                .findFirst()
                .orElseThrow();
        RejectedOrderBookEvent rejection = rejects().get(0);

        assertThat(reset.getSourceIds()).containsExactly("job1-gap");
        assertThat(rejection.getSourceIds()).containsExactly("job1-gap");
        assertThat(reset.getId()).isNotBlank().isNotEqualTo(rejection.getId());
    }

    // ---- control-plane: snapshot_request commands
    // --------------------------------
    @Test
    @DisplayName("control-plane: an update with no baseline requests a snapshot for its (exchange, pair)")
    void noBaselineRequestsSnapshot() throws Exception {
        send(delta(6, 1, "update", 5L, 1L));

        assertThat(controlCommands()).singleElement().satisfies(cmd -> {
            assertThat(cmd.getAction()).isEqualTo(ControlCommand.SNAPSHOT_REQUEST);
            assertThat(cmd.getReason()).isEqualTo(TypeValidateFunction.NO_BASELINE);
            assertThat(cmd.getExchangeId()).isEqualTo(6);
            assertThat(cmd.getPairId()).isEqualTo(1);
        });
    }

    @Test
    @DisplayName("control-plane: a sequence gap requests a snapshot for its (exchange, pair)")
    void sequenceGapRequestsSnapshot() throws Exception {
        send(delta(6, 1, "snapshot", 10L, 1L));
        send(delta(6, 1, "update", 15L, 1L)); // gap

        assertThat(controlCommands()).singleElement().satisfies(cmd -> {
            assertThat(cmd.getAction()).isEqualTo(ControlCommand.SNAPSHOT_REQUEST);
            assertThat(cmd.getReason()).isEqualTo(TypeValidateFunction.SEQUENCE_GAP);
            assertThat(cmd.getExchangeId()).isEqualTo(6);
            assertThat(cmd.getPairId()).isEqualTo(1);
        });
    }

    /**
     * The two triggers have to be told apart on the topic, not just in the
     * dead-letter stream: a market that has never had a baseline needs a first
     * snapshot, one whose book a gap invalidated needs a replacement, and the
     * collector may well answer them differently (a cold start asks for every
     * subscribed market at once — see the class javadoc).
     */
    @Test
    @DisplayName("control-plane: the reason distinguishes the two triggers on one key")
    void reasonDistinguishesTheTwoTriggers() throws Exception {
        send(delta(6, 1, "update", 5L, 1L));      // no baseline -> command 1
        send(delta(6, 1, "snapshot", 10L, 1L));   // resolves it
        send(delta(6, 1, "update", 15L, 1L));     // gap -> command 2

        assertThat(controlCommands()).extracting(ControlCommand::getReason)
                .containsExactly(TypeValidateFunction.NO_BASELINE,
                        TypeValidateFunction.SEQUENCE_GAP);
    }

    /**
     * The command is a write to a topic, so its lineage is DERIVED like the
     * reset marker's and the dead-letter's — not inherited from the gap event.
     * Inheriting looks fine (both fields hold well-formed values) but reuses an
     * id that is already carried inside the dead-letter envelope, and points
     * one hop too far back, so the request cannot be traced to the event that
     * caused it.
     */
    @Test
    @DisplayName("control-plane: a snapshot_request derives its lineage from the triggering event")
    void snapshotRequestHasItsOwnLineage() throws Exception {
        send(from(delta(6, 1, "snapshot", 10L, 1L), "job1-base"));
        send(from(delta(6, 1, "update", 20L, 1L), "job1-gap")); // gap -> request

        assertThat(controlCommands()).singleElement().satisfies(cmd -> {
            assertThat(cmd.getSourceIds()).containsExactly("job1-gap");
            assertThat(cmd.getId()).isNotBlank().isNotEqualTo("job1-gap");
        });
    }

    /**
     * Without this, a gap in simulated data would ask NiFi for a real snapshot
     * from a real exchange — and the e2e suite feeds nothing but
     * {@code simulation: 1}.
     */
    @Test
    @DisplayName("control-plane: a snapshot_request carries the gap event's simulation flag")
    void snapshotRequestCarriesSimulationFlag() throws Exception {
        RawOrderBookEvent seed = delta(6, 1, "snapshot", 10L, 1L);
        seed.setSimulation(1);
        send(seed);

        RawOrderBookEvent gap = delta(6, 1, "update", 15L, 1L); // gap -> request
        gap.setSimulation(1);
        send(gap);

        assertThat(controlCommands()).singleElement()
                .extracting(ControlCommand::getSimulation).isEqualTo(1);
    }

    @Test
    @DisplayName("control-plane: repeated rejects for the same unresolved gap request only ONE snapshot")
    void repeatedGapRejectsRequestSnapshotOnce() throws Exception {
        send(delta(6, 1, "snapshot", 10L, 1L));
        send(delta(6, 1, "update", 15L, 1L)); // gap -> request #1
        send(delta(6, 1, "update", 16L, 1L)); // still awaiting -> no second request
        send(delta(6, 1, "update", 17L, 1L)); // still awaiting -> no third request

        assertThat(controlCommands()).hasSize(1);
    }

    @Test
    @DisplayName("control-plane: repeated updates with no baseline request only ONE snapshot")
    void repeatedNoBaselineRequestsSnapshotOnce() throws Exception {
        send(delta(6, 1, "update", 5L, 1L)); // no baseline -> request #1
        send(delta(6, 1, "update", 6L, 1L)); // still no baseline -> no second request

        assertThat(controlCommands()).hasSize(1);
    }

    @Test
    @DisplayName("control-plane: after a snapshot resolves a gap, a NEW gap requests a snapshot again")
    void newGapAfterResyncRequestsSnapshotAgain() throws Exception {
        send(delta(6, 1, "snapshot", 10L, 1L));
        send(delta(6, 1, "update", 15L, 1L)); // gap -> request #1
        send(delta(6, 1, "snapshot", 20L, 1L)); // resync -> clears the "already requested" flag
        send(delta(6, 1, "update", 25L, 1L)); // a fresh gap -> request #2

        assertThat(controlCommands()).hasSize(2);
    }

    @Test
    @DisplayName("control-plane: a null-seq snapshot resolving a no-baseline condition clears the request flag too")
    void nullSeqBaselineClearsRequestFlag() throws Exception {
        send(delta(6, 1, "update", 500L, 1L)); // no baseline (ex6 before its REST snapshot) -> request #1
        send(nullSeqSnapshot(6, 1, 499L)); // REST snapshot arrives, flags a resync
        send(delta(6, 1, "update", 900L, 1L)); // adopts baseline unconditionally, clears request flag
        send(delta(6, 1, "update", 950L, 1L)); // a fresh gap -> request #2

        assertThat(controlCommands()).hasSize(2);
    }

    @Test
    @DisplayName("control-plane: state is per (exchange_id, pair_id) — a request for one key doesn't affect another")
    void controlRequestsIsolatedPerKey() throws Exception {
        send(delta(6, 1, "update", 5L, 1L)); // no baseline on ex6/p1 -> request
        send(delta(6, 2, "update", 5L, 1L)); // no baseline on ex6/p2 -> separate request

        assertThat(controlCommands()).hasSize(2);
        assertThat(controlCommands()).extracting(ControlCommand::getPairId)
                .containsExactlyInAnyOrder(1, 2);
    }

    // ---- control-plane: escaping a stuck resync (regression, 2026-08-19) -------
    @Test
    @DisplayName("control-plane: a resync snapshot older than the last delta is ACCEPTED, not deadlocked (ex6)")
    void resyncSnapshotWithLaggingClockIsAccepted() throws Exception {
        // ex6 shape: REST snapshot seeds the resync, first WS delta adopts the baseline.
        send(nullSeqSnapshot(6, 1, 1_000L));
        send(delta(6, 1, "update", 10L, 1L));   // adopts baseline, lastEventTime = 10
        send(delta(6, 1, "update", 11L, 1L));   // contiguous, lastEventTime = 11
        send(delta(6, 1, "update", 99L, 1L));   // GAP -> one command, book emptied by the reset
        assertThat(controlCommands()).hasSize(1);

        // NiFi answers with a REST snapshot whose event_time trails the last accepted delta —
        // different clock, not an old book. Before the fix this rejected out_of_order, and since
        // lastEventTime only advances in emit() the key could never recover or re-ask.
        send(nullSeqSnapshot(6, 1, 5L));
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.SEQUENCE_GAP);

        // and the resync really resolved: the next gap opens a NEW episode and asks again.
        send(delta(6, 1, "update", 200L, 1L));  // adopts the fresh baseline
        send(delta(6, 1, "update", 500L, 1L));  // GAP #2
        assertThat(controlCommands()).hasSize(2);
    }

    @Test
    @DisplayName("control-plane: a resync snapshot at or below lastSeq is ACCEPTED, not deadlocked (ex6/ex8)")
    void resyncSnapshotWithStaleSequenceIsAccepted() throws Exception {
        send(delta(6, 1, "snapshot", 100L, 1L));
        send(delta(6, 1, "update", 101L, 1L));
        send(delta(6, 1, "update", 500L, 1L));  // GAP
        assertThat(controlCommands()).hasSize(1);

        // The requested snapshot comes back with an ordering value behind the pre-gap baseline.
        // There is no good book to protect — the reset already emptied it — so take it.
        send(delta(6, 1, "snapshot", 50L, 1L));
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.SEQUENCE_GAP);

        send(delta(6, 1, "update", 51L, 1L));   // contiguous on the new baseline
        assertThat(validBusiness()).hasSize(4); // snapshot, update, resync snapshot, update
    }

    @Test
    @DisplayName("control-plane: the ordering guards still reject when NO request is outstanding")
    void guardsStillApplyWithoutAnOutstandingRequest() throws Exception {
        // Same two rejections as before the exemption — it must only suspend the guards while a
        // snapshot has actually been asked for, never in steady state.
        send(nullSeqSnapshot(1, 1, 1_000L));
        send(nullSeqSnapshot(1, 1, 900L));      // older REST replay -> still out_of_order
        send(delta(6, 1, "snapshot", 100L, 1L));
        send(delta(6, 1, "snapshot", 100L, 1L)); // duplicate -> still stale_or_duplicate
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.OUT_OF_ORDER,
                        TypeValidateFunction.STALE_OR_DUPLICATE);
        assertThat(controlCommands()).isEmpty();
    }

    // ---- control-plane: re-asking for a request nothing answered ---------------
    //
    // The retry is driven by REJECTED EVENTS, not by a timer: every event the two
    // untrustworthy branches turn away also asks, and the ask is suppressed unless
    // snapshotRetryMs has passed since the last one. So a retry needs BOTH the clock
    // to move and an event to arrive, and these tests drive both by hand.
    /**
     * Reopens the harness with a short retry interval and the given watch list,
     * clock at zero. No markets means nothing is watched for silence, which is
     * what the pure control-plane retry tests want.
     */
    private void withRetryInterval(long ms, WatchedMarket... watched) throws Exception {
        harness.close();
        harness = openHarness(new TypeValidateFunction(ms, watchList(watched)));
        harness.setProcessingTime(0L);
    }

    @Test
    @DisplayName("control-plane: an unanswered request is re-asked once the interval has passed")
    void unansweredRequestIsRetried() throws Exception {
        withRetryInterval(60_000L);
        send(delta(8, 1, "snapshot", 1000L, 300L));
        send(delta(8, 1, "update", 1300L, 300L));
        send(from(delta(8, 1, "update", 99000L, 300L), "job1-gap")); // GAP — unanswered
        assertThat(controlCommands()).hasSize(1);

        // The feed keeps talking, as a feed does after a gap. Every one of these is held
        // as awaiting_snapshot, and each one is a chance to re-ask.
        harness.setProcessingTime(60_000L);
        send(from(delta(8, 1, "update", 99300L, 300L), "job1-held-1"));
        assertThat(controlCommands()).hasSize(2);

        harness.setProcessingTime(120_000L);
        send(from(delta(8, 1, "update", 99600L, 300L), "job1-held-2"));
        assertThat(controlCommands()).hasSize(3);

        // Each retry is its own record, and names the update it was sent alongside rather
        // than re-naming the original gap — so every command points at a record that is
        // really on the dead-letter topic, and the three are distinguishable.
        assertThat(controlCommands()).extracting(ControlCommand::getId).doesNotHaveDuplicates();
        assertThat(controlCommands()).flatExtracting(ControlCommand::getSourceIds)
                .containsExactly("job1-gap", "job1-held-1", "job1-held-2");
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.SEQUENCE_GAP,
                        TypeValidateFunction.AWAITING_SNAPSHOT,
                        TypeValidateFunction.AWAITING_SNAPSHOT);
    }

    @Test
    @DisplayName("control-plane: rejected events inside the interval do NOT re-ask")
    void rejectionsInsideTheIntervalDoNotReAsk() throws Exception {
        // The flood check, and the reason the ask is time-based rather than per-event: a
        // busy feed can reject hundreds of updates between two retries and must still
        // produce exactly one command.
        withRetryInterval(60_000L);
        send(delta(8, 1, "snapshot", 1000L, 300L));
        send(delta(8, 1, "update", 99000L, 300L)); // GAP -> one command at t=0
        for (long i = 1; i <= 20; i++) {
            harness.setProcessingTime(i * 1_000L); // 20s of traffic, well inside the interval
            send(delta(8, 1, "update", 99000L + i * 300L, 300L));
        }
        assertThat(controlCommands()).hasSize(1);
        assertThat(rejects()).hasSize(21);
    }

    @Test
    @DisplayName("control-plane: re-asking stops once a snapshot resolves the episode")
    void retryStopsAfterResolution() throws Exception {
        withRetryInterval(60_000L);
        send(delta(8, 1, "snapshot", 1000L, 300L));
        send(delta(8, 1, "update", 99000L, 300L)); // GAP
        assertThat(controlCommands()).hasSize(1);

        send(delta(8, 1, "snapshot", 99300L, 300L)); // the answer arrives
        // Long past several intervals, with the feed healthy again: nothing more is asked.
        harness.setProcessingTime(600_000L);
        send(delta(8, 1, "update", 99600L, 300L));
        harness.setProcessingTime(1_200_000L);
        send(delta(8, 1, "update", 99900L, 300L));
        assertThat(controlCommands()).hasSize(1);
    }

    @Test
    @DisplayName("control-plane: a no_baseline episode re-asks too, not just a gap")
    void noBaselineEpisodeIsRetried() throws Exception {
        // The cold-start case, and the one a job restart produces on every delta key at
        // once. lastSeq stays null, so every update rejects no_baseline and the episode
        // can only end when a snapshot finally lands.
        withRetryInterval(60_000L);
        send(delta(8, 1, "update", 1000L, 300L)); // no baseline -> one command at t=0
        assertThat(controlCommands()).hasSize(1);

        harness.setProcessingTime(60_000L);
        send(delta(8, 1, "update", 1300L, 300L));
        assertThat(controlCommands()).hasSize(2);

        send(delta(8, 1, "snapshot", 1600L, 300L)); // baseline at last
        harness.setProcessingTime(600_000L);
        send(delta(8, 1, "update", 1900L, 300L));
        assertThat(controlCommands()).hasSize(2);
        assertThat(controlCommands()).extracting(ControlCommand::getReason)
                .containsOnly(TypeValidateFunction.NO_BASELINE);
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.NO_BASELINE,
                        TypeValidateFunction.NO_BASELINE);
    }

    @Test
    @DisplayName("control-plane: overlapping episodes do not multiply the ask rate")
    void overlappingEpisodesDoNotMultiplyTheAskRate() throws Exception {
        // The defect the timer version had: it registered a chain per episode and
        // cancelled none, so two episodes inside one interval left two live chains and
        // the command rate doubled permanently. With one timestamp per key there is
        // nothing to stack — a second episode overwrites the first's clock.
        withRetryInterval(60_000L);
        send(delta(6, 1, "snapshot", 100L, 1L));
        send(delta(6, 1, "update", 500L, 1L));   // GAP #1 at t=0 -> command 1

        harness.setProcessingTime(10_000L);
        send(delta(6, 1, "snapshot", 600L, 1L)); // resolved, well inside the interval
        send(delta(6, 1, "update", 601L, 1L));

        harness.setProcessingTime(20_000L);
        send(delta(6, 1, "update", 900L, 1L));   // GAP #2 -> command 2, clock restarts here
        assertThat(controlCommands()).hasSize(2);

        // From here the ask rate must be ONE per interval, not one per episode ever
        // opened. t=70k is 50s after the second ask, so it is still suppressed.
        harness.setProcessingTime(70_000L);
        send(delta(6, 1, "update", 901L, 1L));
        assertThat(controlCommands()).hasSize(2);

        harness.setProcessingTime(80_000L); // now 60s after the ask at t=20k
        send(delta(6, 1, "update", 902L, 1L));
        assertThat(controlCommands()).hasSize(3);
    }

    @Test
    @DisplayName("control-plane: a re-ask targets the same market and carries its simulation flag")
    void retryCommandTargetsTheSameMarket() throws Exception {
        withRetryInterval(60_000L);
        RawOrderBookEvent snapshot = delta(6, 4, "snapshot", 10L, 1L);
        snapshot.setSimulation(1);
        RawOrderBookEvent gap = delta(6, 4, "update", 900L, 1L);
        gap.setSimulation(1);
        RawOrderBookEvent held = delta(6, 4, "update", 901L, 1L);
        held.setSimulation(1);
        send(snapshot);
        send(gap);
        harness.setProcessingTime(60_000L);
        send(held);

        assertThat(controlCommands()).hasSize(2);
        assertThat(controlCommands()).allSatisfy(c -> {
            assertThat(c.getExchangeId()).isEqualTo(6);
            assertThat(c.getPairId()).isEqualTo(4);
            assertThat(c.getSimulation()).isEqualTo(1);
            assertThat(c.getAction()).isEqualTo(ControlCommand.SNAPSHOT_REQUEST);
        });
    }

    /**
     * A retry is triggered by an update that is dead-lettered {@code
     * awaiting_snapshot}, but that is bookkeeping about a request we already
     * sent — it is not a reason to want a snapshot. The command has to keep
     * naming the condition the collector is being asked to fix, which is the
     * one that opened the episode.
     */
    @Test
    @DisplayName("control-plane: a re-ask keeps the reason that OPENED the episode")
    void retryKeepsTheEpisodeReason() throws Exception {
        withRetryInterval(60_000L);
        send(delta(8, 1, "snapshot", 1000L, 300L));
        send(delta(8, 1, "update", 99000L, 300L)); // GAP -> command at t=0
        harness.setProcessingTime(60_000L);
        send(delta(8, 1, "update", 99300L, 300L)); // awaiting_snapshot -> re-ask

        assertThat(controlCommands()).hasSize(2);
        assertThat(controlCommands()).extracting(ControlCommand::getReason)
                .containsOnly(TypeValidateFunction.SEQUENCE_GAP);
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.SEQUENCE_GAP,
                        TypeValidateFunction.AWAITING_SNAPSHOT);
    }

    @Test
    @DisplayName("control-plane: a healthy market never asks, however long it runs")
    void healthyMarketNeverRetries() throws Exception {
        withRetryInterval(60_000L);
        send(delta(8, 1, "snapshot", 1000L, 300L));
        send(delta(8, 1, "update", 1300L, 300L));
        harness.setProcessingTime(600_000L);
        send(delta(8, 1, "update", 1600L, 300L));
        harness.setProcessingTime(1_200_000L);
        send(delta(8, 1, "update", 1900L, 300L));
        assertThat(controlCommands()).isEmpty();
    }

    // ---- staleness: silence detection --------------------------------------------
    //
    // A market that stops sending is emptied and re-asked for. A market that has NEVER
    // sent anything is deliberately NOT job 2's business (see TypeValidateFunction.STALE):
    // it has no keyed state to watch, and the answer there is an alert from the staleness
    // exporter, not a snapshot request the collector cannot act on.
    //
    // Silence is measured in processing time; every test drives the clock by hand and the
    // harness fires the timers.
    private static final WatchedMarket EX6 = new WatchedMarket(6, 1, 60);
    private static final WatchedMarket EX8 = new WatchedMarket(8, 1, 60);

    @Test
    @DisplayName("staleness: a market that never sends anything is not watched at all")
    void neverReceivedIsNeverWatched() throws Exception {
        withRetryInterval(600_000L, EX6);

        // The market is subscribed and in the watch list, but has produced no event, so
        // it has no keyed state and no deadline. Deliberate: noticing it would mean
        // importing the whole roster into a stream validator, and a snapshot cannot fix
        // a subscription that was never wired up.
        harness.setProcessingTime(10_000_000L);

        assertThat(controlCommands()).isEmpty();
        assertThat(valid()).isEmpty();
        assertThat(rejects()).isEmpty();
    }

    @Test
    @DisplayName("staleness: a market that spoke and stopped asks with reason stale")
    void wentSilentAsksWithStaleReason() throws Exception {
        withRetryInterval(600_000L, EX6);
        send(delta(6, 1, "snapshot", 1000L, 1L));

        harness.setProcessingTime(30_000L);
        assertThat(controlCommands()).as("spoke at t=0, only 30s of silence").isEmpty();

        harness.setProcessingTime(61_000L);

        assertThat(controlCommands()).hasSize(1);
        ControlCommand command = controlCommands().get(0);
        assertThat(command.getAction()).isEqualTo(ControlCommand.SNAPSHOT_REQUEST);
        assertThat(command.getReason()).isEqualTo(TypeValidateFunction.STALE);
        assertThat(command.getExchangeId()).isEqualTo(6);
        assertThat(command.getPairId()).isEqualTo(1);
    }

    @Test
    @DisplayName("staleness: going silent empties the book with a reset, exactly once per episode")
    void wentSilentEmitsOneReset() throws Exception {
        withRetryInterval(60_000L, EX6);
        send(delta(6, 1, "snapshot", 1000L, 1L));

        harness.setProcessingTime(61_000L);
        harness.setProcessingTime(130_000L); // past the retry interval, so it re-asks
        harness.setProcessingTime(200_000L);

        List<RawOrderBookEvent> resets = valid().stream()
                .filter(e -> TypeValidateFunction.RESET.equals(e.getType()))
                .collect(Collectors.toList());
        assertThat(resets).as("the book is emptied once; re-asking does not re-empty it").hasSize(1);
        assertThat(controlCommands().size()).as("but the ask does repeat").isGreaterThan(1);
    }

    @Test
    @DisplayName("staleness: the silence reset carries no parent - nothing caused it but time")
    void silenceResetHasEmptyLineage() throws Exception {
        withRetryInterval(600_000L, EX6);
        send(delta(6, 1, "snapshot", 1000L, 1L));
        harness.setProcessingTime(61_000L);

        RawOrderBookEvent reset = valid().stream()
                .filter(e -> TypeValidateFunction.RESET.equals(e.getType()))
                .findFirst().orElseThrow();
        // Empty, never [""] - a blank id passes every "is the field set" check while
        // being untraceable, which is a bug the timer implementation actually shipped.
        assertThat(reset.getSourceIds()).isEmpty();
        assertThat(reset.getId()).isNotBlank();
        assertThat(controlCommands().get(0).getSourceIds()).isEmpty();
    }

    @Test
    @DisplayName("staleness: a silent SIMULATED market must not make the collector call a real exchange")
    void silenceCarriesTheSimulationFlagOfTheLastEvent() throws Exception {
        withRetryInterval(600_000L, new WatchedMarket(5, 1, 60));
        RawOrderBookEvent simulated = delta(5, 1, "snapshot", 1000L, 1L);
        simulated.setSimulation(1);
        send(simulated);

        harness.setProcessingTime(61_000L);

        assertThat(controlCommands().get(0).getSimulation()).isEqualTo(1);
        RawOrderBookEvent reset = valid().stream()
                .filter(e -> TypeValidateFunction.RESET.equals(e.getType()))
                .findFirst().orElseThrow();
        assertThat(reset.getSimulation()).isEqualTo(1);
    }

    @Test
    @DisplayName("staleness: data arriving moves the deadline, it does not just cancel it")
    void arrivalResetsTheClock() throws Exception {
        withRetryInterval(600_000L, EX6);
        send(delta(6, 1, "snapshot", 1000L, 1L)); // deadline t=60s

        harness.setProcessingTime(50_000L);
        send(delta(6, 1, "update", 1001L, 1L)); // spoke again, so the deadline is t=110s

        // The t=60s timer fires here and must re-arm rather than judge: 100s since the
        // job started, but only 50s since the market last spoke.
        harness.setProcessingTime(100_000L);
        assertThat(controlCommands()).isEmpty();

        // ...and the moved deadline still bites at the right moment.
        harness.setProcessingTime(111_000L);
        assertThat(controlCommands()).hasSize(1);
        assertThat(controlCommands().get(0).getReason()).isEqualTo(TypeValidateFunction.STALE);
    }

    @Test
    @DisplayName("staleness: a REJECTED event still counts as arriving - the feed is alive")
    void rejectedEventsCountAsArrival() throws Exception {
        withRetryInterval(600_000L, EX8);
        send(delta(8, 1, "snapshot", 1000L, 300L));
        harness.setProcessingTime(50_000L);
        send(delta(8, 1, "snapshot", 1L, 300L)); // stale_or_duplicate - rejected

        harness.setProcessingTime(100_000L);

        assertThat(rejects()).hasSize(1);
        // Silence means nothing ARRIVED. A key rejecting everything is alive and is
        // already re-asking on the rejection path; calling it stale too would be one
        // fault asking twice.
        assertThat(controlCommands()).isEmpty();
    }

    @Test
    @DisplayName("staleness: each market's own threshold is used, not a shared one")
    void thresholdIsPerMarket() throws Exception {
        withRetryInterval(600_000L, new WatchedMarket(6, 1, 30), new WatchedMarket(8, 1, 300));
        send(delta(6, 1, "snapshot", 1000L, 1L));
        send(delta(8, 1, "snapshot", 1000L, 300L));

        harness.setProcessingTime(60_000L); // 60s of silence: past ex6's 30, inside the 300s threshold

        assertThat(controlCommands()).hasSize(1);
        assertThat(controlCommands().get(0).getExchangeId()).isEqualTo(6);
    }

    @Test
    @DisplayName("staleness: a market absent from the watch list is never judged silent")
    void unwatchedMarketIsNeverStale() throws Exception {
        withRetryInterval(600_000L, EX6); // ex8 is NOT watched
        send(delta(8, 1, "snapshot", 1000L, 300L));

        harness.setProcessingTime(10_000_000L);

        // Unsubscribing a market must stop the asking without a resubmit, so an absent
        // row means no timer at all rather than a default threshold.
        assertThat(controlCommands()).isEmpty();
    }

    @Test
    @DisplayName("unsubscribe: the book is emptied, so no consumer is left holding a phantom")
    void unsubscribeEmptiesTheBook() throws Exception {
        Map<String, WatchedMarket> roster = new HashMap<>();
        roster.put(EX6.key(), EX6);
        harness.close();
        harness = openHarness(new TypeValidateFunction(600_000L,
                new RefreshingLookup<>(() -> roster, Long.MAX_VALUE)));
        harness.setProcessingTime(0L);
        send(delta(6, 1, "snapshot", 1000L, 1L)); // a real book exists downstream now

        roster.remove(EX6.key()); // operator unsubscribes it
        harness.setProcessingTime(61_000L); // the outstanding deadline fires

        List<RawOrderBookEvent> resets = valid().stream()
                .filter(e -> TypeValidateFunction.RESET.equals(e.getType()))
                .collect(Collectors.toList());
        assertThat(resets).as("the book must be emptied, not abandoned").hasSize(1);
        assertThat(resets.get(0).getExchangeId()).isEqualTo(6);
        assertThat(resets.get(0).getPairId()).isEqualTo(1);
        assertThat(resets.get(0).getSourceIds()).isEmpty();

        // We are dropping the market, not recovering it. Asking would tell NiFi to
        // reopen the very feed the operator just closed.
        assertThat(controlCommands()).as("no request on an unsubscribe").isEmpty();
    }

    @Test
    @DisplayName("unsubscribe: the book is emptied exactly once, never once per deadline")
    void unsubscribeResetFiresOnce() throws Exception {
        Map<String, WatchedMarket> roster = new HashMap<>();
        roster.put(EX6.key(), EX6);
        harness.close();
        harness = openHarness(new TypeValidateFunction(600_000L,
                new RefreshingLookup<>(() -> roster, Long.MAX_VALUE)));
        harness.setProcessingTime(0L);
        send(delta(6, 1, "snapshot", 1000L, 1L));

        roster.remove(EX6.key());
        harness.setProcessingTime(61_000L);
        harness.setProcessingTime(10_000_000L); // long past many further deadlines

        assertThat(valid().stream()
                .filter(e -> TypeValidateFunction.RESET.equals(e.getType()))
                .count()).isEqualTo(1L);
        assertThat(harness.numProcessingTimeTimers())
                .as("nothing re-armed: the key is not watched any more").isZero();
    }

    @Test
    @DisplayName("unsubscribe: a market that never sent anything empties nothing")
    void unsubscribeOfSilentMarketEmitsNoReset() throws Exception {
        withRetryInterval(600_000L, EX6); // ex8 never watched, never fed

        harness.setProcessingTime(10_000_000L);

        // No book was ever built for ex8, so there is nothing to empty. Enforced a step
        // earlier than emitUnsubscribeReset: no event means no timer, so the unsubscribe
        // branch is never reached at all.
        assertThat(valid()).isEmpty();
        assertThat(controlCommands()).isEmpty();
    }

    @Test
    @DisplayName("staleness: a market unsubscribed and resubscribed mid-flight is watched again")
    void resubscribedMarketIsWatchedAgain() throws Exception {
        // A mutable roster, because this is exactly what RefreshingLookup exists for:
        // the watch list changes while the job runs.
        Map<String, WatchedMarket> roster = new HashMap<>();
        roster.put(EX6.key(), EX6);
        harness.close();
        harness = openHarness(new TypeValidateFunction(600_000L,
                new RefreshingLookup<>(() -> roster, Long.MAX_VALUE)));
        harness.setProcessingTime(0L);

        send(delta(6, 1, "snapshot", 1000L, 1L)); // deadline at t=60s

        roster.remove(EX6.key()); // unsubscribed while that deadline is in flight
        harness.setProcessingTime(61_000L); // it fires, finds no row, stops watching
        assertThat(controlCommands()).as("not watched any more").isEmpty();

        roster.put(EX6.key(), EX6); // subscribed again
        send(delta(6, 1, "update", 1001L, 1L));

        // The spent timer must have been cleared when it fired, or nothing can ever arm
        // a new one for this key and the market stays unwatched until the job restarts.
        harness.setProcessingTime(200_000L);
        assertThat(controlCommands()).hasSize(1);
        assertThat(controlCommands().get(0).getReason()).isEqualTo(TypeValidateFunction.STALE);
    }

    @Test
    @DisplayName("staleness: silence and rejection share ONE suppression window, never two")
    void silenceAndRejectionDoNotDoubleAsk() throws Exception {
        withRetryInterval(600_000L, EX8);
        // no_baseline: an update with no snapshot ever - asks on the rejection path.
        send(delta(8, 1, "update", 1000L, 300L));
        assertThat(controlCommands()).hasSize(1);

        // Now let it go silent too. Both conditions hold, but the interval has not
        // passed, so the market must not ask twice for the same thing.
        harness.setProcessingTime(61_000L);

        assertThat(controlCommands()).hasSize(1);
    }

    @Test
    @DisplayName("staleness: a busy market holds exactly ONE timer, not one per event")
    void trafficDoesNotMultiplyTheTimerChain() throws Exception {
        withRetryInterval(600_000L, EX6);
        send(delta(6, 1, "snapshot", 1000L, 1L));
        for (int i = 1; i <= 20; i++) {
            harness.setProcessingTime(i * 1_000L);
            send(delta(6, 1, "update", 1000L + i, 1L));
        }

        // Asserted on the timer state itself, deliberately. The OUTPUT cannot see this:
        // duplicate chains are absorbed by the once-per-episode reset guard and the
        // shared ask window, so 21 chains and 1 chain emit the same records. What they
        // do not share is state - 21 chains is an unbounded leak that grows with
        // traffic, and re-arming keeps every one of them alive. This is the defect that
        // sank the first timer-based control plane; it is only visible here.
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);

        // ...and it still survives firing: one in, one out, never a fan-out.
        harness.setProcessingTime(200_000L);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);
    }

    @Test
    @DisplayName("staleness: a feed that resumes after a silence reset is accepted, not rejected")
    void resumingAfterSilenceIsNotRejectedAsOutOfOrder() throws Exception {
        withRetryInterval(600_000L, new WatchedMarket(3, 1, 60));
        send(nullSeqSnapshot(3, 1, 5_000L)); // exchange clock is nowhere near wall clock

        harness.setProcessingTime(61_000L); // silence: asks, and empties the book with a reset

        // The feed comes back on its own clock, far "older" than the wall-clock instant
        // the reset was stamped with. It must be accepted: a silence episode is a resync
        // like any other, so the ordering guards are suspended until it is answered.
        // Without that exemption the key would reject every returning event forever and
        // the market could never recover - the 2026-08-19 deadlock, reached from a new
        // direction.
        send(nullSeqSnapshot(3, 1, 2_000L)); // BEHIND the last accepted event time

        assertThat(rejects()).isEmpty();
        assertThat(validBusiness()).hasSize(2);
    }

}
