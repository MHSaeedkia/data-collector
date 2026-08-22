package io.tibobit.normalizer.typevalidate;

import io.tibobit.normalizer.model.ControlCommand;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import io.tibobit.normalizer.model.RejectedOrderBookEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link TypeValidateFunction} against its documented sequence rules,
 * driven through Flink's
 * {@link KeyedOneInputStreamOperatorTestHarness} keyed exactly as the job —
 * {@code (exchange_id,
 * pair_id)} — so real keyed ValueState and the {@code open(OpenContext)}
 * lifecycle run, not a mock.
 */
class TypeValidateFunctionTest {

    private KeyedOneInputStreamOperatorTestHarness<String, RawOrderBookEvent, RawOrderBookEvent> harness;

    @BeforeEach
    void openHarness() throws Exception {
        KeyedProcessOperator<String, RawOrderBookEvent, RawOrderBookEvent> operator = new KeyedProcessOperator<>(
                new TypeValidateFunction());
        KeySelector<RawOrderBookEvent, String> byKey = e -> e.getExchangeId() + "|" + e.getPairId();
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                operator, byKey, TypeInformation.of(String.class));
        harness.open();
    }

    @AfterEach
    void closeHarness() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    // ---- helpers ----------------------------------------------------------------

    /** Snapshot-feed event (jump 0) for ex/pair with the given ordering value. */
    private static RawOrderBookEvent snapshotFeed(int ex, int pair, Long seq) {
        return new RawOrderBookEvent(ex, pair, "snapshot", seq, 0L, seq == null ? 0L : seq,
                List.of(), List.of());
    }

    /**
     * Null-seq snapshot (ex3 wallex / ex1 nobitex REST) with an explicit event time
     * — the field the
     * out-of-order guard orders these by, since they carry no sequence id.
     */
    private static RawOrderBookEvent nullSeqSnapshot(int ex, int pair, long eventTime) {
        return new RawOrderBookEvent(ex, pair, "snapshot", null, 0L, eventTime, List.of(), List.of());
    }

    /**
     * Delta-feed message (snapshot or update) with a nonzero jump (ex6=1, ex8=300).
     */
    private static RawOrderBookEvent delta(int ex, int pair, String type, long seq, long jump) {
        return new RawOrderBookEvent(ex, pair, type, seq, jump, seq, List.of(), List.of());
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
    @DisplayName("delta feed (ex8, jump 300): update at last + 300 is accepted")
    void contiguousUpdateAcceptedJump300() throws Exception {
        send(delta(8, 1, "snapshot", 1000L, 300L));
        send(delta(8, 1, "update", 1300L, 300L));
        assertThat(valid()).hasSize(2);
        assertThat(rejects()).isEmpty();
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

    // ---- ex1 nobitex: null-seq REST snapshot resyncs the WS delta stream
    // ---------

    @Test
    @DisplayName("ex1: the first update after a null-seq REST snapshot adopts its offset as the baseline, then gaps are enforced")
    void restSnapshotResyncsThenGapChecks() throws Exception {
        send(nullSeqSnapshot(1, 1, 499L)); // REST snapshot: no offset, flags a resync
        send(delta(1, 1, "update", 500L, 1L)); // first WS delta -> adopts 500 as baseline
        send(delta(1, 1, "update", 501L, 1L)); // contiguous -> ok
        send(delta(1, 1, "update", 505L, 1L)); // gap (expected 502) -> sequence_gap

        assertThat(validBusiness()).extracting(RawOrderBookEvent::getSequenceId)
                .containsExactly(null, 500L, 501L);
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.SEQUENCE_GAP);
    }

    @Test
    @DisplayName("ex1: a later REST snapshot re-anchors the baseline unconditionally, recovering from awaiting_snapshot")
    void laterRestSnapshotReanchors() throws Exception {
        send(nullSeqSnapshot(1, 1, 499L)); // resync
        send(delta(1, 1, "update", 500L, 1L)); // baseline 500
        send(delta(1, 1, "update", 505L, 1L)); // gap -> sequence_gap, awaiting
        send(delta(1, 1, "update", 506L, 1L)); // still awaiting -> awaiting_snapshot
        send(nullSeqSnapshot(1, 1, 899L)); // REST re-snapshot (newer) -> clears awaiting, flags resync
        send(delta(1, 1, "update", 900L, 1L)); // adopts 900 unconditionally
        send(delta(1, 1, "update", 901L, 1L)); // contiguous -> ok

        assertThat(validBusiness()).extracting(RawOrderBookEvent::getSequenceId)
                .containsExactly(null, 500L, null, 900L, 901L);
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.SEQUENCE_GAP,
                        TypeValidateFunction.AWAITING_SNAPSHOT);
    }

    @Test
    @DisplayName("ex1: an update before any REST snapshot still rejects no_baseline (resync flag gates the bootstrap)")
    void updateBeforeAnyRestSnapshotRejected() throws Exception {
        send(delta(1, 1, "update", 500L, 1L));
        assertThat(valid()).isEmpty();
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.NO_BASELINE);
    }

    @Test
    @DisplayName("ex1: an OLD REST snapshot replayed after newer WS deltas is rejected out_of_order and does NOT re-arm the resync")
    void staleRestSnapshotAfterUpdatesRejected() throws Exception {
        send(nullSeqSnapshot(1, 1, 499L)); // resync, event time 499
        send(delta(1, 1, "update", 500L, 1L)); // adopts 500 as baseline (event time 500)
        send(delta(1, 1, "update", 501L, 1L)); // contiguous -> ok (event time 501)
        send(nullSeqSnapshot(1, 1, 499L)); // OLD snapshot replayed: 499 < 501 -> out_of_order
        send(delta(1, 1, "update", 600L, 1L)); // if the stale snapshot had wrongly re-armed the
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

    /** Every incoming event arrives with the id job 1 gave it. */
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
     * names the rejected
     * event as its parent. The nested event keeps the id it came in with — it is
     * being reported on,
     * not forwarded, and that id is the link back to the raw stream.
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
     * stream and a
     * dead-letter record — and both name it as their parent while carrying distinct
     * ids of their
     * own. A fan-out of lineage, not a fan-in.
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
            assertThat(cmd.getExchangeId()).isEqualTo(6);
            assertThat(cmd.getPairId()).isEqualTo(1);
        });
    }

    /**
     * The command is a write to a topic, so its lineage is DERIVED like the reset
     * marker's and the dead-letter's — not inherited from the gap event. Inheriting
     * looks fine (both fields hold well-formed values) but reuses an id that is
     * already carried inside the dead-letter envelope, and points one hop too far
     * back, so the request cannot be traced to the event that caused it.
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
     * Without this, a gap in simulated data would ask NiFi for a real snapshot from
     * a real exchange — and the e2e suite feeds nothing but {@code simulation: 1}.
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
        send(delta(1, 1, "update", 500L, 1L)); // no baseline (ex1 before its REST snapshot) -> request #1
        send(nullSeqSnapshot(1, 1, 499L)); // REST snapshot arrives, flags a resync
        send(delta(1, 1, "update", 900L, 1L)); // adopts baseline unconditionally, clears request flag
        send(delta(1, 1, "update", 950L, 1L)); // a fresh gap -> request #2

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
    @DisplayName("control-plane: a resync snapshot older than the last delta is ACCEPTED, not deadlocked (ex1/ex2)")
    void resyncSnapshotWithLaggingClockIsAccepted() throws Exception {
        // ex1 shape: REST snapshot seeds the resync, first WS delta adopts the baseline.
        send(nullSeqSnapshot(1, 1, 1_000L));
        send(delta(1, 1, "update", 10L, 1L));   // adopts baseline, lastEventTime = 10
        send(delta(1, 1, "update", 11L, 1L));   // contiguous, lastEventTime = 11
        send(delta(1, 1, "update", 99L, 1L));   // GAP -> one command, book emptied by the reset
        assertThat(controlCommands()).hasSize(1);

        // NiFi answers with a REST snapshot whose event_time trails the last accepted delta —
        // different clock, not an old book. Before the fix this rejected out_of_order, and since
        // lastEventTime only advances in emit() the key could never recover or re-ask.
        send(nullSeqSnapshot(1, 1, 5L));
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.SEQUENCE_GAP);

        // and the resync really resolved: the next gap opens a NEW episode and asks again.
        send(delta(1, 1, "update", 200L, 1L));  // adopts the fresh baseline
        send(delta(1, 1, "update", 500L, 1L));  // GAP #2
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

    /** Reopens the harness with a short retry interval, clock at zero. */
    private void withRetryInterval(long ms) throws Exception {
        harness.close();
        KeyedProcessOperator<String, RawOrderBookEvent, RawOrderBookEvent> operator =
                new KeyedProcessOperator<>(new TypeValidateFunction(ms));
        KeySelector<RawOrderBookEvent, String> byKey = e -> e.getExchangeId() + "|" + e.getPairId();
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                operator, byKey, TypeInformation.of(String.class));
        harness.open();
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
}
