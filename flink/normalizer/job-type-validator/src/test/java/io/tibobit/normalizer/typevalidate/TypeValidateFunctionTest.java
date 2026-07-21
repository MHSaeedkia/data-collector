package io.tibobit.normalizer.typevalidate;

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
 * Tests {@link TypeValidateFunction} against its documented sequence rules, driven through Flink's
 * {@link KeyedOneInputStreamOperatorTestHarness} keyed exactly as the job — {@code (exchange_id,
 * pair_id)} — so real keyed ValueState and the {@code open(OpenContext)} lifecycle run, not a mock.
 */
class TypeValidateFunctionTest {

    private KeyedOneInputStreamOperatorTestHarness<String, RawOrderBookEvent, RawOrderBookEvent> harness;

    @BeforeEach
    void openHarness() throws Exception {
        KeyedProcessOperator<String, RawOrderBookEvent, RawOrderBookEvent> operator =
                new KeyedProcessOperator<>(new TypeValidateFunction());
        KeySelector<RawOrderBookEvent, String> byKey =
                e -> e.getExchangeId() + "|" + e.getPairId();
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
     * Null-seq snapshot (ex3 wallex / ex1 nobitex REST) with an explicit event time — the field the
     * out-of-order guard orders these by, since they carry no sequence id.
     */
    private static RawOrderBookEvent nullSeqSnapshot(int ex, int pair, long eventTime) {
        return new RawOrderBookEvent(ex, pair, "snapshot", null, 0L, eventTime, List.of(), List.of());
    }

    /** Delta-feed message (snapshot or update) with a nonzero jump (ex6=1, ex8=300). */
    private static RawOrderBookEvent delta(int ex, int pair, String type, long seq, long jump) {
        return new RawOrderBookEvent(ex, pair, type, seq, jump, seq, List.of(), List.of());
    }

    private void send(RawOrderBookEvent e) throws Exception {
        harness.processElement(new StreamRecord<>(e));
    }

    private List<RawOrderBookEvent> valid() {
        return harness.extractOutputValues();
    }

    private List<RejectedOrderBookEvent> rejects() {
        ConcurrentLinkedQueue<StreamRecord<RejectedOrderBookEvent>> q =
                harness.getSideOutput(TypeValidateFunction.REJECTED);
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
        send(snapshotFeed(1, 1, 99L));  // out of order
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
        send(delta(6, 1, "update", 8L, 1L));  // older
        assertThat(valid()).hasSize(2);
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.STALE_OR_DUPLICATE,
                        TypeValidateFunction.STALE_OR_DUPLICATE);
    }

    @Test
    @DisplayName("delta feed: a gap rejects sequence_gap then holds every update as awaiting_snapshot until a snapshot re-syncs")
    void gapThenAwaitingSnapshotUntilResync() throws Exception {
        send(delta(6, 1, "snapshot", 10L, 1L));
        send(delta(6, 1, "update", 11L, 1L));   // ok
        send(delta(6, 1, "update", 15L, 1L));   // gap (expected 12) -> sequence_gap
        send(delta(6, 1, "update", 16L, 1L));   // still awaiting -> awaiting_snapshot
        send(delta(6, 1, "snapshot", 20L, 1L)); // re-sync -> accepted, clears awaiting
        send(delta(6, 1, "update", 21L, 1L));   // contiguous again -> ok

        assertThat(valid()).extracting(RawOrderBookEvent::getSequenceId)
                .containsExactly(10L, 11L, 20L, 21L);
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.SEQUENCE_GAP,
                        TypeValidateFunction.AWAITING_SNAPSHOT);
    }

    // ---- ex1 nobitex: null-seq REST snapshot resyncs the WS delta stream ---------

    @Test
    @DisplayName("ex1: the first update after a null-seq REST snapshot adopts its offset as the baseline, then gaps are enforced")
    void restSnapshotResyncsThenGapChecks() throws Exception {
        send(nullSeqSnapshot(1, 1, 499L));      // REST snapshot: no offset, flags a resync
        send(delta(1, 1, "update", 500L, 1L)); // first WS delta -> adopts 500 as baseline
        send(delta(1, 1, "update", 501L, 1L)); // contiguous -> ok
        send(delta(1, 1, "update", 505L, 1L)); // gap (expected 502) -> sequence_gap

        assertThat(valid()).extracting(RawOrderBookEvent::getSequenceId)
                .containsExactly(null, 500L, 501L);
        assertThat(rejects()).extracting(RejectedOrderBookEvent::getRejectReason)
                .containsExactly(TypeValidateFunction.SEQUENCE_GAP);
    }

    @Test
    @DisplayName("ex1: a later REST snapshot re-anchors the baseline unconditionally, recovering from awaiting_snapshot")
    void laterRestSnapshotReanchors() throws Exception {
        send(nullSeqSnapshot(1, 1, 499L));      // resync
        send(delta(1, 1, "update", 500L, 1L)); // baseline 500
        send(delta(1, 1, "update", 505L, 1L)); // gap -> sequence_gap, awaiting
        send(delta(1, 1, "update", 506L, 1L)); // still awaiting -> awaiting_snapshot
        send(nullSeqSnapshot(1, 1, 899L));      // REST re-snapshot (newer) -> clears awaiting, flags resync
        send(delta(1, 1, "update", 900L, 1L)); // adopts 900 unconditionally
        send(delta(1, 1, "update", 901L, 1L)); // contiguous -> ok

        assertThat(valid()).extracting(RawOrderBookEvent::getSequenceId)
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
        send(nullSeqSnapshot(1, 1, 499L));      // resync, event time 499
        send(delta(1, 1, "update", 500L, 1L)); // adopts 500 as baseline (event time 500)
        send(delta(1, 1, "update", 501L, 1L)); // contiguous -> ok (event time 501)
        send(nullSeqSnapshot(1, 1, 499L));      // OLD snapshot replayed: 499 < 501 -> out_of_order
        send(delta(1, 1, "update", 600L, 1L)); // if the stale snapshot had wrongly re-armed the
                                               // resync this would be ADOPTED; instead it is a gap

        assertThat(valid()).extracting(RawOrderBookEvent::getSequenceId)
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
        send(snapshotFeed(1, 1, 99L));  // rejected

        RawOrderBookEvent ok = valid().get(0);
        assertThat(ok.getPipelineTimings().getTypeValidateIn()).isNotNull();
        assertThat(ok.getPipelineTimings().getTypeValidateOut()).isNotNull();

        RawOrderBookEvent bad = rejects().get(0).getEvent();
        assertThat(bad.getPipelineTimings().getTypeValidateIn()).isNotNull();
        assertThat(bad.getPipelineTimings().getTypeValidateOut()).isNull();
    }
}
