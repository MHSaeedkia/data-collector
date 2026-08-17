package io.tibobit.normalizer.typevalidate;

import io.tibobit.normalizer.model.ControlCommand;
import io.tibobit.normalizer.model.Lineage;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import io.tibobit.normalizer.model.RejectedOrderBookEvent;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;

/**
 * Job 2 — type validation, keyed by {@code (exchange_id, pair_id)}. Applies the sequence rules
 * decided in memory/project_raw_pipeline_decision.md; valid events go to the main output, rejects
 * to the {@link #REJECTED} side output (dead-letter). Two rule kinds, selected by what the parser
 * stamped on the event (see [[pair-extractor]]):
 *
 * <ul>
 *   <li><b>No ordering field</b> ({@code sequence_id == null}): no sequence to order by, so
 *       out-of-order is detected by <b>event time</b> instead — a null-seq snapshot older than the
 *       last accepted event is rejected {@code out_of_order} (an old ex1 REST snapshot replayed
 *       after newer WS deltas must not overwrite the newer book). Two exchanges land here: ex3
 *       wallex (never sends updates) and the ex1 nobitex REST snapshot (a resync whose Centrifugo
 *       offset is unknown). An accepted one sets {@code baselinePending} so that the next update on
 *       the key adopts its offset as a fresh baseline (see [[pair-extractor]]); for ex3 that flag is
 *       never consumed.</li>
 *   <li><b>Snapshot</b> ({@code type == "snapshot"}): a fresh baseline, but out-of-order/duplicate
 *       dropped — reject {@code stale_or_duplicate} if {@code sequence_id <= lastSeq}. Otherwise it
 *       re-syncs the book: store {@code lastSeq}, clear {@code awaitingSnapshot}.</li>
 *   <li><b>Update</b> ({@code type == "update"}, delta feeds ex1/ex6/ex8): needs a baseline and a
 *       contiguous sequence. No baseline yet → {@code no_baseline}; still waiting to re-sync after
 *       a gap → {@code awaiting_snapshot}; {@code sequence_id == lastSeq + sequence_jump} → valid;
 *       {@code sequence_id <= lastSeq} → {@code stale_or_duplicate}; any other forward jump is a
 *       gap → {@code sequence_gap} + set {@code awaitingSnapshot} (every update rejected until the
 *       next snapshot re-syncs).</li>
 * </ul>
 *
 * State per key: {@code lastSeq} (last accepted sequence id) and {@code awaitingSnapshot}. Topics
 * are single-partition so per-key order holds; no checkpointing configured (cold-start gap shared
 * with the rest of the platform — book is unvalidated until the first snapshot after a restart).
 */
public class TypeValidateFunction
        extends KeyedProcessFunction<String, RawOrderBookEvent, RawOrderBookEvent> {

    /** Dead-letter side output. Shared by the job wiring and the tests. */
    public static final OutputTag<RejectedOrderBookEvent> REJECTED =
            new OutputTag<>("rejected") {};

     /** Control-plane side output: snapshot_request commands routed to NiFi. NEW. */
    public static final OutputTag<ControlCommand> CONTROL =
            new OutputTag<>("control") {};

    static final String STALE_OR_DUPLICATE = "stale_or_duplicate";
    static final String SEQUENCE_GAP = "sequence_gap";
    static final String AWAITING_SNAPSHOT = "awaiting_snapshot";
    static final String NO_BASELINE = "no_baseline";
    static final String OUT_OF_ORDER = "out_of_order";

    /**
     * Event {@code type} of the synthetic reset marker emitted onto the main stream on a gap. Job 5
     * turns it into an emptied book so the exchange drops out of the aggregated view instead of
     * serving its pre-gap diverged book.
     */
    static final String RESET = "reset";

    private transient ValueState<Long> lastSeq;
    private transient ValueState<Boolean> awaitingSnapshot;
    private transient ValueState<Boolean> baselinePending;
    private transient ValueState<Long> lastEventTime;

    // NEW: guards against re-sending snapshot_request on every rejected event while we
    // wait for the snapshot that resolves the gap/no-baseline condition.
    private transient ValueState<Boolean> snapshotRequested;

    @Override
    public void open(OpenContext openContext) {
        lastSeq = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastSeq", Long.class));
        awaitingSnapshot = getRuntimeContext().getState(
                new ValueStateDescriptor<>("awaitingSnapshot", Boolean.class));
        baselinePending = getRuntimeContext().getState(
                new ValueStateDescriptor<>("baselinePending", Boolean.class));
        lastEventTime = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastEventTime", Long.class));
         snapshotRequested = getRuntimeContext().getState(   // NEW
                new ValueStateDescriptor<>("snapshotRequested", Boolean.class));
    }

    @Override
    public void processElement(RawOrderBookEvent event, Context ctx,
                               Collector<RawOrderBookEvent> out) throws Exception {
        event.getPipelineTimings().setTypeValidateIn(System.currentTimeMillis());

        // No ordering field (ex3 wallex; ex1 nobitex REST snapshot): there is no sequence to order
        // by, so detect out-of-order by EVENT TIME instead. A re-sent older snapshot (e.g. ex1
        // REST replays an old book after newer WS deltas) must be rejected — otherwise it overwrites
        // a newer book and wrongly re-arms the resync. Only once accepted does it flag the key so the
        // next update adopts its offset as a fresh baseline (ex3 never sends updates, so its flag is
        // set but never consumed).
        if (event.getSequenceId() == null) {
            Long lastEt = lastEventTime.value();
            if (lastEt != null && event.getEventTime() < lastEt) {
                reject(event, OUT_OF_ORDER, ctx);
                return;
            }
            baselinePending.update(true);
            awaitingSnapshot.update(false);
            snapshotRequested.update(false);   // NEW: this resync satisfies any pending request
            emit(event, out);
            return;
        }

        long seq = event.getSequenceId();
        Long last = lastSeq.value();

        if ("snapshot".equals(event.getType())) {
            if (last != null && seq <= last) {
                reject(event, STALE_OR_DUPLICATE, ctx);
                return;
            }
            lastSeq.update(seq);
            awaitingSnapshot.update(false);
            snapshotRequested.update(false);   // NEW: the requested snapshot has arrived
            emit(event, out);
            return;
        }

        // update (delta feeds only)
        if (Boolean.TRUE.equals(baselinePending.value())) {
            // First update after a null-seq snapshot (ex1 REST resync): adopt its offset as the
            // baseline unconditionally, then resume contiguity checks from there.
            lastSeq.update(seq);
            baselinePending.update(false);
            snapshotRequested.update(false);   // NEW: baseline established, any request is moot
            emit(event, out);
            return;
        }
        if (last == null) {
            requestSnapshotOnce(event, ctx);   // NEW
            reject(event, NO_BASELINE, ctx);
            return;
        }
        if (Boolean.TRUE.equals(awaitingSnapshot.value())) {
            reject(event, AWAITING_SNAPSHOT, ctx);
            return;
        }
        if (seq == last + event.getSequenceJump()) {
            lastSeq.update(seq);
            emit(event, out);
        } else if (seq <= last) {
            reject(event, STALE_OR_DUPLICATE, ctx);
        } else {
            awaitingSnapshot.update(true);
            requestSnapshotOnce(event, ctx);   // NEW
            emitReset(event, out);
            reject(event, SEQUENCE_GAP, ctx);
        }
    }

    /**
     * Emits a snapshot_request control command exactly once per gap/no-baseline episode — a
     * second call before the flag is cleared is a no-op, so NiFi doesn't get flooded with
     * duplicate requests while every subsequent update keeps rejecting on the same condition.
     */
    private void requestSnapshotOnce(RawOrderBookEvent event, Context ctx) throws Exception {
        if (Boolean.TRUE.equals(snapshotRequested.value())) {
            return;
        }
        snapshotRequested.update(true);
        ctx.output(CONTROL, new ControlCommand(
                ControlCommand.SNAPSHOT_REQUEST, event.getExchangeId(), event.getPairId(),event.getSimulation(),event.getId(),event.getSourceIds()));
    }

    /**
     * Emits a synthetic {@link #RESET} marker on the main stream carrying the gap event's identity
     * and event time but no book (null sides, null sequence). Reached only on the not-awaiting →
     * awaiting transition, so it fires exactly once per gap episode — subsequent updates return at
     * the {@code awaitingSnapshot} reject above. A fresh {@link
     * io.tibobit.normalizer.model.PipelineTimings} (not the gap event's) is used so stamping
     * {@code type_validate_out} here does not leak onto the event that is still being dead-lettered.
     *
     * <p>{@code simulation} IS inherited from the gap event: the marker stands in for that
     * exchange's stream, so an emptied simulation book must not come back out flagged as live.
     *
     * <p>Lineage is NOT inherited — it is derived. The marker is a new record caused by the gap
     * event, so the gap event is its source and the marker gets an id of its own. The gap event
     * is simultaneously dead-lettered, so its one id legitimately appears as the parent of two
     * different records.
     */
    private void emitReset(RawOrderBookEvent gap, Collector<RawOrderBookEvent> out) {
        RawOrderBookEvent reset = new RawOrderBookEvent(
                gap.getExchangeId(), gap.getPairId(), RESET, null, 0L, gap.getEventTime(),
                null, null);
        reset.setSimulation(gap.getSimulation());
        reset.setSourceIds(List.of(gap.getId()));
        reset.setId(Lineage.newId());
        reset.getPipelineTimings().setTypeValidateIn(gap.getPipelineTimings().getTypeValidateIn());
        reset.getPipelineTimings().setTypeValidateOut(System.currentTimeMillis());
        out.collect(reset);
    }

    private void emit(RawOrderBookEvent event, Collector<RawOrderBookEvent> out) throws Exception {
        // Track the event time of the last accepted event so a later null-seq snapshot can be
        // ordered against it (the out-of-order guard above).
        lastEventTime.update(event.getEventTime());
        Lineage.restamp(event);
        event.getPipelineTimings().setTypeValidateOut(System.currentTimeMillis());
        out.collect(event);
    }

    private void reject(RawOrderBookEvent event, String reason, Context ctx) {
        // typeValidateIn is already stamped; leave typeValidateOut null — the event never leaves
        // the validator onto the main stream. rejectedAt records when it was dead-lettered.
        //
        // The envelope gets its own id: the dead-letter topic is a topic. The event inside
        // keeps the id job 1 gave it — deliberately NOT restamped, since it is being recorded, not
        // forwarded, and that id is what links this record back to the raw stream.
        RejectedOrderBookEvent rejection =
                new RejectedOrderBookEvent(event, reason, System.currentTimeMillis());
        rejection.setSourceIds(List.of(event.getId()));
        rejection.setId(Lineage.newId());
        ctx.output(REJECTED, rejection);
    }
}
