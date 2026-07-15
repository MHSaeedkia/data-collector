package io.tibobit.normalizer.typevalidate;

import io.tibobit.normalizer.model.RawOrderBookEvent;
import io.tibobit.normalizer.model.RejectedOrderBookEvent;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Job 2 — type validation, keyed by {@code (exchange_id, pair_id)}. Applies the sequence rules
 * decided in memory/project_raw_pipeline_decision.md; valid events go to the main output, rejects
 * to the {@link #REJECTED} side output (dead-letter). Two rule kinds, selected by what the parser
 * stamped on the event (see [[pair-extractor]]):
 *
 * <ul>
 *   <li><b>No ordering field</b> ({@code sequence_id == null}, ex3 wallex only): pass through
 *       unchecked — nothing on the wire to order by.</li>
 *   <li><b>Snapshot</b> ({@code type == "snapshot"}): a fresh baseline, but out-of-order/duplicate
 *       dropped — reject {@code stale_or_duplicate} if {@code sequence_id <= lastSeq}. Otherwise it
 *       re-syncs the book: store {@code lastSeq}, clear {@code awaitingSnapshot}.</li>
 *   <li><b>Update</b> ({@code type == "update"}, delta feeds ex6/ex8): needs a baseline and a
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

    static final String STALE_OR_DUPLICATE = "stale_or_duplicate";
    static final String SEQUENCE_GAP = "sequence_gap";
    static final String AWAITING_SNAPSHOT = "awaiting_snapshot";
    static final String NO_BASELINE = "no_baseline";

    private transient ValueState<Long> lastSeq;
    private transient ValueState<Boolean> awaitingSnapshot;

    @Override
    public void open(OpenContext openContext) {
        lastSeq = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastSeq", Long.class));
        awaitingSnapshot = getRuntimeContext().getState(
                new ValueStateDescriptor<>("awaitingSnapshot", Boolean.class));
    }

    @Override
    public void processElement(RawOrderBookEvent event, Context ctx,
                               Collector<RawOrderBookEvent> out) throws Exception {
        event.getPipelineTimings().setTypeValidateIn(System.currentTimeMillis());

        // ex3 wallex: no ordering field at all — nothing to validate, pass through.
        if (event.getSequenceId() == null) {
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
            emit(event, out);
            return;
        }

        // update (delta feeds only)
        if (last == null) {
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
            reject(event, SEQUENCE_GAP, ctx);
        }
    }

    private void emit(RawOrderBookEvent event, Collector<RawOrderBookEvent> out) {
        event.getPipelineTimings().setTypeValidateOut(System.currentTimeMillis());
        out.collect(event);
    }

    private void reject(RawOrderBookEvent event, String reason, Context ctx) {
        // typeValidateIn is already stamped; leave typeValidateOut null — the event never leaves
        // the validator onto the main stream. rejectedAt records when it was dead-lettered.
        ctx.output(REJECTED, new RejectedOrderBookEvent(event, reason, System.currentTimeMillis()));
    }
}
