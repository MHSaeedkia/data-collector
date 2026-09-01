package io.tibobit.normalizer.typevalidate;

import java.util.List;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import io.tibobit.normalizer.model.ControlCommand;
import io.tibobit.normalizer.model.Lineage;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import io.tibobit.normalizer.model.RejectedOrderBookEvent;

/**
 * Job 2 — type validation, keyed by {@code (exchange_id, pair_id)}. Applies the
 * sequence rules decided in memory/project_raw_pipeline_decision.md; valid
 * events go to the main output, rejects to the {@link #REJECTED} side output
 * (dead-letter). Two rule kinds, selected by what the parser stamped on the
 * event (see [[pair-extractor]]):
 *
 * <ul>
 * <li><b>No ordering field</b> ({@code sequence_id == null}): no sequence to
 * order by, so out-of-order is detected by <b>event time</b> instead — a
 * null-seq snapshot older than the last accepted event is rejected
 * {@code out_of_order} (an old ex1 REST snapshot replayed after newer WS deltas
 * must not overwrite the newer book). Three exchanges land here: ex3 wallex
 * (never sends updates), ex9 lbank (a full-snapshot feed with no counter on the
 * wire at all — added 2026-08-26, and like ex3 it never sends updates), and the
 * ex1 nobitex REST snapshot (a resync whose Centrifugo offset is unknown). An
 * accepted one sets {@code baselinePending} so that the next update on the key
 * adopts its offset as a fresh baseline (see [[pair-extractor]]); for ex3 and
 * ex9 that flag is never consumed.
 *
 * <p>Note the comparison is {@code <}, not {@code <=}: a frame whose event time
 * EQUALS the last accepted one is accepted and re-emitted (user decision
 * 2026-08-26). Only a strictly older frame is out of order. That matters most
 * for ex9, whose duplicate-timestamp frames are therefore a re-emitted book
 * rather than a dead letter.</li>
 * <li><b>Snapshot</b> ({@code type == "snapshot"}): a fresh baseline, but
 * out-of-order/duplicate dropped — reject {@code stale_or_duplicate} if
 * {@code sequence_id <= lastSeq}. Otherwise it re-syncs the book: store
 * {@code lastSeq} and mark the stream trusted again. Ordering is the WHOLE
 * test: {@code sequence_jump} is deliberately never applied to a snapshot,
 * because a snapshot re-anchors the sequence rather than continuing it, and the
 * interval since the last update is the collector's choice, not the exchange's
 * cadence. Contiguity therefore has exactly two sites, both in the update
 * branch below — snapshot → next update, and update → update.</li>
 * <li><b>Update</b> ({@code type == "update"}, delta feeds ex1/ex2/ex5/ex6/ex8): needs
 * a baseline and a contiguous sequence. No baseline yet → {@code no_baseline};
 * still waiting to re-sync after a gap → {@code awaiting_snapshot};
 * {@code sequence_id} within {@code sequence_jump_tolerance} of
 * {@code lastSeq + sequence_jump} → valid (the tolerance is 0 for every
 * exchange but ex5/bitget, whose sequence is a millisecond clock — see the
 * window comment in {@code processElement});
 * {@code sequence_id <= lastSeq} → {@code stale_or_duplicate}; any other
 * forward jump is a gap → {@code sequence_gap}, and the stream is marked
 * untrusted (every update rejected until the next snapshot re-syncs).</li>
 * </ul>
 *
 * <p>
 * <b>The control plane.</b> While the stream is untrusted the book downstream
 * is empty — the gap emitted a {@link #RESET} — and only the collector can fix
 * that, by re-sending a snapshot. So the two untrustworthy branches also put a
 * {@code snapshot_request} on the {@link #CONTROL} side output, tagged with the
 * {@code reason} that made the stream untrustworthy and repeated on an interval
 * until something resolves the condition. That whole feature is one
 * piece of state, {@link #resyncRequestedAt}, and one method,
 * {@link #askForSnapshot}; there is no timer and no second flag. Job 2 is the
 * only producer of those commands.
 *
 * <p>
 * State per key: {@code lastSeq} (last accepted sequence id),
 * {@code lastEventTime} (ordering for null-seq snapshots),
 * {@code baselinePending} (the ex1 REST resync bootstrap) and
 * {@code resyncRequestedAt}. Topics are single-partition so per-key order
 * holds; no checkpointing configured (cold-start gap shared with the rest of
 * the platform — book is unvalidated until the first snapshot after a restart,
 * which on a delta feed means every key opens a {@code no_baseline} episode and
 * asks at once).
 */
public class TypeValidateFunction
        extends KeyedCoProcessFunction<String, RawOrderBookEvent, StalenessTick, RawOrderBookEvent> {

    /**
     * Dead-letter side output. Shared by the job wiring and the tests.
     */
    public static final OutputTag<RejectedOrderBookEvent> REJECTED = new OutputTag<>("rejected") {
    };

    /**
     * Control-plane side output: snapshot_request commands routed to the
     * collector.
     */
    public static final OutputTag<ControlCommand> CONTROL = new OutputTag<>("control") {
    };

    static final String STALE_OR_DUPLICATE = "stale_or_duplicate";
    static final String SEQUENCE_GAP = "sequence_gap";
    static final String AWAITING_SNAPSHOT = "awaiting_snapshot";
    static final String NO_BASELINE = "no_baseline";
    static final String OUT_OF_ORDER = "out_of_order";

    /**
     * Control-plane reasons for the two SILENCE conditions. Neither is a reject
     * reason — no event was rejected, that is the whole point — so unlike the
     * four above they appear only on {@code control-plane}, never on the
     * dead-letter topic.
     *
     * <p>
     * They are kept apart because they mean different things to the collector.
     * {@link #NO_DATA_RECEIVED} says this market has produced nothing since the
     * job started watching it: the subscription may never have been wired up,
     * or the exchange may have been down the whole time. {@link #STALE} says it
     * was working and stopped, which is a live feed that broke.
     */
    static final String NO_DATA_RECEIVED = "no_data_received";
    static final String STALE = "stale";

    /**
     * Event {@code type} of the synthetic reset marker emitted onto the main
     * stream on a gap. Job 5 turns it into an emptied book so the exchange
     * drops out of the aggregated view instead of serving its pre-gap diverged
     * book.
     */
    static final String RESET = "reset";

    private transient ValueState<Long> lastSeq;
    private transient ValueState<Boolean> baselinePending;
    private transient ValueState<Long> lastEventTime;

    /**
     * Processing-time millis at which this key last asked for a snapshot, or
     * {@code null} while the stream is trusted and nothing is outstanding. This
     * ONE field is the whole control plane's state, and it does three jobs:
     *
     * <ul>
     * <li><b>"the book is untrustworthy"</b> — it replaces the old
     * {@code awaitingSnapshot} flag outright. The two were never independent:
     * every branch that set one set the other, and they diverged only on the
     * {@code no_baseline} path, which is already discriminated one line earlier
     * by {@code lastSeq == null}. Keeping both was what made the resync
     * deadlock so hard to see — a state machine with two flags for one
     * condition has states that should not exist, and it reached one.</li>
     * <li><b>"a request is outstanding"</b> — see {@link #resyncPending()}, the
     * exemption that stops the ordering guards from throwing away the very
     * snapshot they asked for.</li>
     * <li><b>"and we asked at T"</b> — which is what makes re-asking a
     * comparison rather than a timer. See {@link #askForSnapshot}.</li>
     * </ul>
     *
     * <p>
     * Non-null therefore means exactly: the book cannot be trusted, we have
     * asked for a replacement, and here is when. Nothing else needs storing.
     */
    private transient ValueState<Long> resyncRequestedAt;

    /**
     * Processing-time millis of the last event that ARRIVED for this key —
     * accepted or rejected, no distinction. Silence means nothing came at all,
     * so a key that is rejecting everything is NOT stale: data is flowing and
     * the existing per-rejection retry already handles it.
     *
     * <p>
     * Null carries meaning and is the discriminator between the two silence
     * conditions: null means nothing has EVER arrived for this market, non-null
     * means it spoke once and then stopped. They are told apart here rather
     * than by a flag because the distinction is exactly "is there a last
     * arrival", and inventing a second field for a condition already implied by
     * the first is what made the resync deadlock unreadable.
     */
    private transient ValueState<Long> lastArrivalMs;

    /**
     * Processing-time millis at which this key was first ticked. Silence is
     * measured from here until the first event arrives, so a market that has
     * never spoken gets its full threshold from when we STARTED WATCHING rather
     * than from time zero — otherwise every subscribed market would fire a
     * request in the first second after every job submit.
     */
    private transient ValueState<Long> watchingSince;

    /**
     * The {@code simulation} flag of the last event that arrived, or null if
     * none ever has. Needed because a silence-triggered command has no
     * triggering event to copy it from, and the field's contract is that a
     * request raised by simulated data must not make the collector call a real
     * exchange.
     */
    private transient ValueState<Integer> lastSimulation;

    /** Default re-ask interval; overridden per job by SNAPSHOT_RETRY_MS. */
    static final long DEFAULT_SNAPSHOT_RETRY_MS = 300_000L;

    private final long snapshotRetryMs;

    public TypeValidateFunction() {
        this(DEFAULT_SNAPSHOT_RETRY_MS);
    }

    public TypeValidateFunction(long snapshotRetryMs) {
        this.snapshotRetryMs = snapshotRetryMs;
    }

    @Override
    public void open(OpenContext openContext) {
        lastSeq = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastSeq", Long.class));
        baselinePending = getRuntimeContext().getState(
                new ValueStateDescriptor<>("baselinePending", Boolean.class));
        lastEventTime = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastEventTime", Long.class));
        resyncRequestedAt = getRuntimeContext().getState(
                new ValueStateDescriptor<>("resyncRequestedAt", Long.class));
        lastArrivalMs = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastArrivalMs", Long.class));
        watchingSince = getRuntimeContext().getState(
                new ValueStateDescriptor<>("watchingSince", Long.class));
        lastSimulation = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastSimulation", Integer.class));
    }

    @Override
    public void processElement1(RawOrderBookEvent event, Context ctx,
            Collector<RawOrderBookEvent> out) throws Exception {
        event.getPipelineTimings().setTypeValidateIn(System.currentTimeMillis());

        // The market spoke. Recorded before any branch and for every event whatever its
        // verdict, because staleness here means "nothing arrived", not "nothing was
        // accepted": a key rejecting every update is alive and already re-asking on the
        // rejection path, and calling it stale as well would double-ask for one fault.
        lastArrivalMs.update(ctx.timerService().currentProcessingTime());
        lastSimulation.update(event.getSimulation());

        // No ordering field (ex3 wallex; ex1 nobitex REST snapshot): there is no
        // sequence to order
        // by, so detect out-of-order by EVENT TIME instead. A re-sent older snapshot
        // (e.g. ex1
        // REST replays an old book after newer WS deltas) must be rejected — otherwise
        // it overwrites
        // a newer book and wrongly re-arms the resync. Only once accepted does it flag
        // the key so the
        // next update adopts its offset as a fresh baseline (ex3 never sends updates,
        // so its flag is
        // set but never consumed).
        if (event.getSequenceId() == null) {
            Long lastEt = lastEventTime.value();
            if (!resyncPending() && lastEt != null && event.getEventTime() < lastEt) {
                reject(event, OUT_OF_ORDER, ctx);
                return;
            }
            baselinePending.update(true);
            resyncTrusted(); // this resync answers any outstanding request
            emit(event, out);
            return;
        }

        long seq = event.getSequenceId();
        Long last = lastSeq.value();

        if ("snapshot".equals(event.getType())) {
            // Ordering only — NO jump check here, for any exchange. See the class javadoc.
            if (!resyncPending() && last != null && seq <= last) {
                reject(event, STALE_OR_DUPLICATE, ctx);
                return;
            }
            lastSeq.update(seq);
            resyncTrusted(); // the requested snapshot has arrived
            emit(event, out);
            return;
        }

        // update (delta feeds only)
        if (Boolean.TRUE.equals(baselinePending.value())) {
            // First update after a null-seq snapshot (ex1 REST resync): adopt its offset as
            // the
            // baseline unconditionally, then resume contiguity checks from there.
            lastSeq.update(seq);
            baselinePending.update(false);
            resyncTrusted(); // baseline established, any request is moot
            emit(event, out);
            return;
        }
        // The two untrustworthy conditions. Both ask, and both keep asking on the retry
        // interval for as long as the condition holds — see askForSnapshot. They are told
        // apart by lastSeq alone: no baseline has ever been set (cold start, or a restart),
        // versus a baseline that a gap invalidated. That is why the control plane needs no
        // flag of its own to distinguish them.
        if (last == null) {
            askForSnapshot(event, ctx);
            reject(event, NO_BASELINE, ctx);
            return;
        }
        if (resyncPending()) {
            askForSnapshot(event, ctx);
            reject(event, AWAITING_SNAPSHOT, ctx);
            return;
        }
        // Contiguity, as a WINDOW rather than an equality: the expected next sequence is
        // last + jump, and sequence_jump_tolerance is how far either side of it still counts as
        // contiguous. Every exchange but ex5 stamps tolerance 0, which collapses this back to the
        // exact `seq == last + jump` check it has always been (ex6 jump 1, ex8 jump 300 — real
        // counters and a real fixed cadence). ex5/bitget stamps 600 +/- 10 because its sequence
        // is a millisecond clock, not a counter, so it never lands on an exact multiple.
        long expected = last + event.getSequenceJump();
        long tolerance = event.getSequenceJumpTolerance();
        if (seq >= expected - tolerance && seq <= expected + tolerance) {
            lastSeq.update(seq);
            emit(event, out);
        } else if (seq <= last) {
            reject(event, STALE_OR_DUPLICATE, ctx);
        } else {
            // Reached only when no resync is pending (the branch above returns otherwise),
            // so the reset marker fires exactly once per episode, as it always has.
            askForSnapshot(event, ctx);
            emitReset(event, out);
            reject(event, SEQUENCE_GAP, ctx);
        }
    }

    /**
     * The silence check: one tick per subscribed market per poll, carrying that
     * market's own {@code staleness_threshold_seconds}. This is the input that
     * exists so a market which has never sent anything still HAS a key — a
     * keyed function cannot notice a market it has never heard of, and no timer
     * can fire for a key that was never created.
     *
     * <p>
     * Two conditions, told apart by {@link #lastArrivalMs} being null, and only
     * one of them clears the book:
     *
     * <ul>
     * <li><b>Never received</b> — nothing has arrived since we started
     * watching. NO reset is emitted: there is no book downstream to empty, and
     * emitting one would create keyed state in jobs 3-5 for a market that has
     * never existed. Only the request goes out.</li>
     * <li><b>Went silent</b> — it spoke and then stopped, so there IS a book
     * downstream and it is now describing a market nobody is watching any more.
     * That book is emitted away with a {@link #RESET} so the exchange drops out
     * of the aggregated view, exactly as it does on a sequence gap.</li>
     * </ul>
     *
     * <p>
     * Both go through the same {@link #askForSnapshot} suppression as every
     * other ask, so a stale key and a rejecting key can never combine into two
     * commands per interval — the defect that the timer implementation shipped
     * with, arrived at by a different route.
     */
    @Override
    public void processElement2(StalenessTick tick, Context ctx,
            Collector<RawOrderBookEvent> out) throws Exception {
        long now = ctx.timerService().currentProcessingTime();

        Long watching = watchingSince.value();
        if (watching == null) {
            // The first tick starts the clock, it does not judge on it. Without this a
            // submit would fire a request for every subscribed market at once, before any
            // of them had been given a chance to speak.
            watchingSince.update(now);
            return;
        }

        Long lastArrival = lastArrivalMs.value();
        long silentSince = lastArrival != null ? lastArrival : watching;
        if (now - silentSince < tick.thresholdMs()) {
            return;
        }

        if (lastArrival == null) {
            askForSnapshot(NO_DATA_RECEIVED, tick.getExchangeId(), tick.getPairId(),
                    simulationOrLive(), List.of(), ctx);
            return;
        }

        // Captured BEFORE asking, because asking is what opens the episode. Only the
        // first tick past the threshold empties the book; the rest just re-ask.
        boolean firstOfEpisode = !resyncPending();
        askForSnapshot(STALE, tick.getExchangeId(), tick.getPairId(),
                simulationOrLive(), List.of(), ctx);
        if (firstOfEpisode) {
            emitSilenceReset(tick, now, out);
        }
    }

    /**
     * The {@code simulation} flag to put on a command or reset that no event
     * triggered. The last arriving event's flag if this market has ever spoken;
     * 0 (live) if it never has, because then nothing has ever told us
     * otherwise.
     *
     * <p>
     * The never-spoke case is a real limitation, not a safe default: a market
     * configured as simulated that has produced nothing will have a live
     * snapshot requested for it. The DB has no simulation column to read it
     * from — the flag exists only on the wire — so there is nowhere better to
     * get it, and requesting a real snapshot for a market that is not
     * collecting is inert.
     */
    private int simulationOrLive() throws Exception {
        Integer simulation = lastSimulation.value();
        return simulation == null ? 0 : simulation;
    }

    /**
     * Emits a {@link #RESET} for a market that stopped speaking. Same purpose
     * as the gap reset — empty the book so the exchange drops out of the
     * aggregated view rather than serving a frozen one — but built from the
     * tick, because no event triggered it.
     *
     * <p>
     * Three fields cannot be inherited and are set deliberately:
     *
     * <ul>
     * <li>{@code event_time} is processing time. There is no event to take one
     * from, and it is the honest answer to "when did we conclude this": now.
     * ex3/ex4 already stamp processing time as event time, so downstream is not
     * seeing a new kind of value.</li>
     * <li>{@code source_ids} is EMPTY, never {@code [""]}. Nothing caused this
     * record except the passage of time, and a blank string parent is an
     * untraceable id that satisfies every "is the field set" check while
     * meaning nothing — a bug the timer implementation actually shipped.</li>
     * <li>{@code simulation} comes from the last event seen; see
     * {@link #simulationOrLive()}.</li>
     * </ul>
     *
     * <p>
     * It deliberately does NOT go through {@link #emit}, which would advance
     * {@code lastEventTime} to wall-clock now — exchange event times are not
     * wall clock, so a resumed feed would look older than the reset. This is
     * defence in depth and NOT what keeps the key alive: a mutation that
     * advances {@code lastEventTime} here is not observable, because asking
     * opens a resync episode and the ordering guards are suspended while one is
     * outstanding, so the returning event is accepted and overwrites the
     * poisoned value before any guard reads it. The exemption is the load-bearing
     * part; this is a second lock on the same door. Keep both — the exemption
     * has been removed by accident once already (the 2026-08-19 deadlock).
     */
    private void emitSilenceReset(StalenessTick tick, long now, Collector<RawOrderBookEvent> out)
            throws Exception {
        RawOrderBookEvent reset = new RawOrderBookEvent(
                tick.getExchangeId(), tick.getPairId(), RESET, null, 0L, now, null, null);
        reset.setSimulation(simulationOrLive());
        reset.setSourceIds(List.of());
        reset.setId(Lineage.newId());
        reset.getPipelineTimings().setTypeValidateIn(now);
        reset.getPipelineTimings().setTypeValidateOut(System.currentTimeMillis());
        out.collect(reset);
    }

    /**
     * True while a {@code snapshot_request} is outstanding — we asked for a
     * snapshot because the book is untrustworthy (a gap emptied it downstream
     * via {@link #RESET}, or there is no baseline at all) and it has not
     * arrived yet.
     *
     * <p>
     * The two ordering guards — {@code out_of_order} on a null-seq snapshot and
     * {@code stale_or_duplicate} on a sequenced one — exist to stop an OLD
     * snapshot overwriting a GOOD book. While a resync is pending there is no
     * good book to protect, so they do not apply and the resync snapshot is
     * accepted whatever its clock says.
     *
     * <p>
     * Without this exemption the key deadlocks, which is the bug this feature
     * shipped with. The ex1/ex2 resync snapshot is the null-seq REST one, and
     * its {@code event_time} comes from a different clock than the WS deltas
     * that set {@code lastEventTime} — so it can easily look "old" and be
     * rejected. {@code lastEventTime} only advances inside {@link #emit}, which
     * a rejected event never reaches, so every subsequent snapshot fails the
     * identical stale comparison: the guard that rejected the resync is the
     * guard that can never afterwards be satisfied.
     *
     * <p>
     * Accepting an out-of-date snapshot here is strictly better than that: the
     * book was already emptied by the reset, so an old book beats no book, and
     * the next update re-anchors the baseline (or gaps again and asks again).
     */
    private boolean resyncPending() throws Exception {
        return resyncRequestedAt.value() != null;
    }

    /** The book can be trusted again: whatever we asked for has arrived. */
    private void resyncTrusted() throws Exception {
        resyncRequestedAt.clear();
    }

    /**
     * Why this market needs a snapshot, for the {@code reason} field on the
     * command: {@link #NO_BASELINE} if no baseline has ever been adopted,
     * {@link #SEQUENCE_GAP} otherwise. Same discriminator the reject reasons
     * use one line apart — {@code lastSeq} alone — so like everything else in
     * the control plane it needs no state of its own.
     *
     * <p>
     * It is stable for the whole episode, which is what makes a RETRY carry the
     * same reason as the first ask: {@code lastSeq} only moves inside {@link
     * #emit}, and while a resync is pending every event is rejected instead. So
     * a retry says {@code sequence_gap} rather than the {@code
     * awaiting_snapshot} its own trigger event was dead-lettered with — the
     * reason describes what the collector is being asked to fix, not the
     * bookkeeping state of the update that reminded us to ask.
     */
    private String resyncReason() throws Exception {
        return lastSeq.value() == null ? NO_BASELINE : SEQUENCE_GAP;
    }

    /**
     * Asks the collector to re-send a snapshot for this market, unless we
     * already asked within {@code snapshotRetryMs}. Called from BOTH
     * untrustworthy branches on EVERY event they reject, which is what makes
     * the first ask and the hundredth retry the same line of code: the only
     * question either time is "have we asked recently?".
     *
     * <p>
     * <b>Why this is not a timer.</b> The request has to be repeated, because
     * one command per episode plus an episode that only ends on an ACCEPTED
     * snapshot means a single lost command leaves the market dark until the job
     * restarts. The obvious mechanism is a processing-time timer, and it is the
     * wrong one: {@code onTimer} is handed no event, so it needs the exchange,
     * pair, simulation flag and parent id copied into state or parsed back out
     * of the key; and timers are registered per episode but cancelled nowhere,
     * so two episodes inside one retry interval leave two live timer chains,
     * each re-arming the other forever. Rejections, meanwhile, arrive exactly
     * when a retry is worth sending and carry all four fields already.
     *
     * <p>
     * The one case a timer covers and this does not is a market that goes
     * SILENT after the gap: no events, so no retries. That is deliberate. A
     * feed sending nothing cannot be re-synced by anything we put on the topic
     * — and the moment it speaks again, its first update is rejected and asks.
     * Both triggers ({@code no_baseline}, {@code sequence_gap}) are themselves
     * updates, so the feed is by definition alive when an episode opens.
     *
     * <p>
     * The command carries a {@code reason} saying why the snapshot is wanted,
     * taken from {@link #resyncReason()} — so the collector can tell a market
     * that has never had a baseline (nothing to apply updates to) from one
     * whose book a gap invalidated, and can rate-limit or prioritise them
     * differently if it ever needs to.
     *
     * <p>
     * Lineage is DERIVED, not inherited — same rule as {@link #emitReset} and
     * {@link #reject}. This is a write to a topic, so it mints its own id and
     * names the event that triggered it as its parent; inheriting would
     * duplicate the raw event's id (already carried inside the dead-letter
     * envelope) and point one hop too far back. Every command therefore names
     * the update that was dead-lettered alongside it, retries included, so a
     * request can always be tied to a record on the rejected topic. {@code
     * simulation} IS carried: a gap in simulated data must not make the
     * collector call a real exchange.
     */
    private void askForSnapshot(RawOrderBookEvent trigger, Context ctx) throws Exception {
        askForSnapshot(resyncReason(), trigger.getExchangeId(), trigger.getPairId(),
                trigger.getSimulation(), List.of(trigger.getId()), ctx);
    }

    /**
     * The ask itself, shared by the event-driven callers above and the
     * silence-driven ones in {@link #processElement2}. Everything the command
     * needs is a parameter, so no caller has to smuggle a trigger event in to
     * reach it — which is what a rejection has and a tick does not.
     *
     * <p>
     * ONE suppression window governs all four reasons. An event-driven ask and
     * a silence-driven ask for the same market are the same episode wanting the
     * same thing, so they must share the interval rather than each get one.
     */
    private void askForSnapshot(String reason, int exchangeId, int pairId, int simulation,
            List<String> sourceIds, Context ctx) throws Exception {
        long now = ctx.timerService().currentProcessingTime();
        Long askedAt = resyncRequestedAt.value();
        if (askedAt != null && now - askedAt < snapshotRetryMs) {
            return;
        }
        resyncRequestedAt.update(now);
        ctx.output(CONTROL, new ControlCommand(
                ControlCommand.SNAPSHOT_REQUEST,
                reason,
                exchangeId,
                pairId,
                simulation,
                Lineage.newId(),
                sourceIds));
    }

    /**
     * Emits a synthetic {@link #RESET} marker on the main stream carrying the
     * gap event's identity and event time but no book (null sides, null
     * sequence). Reached only on the not-awaiting → awaiting transition, so it
     * fires exactly once per gap episode — subsequent updates return at the
     * {@code awaiting_snapshot} reject above. A fresh {@link
     * io.tibobit.normalizer.model.PipelineTimings} (not the gap event's) is
     * used so stamping {@code type_validate_out} here does not leak onto the
     * event that is still being dead-lettered.
     *
     * <p>
     * {@code simulation} IS inherited from the gap event: the marker stands in
     * for that exchange's stream, so an emptied simulation book must not come
     * back out flagged as live.
     *
     * <p>
     * Lineage is NOT inherited — it is derived. The marker is a new record
     * caused by the gap event, so the gap event is its source and the marker
     * gets an id of its own. The gap event is simultaneously dead-lettered, so
     * its one id legitimately appears as the parent of two different records.
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
        // Track the event time of the last accepted event so a later null-seq snapshot
        // can be
        // ordered against it (the out-of-order guard above).
        lastEventTime.update(event.getEventTime());
        Lineage.restamp(event);
        event.getPipelineTimings().setTypeValidateOut(System.currentTimeMillis());
        out.collect(event);
    }

    private void reject(RawOrderBookEvent event, String reason, Context ctx) {
        // typeValidateIn is already stamped; leave typeValidateOut null — the event
        // never leaves
        // the validator onto the main stream. rejectedAt records when it was
        // dead-lettered.
        //
        // The envelope gets its own id: the dead-letter topic is a topic. The event
        // inside
        // keeps the id job 1 gave it — deliberately NOT restamped, since it is being
        // recorded, not
        // forwarded, and that id is what links this record back to the raw stream.
        RejectedOrderBookEvent rejection = new RejectedOrderBookEvent(event, reason, System.currentTimeMillis());
        rejection.setSourceIds(List.of(event.getId()));
        rejection.setId(Lineage.newId());
        ctx.output(REJECTED, rejection);
    }
}
