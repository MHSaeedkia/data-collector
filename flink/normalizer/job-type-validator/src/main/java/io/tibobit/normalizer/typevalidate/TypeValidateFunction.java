package io.tibobit.normalizer.typevalidate;

import java.util.List;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import io.tibobit.normalizer.lookup.RefreshingLookup;
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
 * <p>
 * Note the comparison is {@code <}, not {@code <=}: a frame whose event time
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
 * <li><b>Update</b> ({@code type == "update"}, delta feeds
 * ex1/ex2/ex5/ex6/ex8): needs a baseline and a contiguous sequence. No baseline
 * yet → {@code no_baseline}; still waiting to re-sync after a gap →
 * {@code awaiting_snapshot}; {@code sequence_id} within
 * {@code sequence_jump_tolerance} of {@code lastSeq + sequence_jump} → valid
 * (the tolerance is 0 for every exchange but ex5/bitget, whose sequence is a
 * millisecond clock — see the window comment in {@code processElement});
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
 * until something resolves the condition. That whole feature is one piece of
 * state, {@link #resyncRequestedAt}, and one method, {@link #askForSnapshot}.
 * Job 2 is the only producer of those commands.
 *
 * <p>
 * <b>Silence.</b> The rules above can only fire when something arrives, so a
 * market that simply stops sending is invisible to them and its book stays
 * frozen downstream forever. {@link #onTimer} closes that: each key holds one
 * processing-time deadline at {@code lastArrival +
 * staleness_threshold_seconds}, and passing it empties the book with a
 * {@link #RESET} and asks, through the same {@link #askForSnapshot} window.
 * Only markets that have spoken at least once are watched — see {@link #STALE}
 * for why the never-received case is deliberately somebody else's job.
 *
 * <p>
 * <b>Unsubscribe is the same problem wearing a different hat.</b> A market
 * dropped from the watch list also stops arriving, and leaving its book
 * standing is worse than leaving a silent one: nothing will ever look at that
 * key again, so the phantom persists for the life of the job. So the timer
 * empties the book on its way out too — a {@link #RESET} and no request, since
 * the market is being dropped, not recovered. Correctness has to live here and
 * not in a consumer: every stage of this pipeline is read by a different team,
 * and a book with no feed behind it must not be on the topic in the first
 * place.
 *
 * <p>
 * State per key: {@code lastSeq} (last accepted sequence id),
 * {@code lastEventTime} (ordering for null-seq snapshots),
 * {@code baselinePending} (the ex1 REST resync bootstrap),
 * {@code resyncRequestedAt}, and for silence {@code lastArrivalMs},
 * {@code lastSimulation} and {@code stalenessTimerAt}. Topics are
 * single-partition so per-key order holds; no checkpointing configured
 * (cold-start gap shared with the rest of the platform — book is unvalidated
 * until the first snapshot after a restart, which on a delta feed means every
 * key opens a {@code no_baseline} episode and asks at once).
 */
public class TypeValidateFunction
        extends KeyedProcessFunction<String, RawOrderBookEvent, RawOrderBookEvent> {

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
     * Control-plane reason for SILENCE: this market was sending and stopped.
     * Not a reject reason — no event was rejected, that is the whole point — so
     * unlike the four above it appears only on {@code control-plane}, never on
     * the dead-letter topic.
     *
     * <p>
     * There is deliberately no companion reason for a market that has NEVER
     * sent anything. Job 2 can only notice a market it has heard from, because
     * a keyed function has no state for a key no event ever created; noticing
     * the rest would mean importing the full subscription roster, which is a
     * monitoring question and already answered by the staleness exporter (see
     * memory/project_staleness_exporter.md). It would also be a request the
     * collector cannot act on: a snapshot does not start a subscription that
     * was never wired up.
     */
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
     * Always non-null wherever the silence path reads it, because the key
     * itself only exists once an event has arrived and set it.
     */
    private transient ValueState<Long> lastArrivalMs;

    /**
     * Timestamp of the ONE outstanding silence timer for this key, or null when
     * none is armed. Its whole purpose is that "one" — the first timer-based
     * control plane registered a timer per episode and cancelled none, so two
     * episodes inside one interval left two live chains each re-arming forever.
     * Here a timer is armed only when this field is null and is cleared the
     * moment it fires, so a key can never accumulate a second chain.
     *
     * <p>
     * It also means there is no per-event timer churn: an arriving event does
     * not cancel and re-register anything. A timer that fires early simply
     * re-arms itself for {@code lastArrivalMs + threshold}, so detection still
     * lands exactly on the deadline.
     */
    private transient ValueState<Long> stalenessTimerAt;

    /**
     * The {@code simulation} flag of the last event that arrived. Needed
     * because a silence-triggered command has no triggering event to copy it
     * from, and the field's contract is that a request raised by simulated data
     * must not make the collector call a real exchange.
     */
    private transient ValueState<Integer> lastSimulation;

    /**
     * The exchange and pair ids of the last event that arrived. They exist for
     * exactly one caller: the unsubscribe reset in {@link #onTimer}, which has
     * to name the market it is emptying at the one moment the watch list no
     * longer holds a row to name it from. Stored rather than parsed out of the
     * key string, for the reason in {@link WatchedMarket}'s javadoc.
     *
     * <p>
     * Cleared once that reset is emitted, so the book is emptied once per
     * unsubscribe rather than once per surviving timer.
     */
    private transient ValueState<Integer> lastExchangeId;

    /** @see #lastExchangeId */
    private transient ValueState<Integer> lastPairId;

    /**
     * Default re-ask interval; overridden per job by SNAPSHOT_RETRY_MS.
     */
    static final long DEFAULT_SNAPSHOT_RETRY_MS = 300_000L;

    private final long snapshotRetryMs;

    /**
     * Every SUBSCRIBED market and its {@code staleness_threshold_seconds},
     * refreshed from Postgres while the job runs. Looked up by the operator's
     * current key; a market absent from it is not watched at all, which is how
     * an unsubscribe stops the silence timer without a resubmit.
     */
    private final RefreshingLookup<String, WatchedMarket> watched;

    public TypeValidateFunction(long snapshotRetryMs, RefreshingLookup<String, WatchedMarket> watched) {
        this.snapshotRetryMs = snapshotRetryMs;
        this.watched = watched;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
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
        stalenessTimerAt = getRuntimeContext().getState(
                new ValueStateDescriptor<>("stalenessTimerAt", Long.class));
        lastSimulation = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastSimulation", Integer.class));
        lastExchangeId = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastExchangeId", Integer.class));
        lastPairId = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastPairId", Integer.class));
        // Propagates: a job that cannot read its watch list must not start up watching
        // nothing, because "watching nothing" and "everything is healthy" look identical
        // from outside.
        watched.open();
    }

    @Override
    public void close() {
        watched.close();
    }

    @Override
    public void processElement(RawOrderBookEvent event, Context ctx,
            Collector<RawOrderBookEvent> out) throws Exception {
        event.getPipelineTimings().setTypeValidateIn(System.currentTimeMillis());

        // The market spoke. Recorded before any branch and for every event whatever its
        // verdict, because staleness here means "nothing arrived", not "nothing was
        // accepted": a key rejecting every update is alive and already re-asking on the
        // rejection path, and calling it stale as well would double-ask for one fault.
        long arrivedAt = ctx.timerService().currentProcessingTime();
        lastArrivalMs.update(arrivedAt);
        lastSimulation.update(event.getSimulation());
        lastExchangeId.update(event.getExchangeId());
        lastPairId.update(event.getPairId());
        armSilenceTimer(arrivedAt, ctx);

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
     * The silence check. A market that was sending and stopped is one nobody
     * downstream can notice: no event arrives, so no rule runs, and job 5 keeps
     * serving a frozen book as if it were live. So every key carries a
     * processing-time deadline at
     * {@code lastArrival + staleness_threshold_seconds}, and when it passes
     * without anything arriving the book is emptied with a {@link #RESET} and a
     * {@code snapshot_request} goes out — exactly what a sequence gap does,
     * reached by silence instead of by a bad sequence.
     *
     * <p>
     * <b>Only markets that have spoken are watched.</b> A market that has never
     * sent a single event has no keyed state and therefore no deadline here,
     * deliberately: seeing it at all would mean importing the whole
     * subscription roster into a stream validator, and the answer would be an
     * alert for a human rather than a command the collector can act on. That
     * belongs to the staleness exporter, which already derives the same roster
     * from the same table. See {@link #STALE}.
     *
     * <p>
     * The reset fires once per episode — guarded on {@link #resyncPending()},
     * which asking sets — while the ask itself repeats, rate-limited by the
     * SAME {@link #askForSnapshot} window every other reason uses. A silent key
     * and a rejecting key can therefore never combine into two commands per
     * interval.
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RawOrderBookEvent> out)
            throws Exception {
        // This timer is spent. Cleared FIRST and unconditionally, so that every path
        // out of here leaves at most the one timer it re-arms below.
        stalenessTimerAt.update(null);

        WatchedMarket market = watched.get(ctx.getCurrentKey());
        if (market == null) {
            // Unsubscribed (or the row is gone) while the timer was in flight. Stop
            // watching -- but empty the book on the way out, because "we no longer
            // carry this market" is downstream indistinguishable from "the book is
            // still what it was" otherwise: job 5 keeps its MapState, job 6 keeps the
            // exchange in the union, and every consumer of the snapshot and aggregated
            // topics goes on being served a book with no feed behind it, forever.
            // Deliberately NO snapshot_request -- we are dropping this market, not
            // trying to recover it, and asking would tell NiFi to reopen a feed it was
            // just told to close.
            emitUnsubscribeReset(ctx, out);
            return;
        }

        Long lastArrival = lastArrivalMs.value();
        if (lastArrival == null) {
            return;
        }

        long now = ctx.timerService().currentProcessingTime();
        if (now - lastArrival < market.thresholdMs()) {
            // It spoke after this timer was armed. Arriving events deliberately do not
            // cancel and re-register the timer, so this is where the deadline is moved
            // instead — detection still lands exactly on lastArrival + threshold.
            armSilenceTimerAt(lastArrival + market.thresholdMs(), ctx);
            return;
        }

        // Captured BEFORE asking, because asking is what opens the episode. Only the
        // first deadline past the threshold empties the book; the rest just re-ask.
        boolean firstOfEpisode = !resyncPending();
        askForSnapshot(STALE, market.getExchangeId(), market.getPairId(),
                simulationOfLastEvent(), List.of(), ctx);
        if (firstOfEpisode) {
            emitSilenceReset(market.getExchangeId(), market.getPairId(), now, out);
        }
        // Still silent, so keep watching: the ask repeats until something arrives.
        armSilenceTimerAt(now + market.thresholdMs(), ctx);
    }

    /**
     * Empties the book for a key whose market has left the watch list.
     *
     * <p>
     * Reaching here proves the market WAS watched: a timer is only ever armed
     * from a successful lookup, so a market that was never subscribed has no
     * timer and never arrives at this branch at all. That is what makes the
     * stored ids safe to trust as "the market this key is".
     *
     * <p>
     * Fires at most once per unsubscribe. What actually guarantees that is the
     * <b>absence of a re-arm</b> on the way out: no timer, so no second firing.
     * Clearing the ids is defence in depth and is NOT observable — a mutation
     * that drops those two lines kills no test, and cannot, because nothing can
     * reach here twice. Keep them anyway: they are what makes "once" survive
     * somebody later deciding this branch should re-arm.
     *
     * <p>
     * The ids are always non-null here, for the same reason {@link
     * #lastArrivalMs} is: the key only exists once an event has arrived and set
     * them, and that happens before the timer that leads here is armed. The
     * check is the same unreachable-by-construction guard the silence path
     * already keeps, and a market that never spoke is covered a step earlier —
     * no event, no timer, nothing to empty.
     */
    private void emitUnsubscribeReset(OnTimerContext ctx, Collector<RawOrderBookEvent> out)
            throws Exception {
        Integer exchangeId = lastExchangeId.value();
        Integer pairId = lastPairId.value();
        if (exchangeId == null || pairId == null) {
            return;
        }
        emitSilenceReset(exchangeId, pairId,
                ctx.timerService().currentProcessingTime(), out);
        lastExchangeId.update(null);
        lastPairId.update(null);
    }

    /**
     * Arms the silence deadline for this key if none is outstanding, measured
     * from the event that just arrived. Does nothing when the market is not in
     * the watch list — an unsubscribed market is supposed to be silent.
     */
    private void armSilenceTimer(long from, Context ctx) throws Exception {
        if (stalenessTimerAt.value() != null) {
            return;
        }
        WatchedMarket market = watched.get(ctx.getCurrentKey());
        if (market == null) {
            return;
        }
        armSilenceTimerAt(from + market.thresholdMs(), ctx);
    }

    /**
     * Registers the one outstanding timer and records it, so nothing arms a
     * second.
     */
    private void armSilenceTimerAt(long at, Context ctx) throws Exception {
        ctx.timerService().registerProcessingTimeTimer(at);
        stalenessTimerAt.update(at);
    }

    /**
     * The {@code simulation} flag for a command or reset that no event
     * triggered: the one the last arriving event carried. Silence is only ever
     * judged on a key that has spoken, so there is always one to copy.
     */
    private int simulationOfLastEvent() throws Exception {
        return lastSimulation.value();
    }

    /**
     * Emits a {@link #RESET} for a market that stopped speaking. Same purpose
     * as the gap reset — empty the book so the exchange drops out of the
     * aggregated view rather than serving a frozen one — but built from the
     * watch-list row, because no event triggered it.
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
     * {@link #simulationOfLastEvent()}.</li>
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
     * poisoned value before any guard reads it. The exemption is the
     * load-bearing part; this is a second lock on the same door. Keep both —
     * the exemption has been removed by accident once already (the 2026-08-19
     * deadlock).
     */
    private void emitSilenceReset(int exchangeId, int pairId, long now,
            Collector<RawOrderBookEvent> out) throws Exception {
        RawOrderBookEvent reset = new RawOrderBookEvent(
                exchangeId, pairId, RESET, null, 0L, now, null, null);
        reset.setSimulation(simulationOfLastEvent());
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

    /**
     * The book can be trusted again: whatever we asked for has arrived.
     */
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
     * The one case this does not cover is a market that goes SILENT after the
     * gap: no events, so no retries. That is handled separately by
     * {@link #onTimer}, which does use a processing-time timer — but exactly
     * one per key, tracked in {@link #stalenessTimerAt} and cleared the moment
     * it fires, so the multiplying-chain defect above has nowhere to live. It
     * needs none of the fields copied into state that a retry timer would
     * either: the market's identity comes from its watch-list row.
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
     * silence-driven one in {@link #onTimer}. Everything the command needs is a
     * parameter, so no caller has to smuggle a trigger event in to reach it —
     * which is what a rejection has and an expiring deadline does not.
     *
     * <p>
     * ONE suppression window governs all three reasons. An event-driven ask and
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
