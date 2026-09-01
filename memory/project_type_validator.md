---
name: type-validator
description: M3 DONE 2026-07-15 — job-type-validator module (raw pipeline job 2): sequence-validation rules, keyed state, dead-letter wiring, and the runtime/smoke gotchas
metadata:
    type: project
---

# Job 2 — type validator (Milestone 3, done 2026-07-15)

`flink/normalizer/job-type-validator/` (package `io.tibobit.normalizer.typevalidate`,
[[normalizer-scaffold]] conventions). Consumes `ex[0-9]+-p[0-9]+-raw-flink` (job 1's output,
`RawOrderBookEvent`), keyed `(exchange_id, pair_id)`, and routes:
valid → `ex{id}-p{id}-type-validated-raw-flink` (SAME `raw-order-book-event` subject),
rejects → dead-letter `ex{id}-p{id}-rejected-flink` (`rejected-order-book-event`). 14 harness
tests + live smoke 3/3 green. (2026-07-21: ex1 became a delta feed — see the ex1 resync note.)

## The rule set (the important decision)

The todo.md M3 sketch ("snapshot = unconditional baseline") was RECONCILED with the later,
authoritative revised job-2 scope in [[raw-pipeline-decision]]. **The discriminator is `type` +
`sequence_id == null`, NOT `sequence_jump` alone** — a delta feed's SNAPSHOT message also carries
jump>0 (verified in `BybitParser`/`OkxParser`: jump is hardcoded per parser, not per message
type). So `TypeValidateFunction.processElement`, with `ValueState {Long lastSeq, Boolean
awaitingSnapshot}`:

1. **`sequence_id == null`** (ex3 wallex; ex1 nobitex REST snapshot — see below) → no sequence to
   order by, so ordered by **event time** instead: `lastEventTime != null && event_time <
   lastEventTime` → reject `out_of_order` (new reason, added 2026-07-21) BEFORE touching state; else
   pass through, set `baselinePending = true` + clear `awaitingSnapshot` (see the ex1 resync note).
2. **`type == "snapshot"`** (a fresh baseline, but out-of-order/duplicate dropped):
   `lastSeq != null && seq <= lastSeq` → reject `stale_or_duplicate`; else accept, set
   `lastSeq = seq`, clear `awaitingSnapshot`. This is the snapshot-feed staleness check AND a
   delta feed's re-sync in one branch. (Non-null-seq snapshots only: ex2/4/5 + ex6/ex8.)
3. **`type == "update"`** (delta feeds ex1/ex6/ex8): FIRST `baselinePending` → adopt `lastSeq =
   seq` unconditionally, clear the flag, accept (ex1 resync bootstrap); else `lastSeq == null` →
   reject `no_baseline`; `awaitingSnapshot` → reject `awaiting_snapshot`; `seq == lastSeq +
   sequence_jump` → accept, `lastSeq = seq`; `seq <= lastSeq` → reject `stale_or_duplicate`; else
   (any other forward jump) → reject `sequence_gap` + set `awaitingSnapshot` (every update rejected
   until the next snapshot re-syncs). Reasons: `stale_or_duplicate` / `sequence_gap` /
   `awaiting_snapshot` / `no_baseline`.

## ex1 nobitex resync (added 2026-07-21, coupled with [[pair-extractor]])

ex1 flipped from a snapshot-only feed to a **REST snapshot + WS delta** feed. The REST snapshot
carries **no offset** (`sequence_id = null`), so it can't seed `lastSeq` like ex6/ex8 snapshots
do. Mechanism: a **third `ValueState<Boolean> baselinePending`** — the null-seq snapshot sets it,
and the **first `update` after it adopts that update's `pub.offset` as the baseline
unconditionally**, then normal `+sequence_jump(=1)` contiguity resumes. The design is
**exchange-agnostic** (no hardcoded exchange_id): ex3 also sets `baselinePending`, but ex3 never
sends updates so the flag is never consumed — harmless. ex6/ex8 snapshots have non-null seq → they
never touch this branch, so their `no_baseline` cold-start semantics are unchanged. 14 harness
tests (3 new for ex1). **Not yet run live** — needs NiFi's REST feed; parser + job 2 + NiFi must
cut over together (a partial deploy makes ex1 updates reject `no_baseline`).

**out-of-order guard for null-seq snapshots (added 2026-07-21, follow-up).** Bug: the null-seq
branch re-armed `baselinePending` + emitted **unconditionally** — so replaying an OLD ex1 REST
snapshot after newer WS deltas (snapshot → update → update → *same old snapshot again*) overwrote
the newer book AND wrongly re-armed the resync (the next update would be adopted as a fresh baseline,
masking a real gap). These frames carry **no sequence id**, so the only ordering signal is event
time. Fix: a fourth state field `ValueState<Long> lastEventTime` (updated in `emit()` for every
accepted event = last-accepted event_time, symmetric with `lastSeq`); the null-seq branch rejects
`out_of_order` when `event_time < lastEventTime` **before** mutating state. Strict `<` (not `<=`) on
purpose: equal-event-time frames pass, so ex3 wallex snapshots stamped the same processing-time
millisecond aren't false-rejected (an equal-time duplicate re-applied is idempotent downstream; the
aggregator dedups by strict `<` too). Seq-bearing snapshots (ex6/ex8) are unaffected — their old
snapshots are already caught by the seq `<= lastSeq` check. 16 harness tests (2 new). Still not run
live.

Rejects go to the `REJECTED` side output (`OutputTag<RejectedOrderBookEvent>`, a public static on
the function so job wiring + tests share it). No metrics counters (unlike job 1's drops — here the
dead-letter topic IS the audit record). Timings: stamps `type_validate_in` on entry and
`type_validate_out` before the main `collect`; rejects keep `type_validate_out` null (never emitted
onward), `rejectedAt` records the dead-letter time.

## Reset marker on gap (added 2026-07-21)

The true-gap `else` branch now ALSO emits a synthetic **reset** onto the MAIN stream (not just the
dead-letter): `type="reset"` (`RESET` constant), null seq/asks/bids, gap event's identity +
event_time. Job 5 turns it into an emptied book so the exchange **drops out** of the aggregated view
instead of serving its pre-gap diverged book until the next snapshot — restoring the old
monolithic-merger "gap ⇒ clear the book" guarantee ([[orderbook-aggregation]]). The offending update
is **still dead-lettered** unchanged.

**Why a fresh `PipelineTimings` on the reset, not the gap event's:** the same gap event is
simultaneously dead-lettered (where `type_validate_out` must stay null). Reusing its timings object
and stamping `_out` for the reset would leak onto the rejected copy — they'd alias. So `emitReset`
builds a new event with its own timings.

**Emitted once per gap episode** for free: the branch is only reached on the not-awaiting→awaiting
transition; every subsequent held update returns at the `awaitingSnapshot` reject above, so no second
reset fires until a snapshot re-syncs and a new gap occurs. Exchange-agnostic (any delta feed). Tests
use a `validBusiness()` helper filtering resets so the existing sequence assertions are unchanged; a
dedicated test pins the reset's fields + once-per-episode.

**LIVE BUG on the first gap test (2026-07-22) — `type` is an Avro ENUM, not a free string.** Plan
assumption #3 ("`RawOrderBookEvent.type` is a plain string, `"reset"` is free") was WRONG. The first
live snapshot→2 updates→gap test crashed the TaskManager: `RawOrderBookEventSerializer.serialize`
→ `new GenericData.EnumSymbol(typeSchema, "reset")` → Avro `getEnumOrdinal("reset")` returned null
(NPE) because the registered `raw-order-book-event` `Type` enum was `["snapshot","update"]`. The job
FAILED, so the reset never reached job 5 → **the book was never cleared** = exactly the user's "gap
not cleaned" symptom (the NPE, not the logic, was the fault). Fix: added `"reset"` to the `Type`
enum in `schemas/raw_order_book_event.avsc` (single source of truth) and re-registered to the live
registry — adding an enum symbol is BACKWARD-compatible (dry-run `is_compatible:true`; now v2 / id 7).
**No Java rebuild**: production reads the wire schema via `AvroSchemaLoader.loadLatest` and the Java
model's `type` is a plain `String`. BUT serializers cache `loadLatest` once, so **the running jobs
must be resubmitted** to pick up v2 — `make run-normalizer-jobs` (cancel+resubmit; NOT
`refresh-normalizer`, which `down -v`s and wipes the registry/data). This is the SAME standing rule
as the `rejected-order-book-event`/`order-book-snapshot` re-registration gotchas above.

**Redo for the other delta feeds — the gap-drop mechanism is exchange-agnostic, so there is NO
per-exchange CODE to repeat.** The reset marker → empty book ([[book-builder]]) → drop-from-union
([[aggregator]]) path all lives in the exchange-agnostic gap branch, and the enum fix is a one-time
global schema change. What IS per-delta-feed is the LIVE verification: repeat the
snapshot→updates→gap live test for each delta feed — **ex1 nobitex, ex6 bybit, ex8 okx** — and
confirm that exchange drops out of `p{id}-{side}` on the gap and returns on the next snapshot/resync.
**ex2 bitpin joined this list 2026-07-25** — it was re-classified from snapshot-only to
REST-snapshot + WS-delta exactly like ex1, and needed NO job-2 code change (the `baselinePending`
resync + null-seq `out_of_order` guard are exchange-agnostic), see [[pair-extractor]].
(The remaining snapshot-only feed ex4 and the no-ordering ex3 never hit the gap branch, so they
never emit a reset. **ex5 bitget LEFT this list 2026-08-22** — it became a snapshot/update delta
feed and now does hit it; see the jump-tolerance note below.) As of 2026-07-22 only the enum fix + re-registration are done; **no delta feed has been
verified live yet.**

## The ordering guards are suspended during a resync (2026-08-19)

`out_of_order` (null-seq) and `stale_or_duplicate` (sequenced) both now sit behind
`!resyncOutstanding()`. They only protect a book that is worth protecting, and once a gap has
emitted its `RESET` there isn't one. Leaving them armed deadlocked the key permanently — the full
mechanism, and why `lastEventTime` made it unrecoverable, is in [[project_control_plane]]. **If you
ever tighten these guards again, the invariant to preserve is: a key with an outstanding
`snapshot_request` must always have SOME path back to an accepted snapshot.**

## Gotchas (all cost real debugging time 2026-07-15)

- **`rejected-order-book-event` registry subject was STALE (v1, no `pipeline_timings`)** → the
  reject sink NPE'd (`Schema.getField("pipeline_timings")` null) in
  `RawOrderBookEventSerializer.toGenericRecord` — exactly the job-1 class of bug, different subject.
  The valid path was fine (`raw-order-book-event` was already re-registered to v2 during the job-1
  fix). Fix: re-registered `schemas/rejected_order_book_event.avsc` → v2. **Same standing rule as
  [[pair-extractor]]: after any raw-pipeline schema edit, re-run `scripts/warmup.sh` (or POST the
  affected subject) — the serializers fetch the schema from the registry at runtime.** `order-book-
  snapshot` is still v1 (harmless until job 5 / M6, which will hit the same wall — re-register then).
- **Smoke producing Avro directly**: `kafka-avro-console-producer` needs
  `--property avro.use.logical.type.converters=true` or it ClassCastExceptions — its JSON reader
  turns `event_time` (timestamp-millis) into an `Instant` that the serializer then can't write.
- **Newly-discovered topic seeks to EARLIEST, not `latest`** (observed): a topic-pattern
  `KafkaSource` with `OffsetsInitializer.latest()` applies `latest` only to partitions present at
  STARTUP; a topic created after the job starts is picked up by periodic discovery (~3 min interval
  here) and its new split seeks to EARLIEST (reprocesses history). So a live smoke against a brand-
  new synthetic topic is flaky until the job is (re)submitted WITH the topic already existing — then
  it's an initial partition at `latest`. jobs 3–6 smokes: create the input topic before submitting.

## E2E smoke test — RAW-IN whole-chain model (rewritten 2026-07-15)

`flink/normalizer/smoke-type-validator.sh` follows the **normalizer smoke rule** (see
[[normalizer-scaffold]]): send **raw exchange payloads ONLY** (to `ex{id}-raw`), let the WHOLE
Flink chain run (job1 pair-extract → job2 type-validate → …), capture the topic of the job under
test, and stamp **event_time = wall-clock execution time** so per-stage `pipeline_timings` are
verified as real, monotonically-increasing processing times. Prior version produced job 2's input
DIRECTLY to `ex99-p99-raw-flink` — that bypassed job 1 (so `pair_extract_*` had to be faked with
sentinels) and could never catch a real upstream-stamping bug.

**Why ex99/p99 was DROPPED:** under raw-in, a synthetic exchange can't be used — job 1
`Parsers.byExchangeId()` only knows ex1–6+8, so `ex99-raw` is dropped (`dropped-no-parser`) and
never reaches job 2. The smoke now drives **ex8 (OKX)** because `OkxParser` reads one `ts` field
that becomes **BOTH `event_time` AND `sequence_id`** (jump 300) — so setting `ts = now` gives an
execution-time event_time *and* full sequence-lifecycle control in one field. OKX `BTC-USDT`
resolves to **pair_id 1** in the warmed DB, so the live key is **(8, 1)** (real, not synthetic).
Idempotency across repeat runs is kept the same way: `ts = now` (epoch millis `date +%s`×1000;
macOS has no `%3N`) is strictly increasing, so the baseline is always fresh and the stale case
(`ts=1`) always rejects. **Prereq: BOTH jobs must be RUNNING** (the smoke checks both), DB warmed,
and no competing live OKX feed writing `ex8-*`.

**8 cases IN ORDER on key (8,1), one full delta lifecycle** (OKX jump 300, contiguous ts = prev+300):
snapshot baseline→valid, two contiguous updates→valid, gap (ts=now+4·300, expected now+3·300)→
dead-letter `sequence_gap`, next update→`awaiting_snapshot`, newer snapshot (now+10·300)→valid
re-sync, contiguous update after re-sync→valid, stale snapshot (ts=1)→`stale_or_duplicate`.
Assertions verify: `event_time == the ts we sent`, and the **timing chain**
`event_time ≤ pair_extract_in ≤ pair_extract_out ≤ type_validate_in ≤ type_validate_out` (all real,
stamped by the two live jobs — no sentinels). On rejects (which wrap the original under `.event`),
upstream `pair_extract_*` PRESERVE and `type_validate_out` stays null. Raw is produced as plain JSON
via `kafka-console-producer` (verbatim topic, no schema registry); output topics are read Confluent-
Avro from their pre-produce end offset (determinism trick). Decoded `pipeline_timings` union key is
**namespace-qualified**: `.pipeline_timings["io.tibobit.orderbook.PipelineTimings"].<field>.long`
(on rejects: `.event.pipeline_timings[…]`). **Smoke 8/8 green 2026-07-15.** Note: cases can
transiently FAIL on the 25s consumer read-timeout (now a two-hop chain) — re-run; not a logic bug.

**Why:** jobs 3–6 consume `-type-validated-raw-flink` and assume these events already passed
sequence validation (job 5 does NOT re-check sequences). **How to apply:** the gap/jump rule keys
off `sequence_jump` stamped by job 1's parsers — if an exchange's jump changes, fix it in the
parser (job 1), not here; job 2 is exchange-agnostic.

**2026-08-03 — `simulation` pass-through.** Valid events forward the same object, so the flag rides for free; but `emitReset` builds a FRESH event, so it explicitly copies the gap event's flag — otherwise emptying a simulated exchange's book would emit a record claiming to be live. See [[simulation-flag]].

**2026-08-22 — the contiguity check became a WINDOW, for ex5/bitget.** bitget's new `depth`-channel
sample dropped `seq` from the wire entirely (the `checksum` that replaced it is a CRC integrity
value, not a sequence), so its ordering field is now the inner millisecond `ts`. A clock never
lands on an exact multiple of a cadence, so `seq == last + jump` could not work: the rule is now
`last + jump - tol <= seq <= last + jump + tol`, with `tol` from the new
`sequence_jump_tolerance` schema field (`default: 0`, so a BACKWARD-compatible evolution).
**ex5 stamps jump 600 / tolerance 10; every other exchange stamps 0, which collapses the window
back to the exact check** — ex6's jump 1 and ex8's jump 300 are unchanged, and that is pinned by
`zeroToleranceIsTheExactCheck`. Job 2 stays exchange-agnostic: it reads the tolerance off the
event, it does not know which exchange sent it. ⚠ **re-register `raw-order-book-event` AND
`rejected-order-book-event`, then resubmit** — same serializer-caches-the-write-schema trap as
`pipeline_timings` / `simulation` / `reason`.

**⚠ The window guards EVERY transition, snapshot→update included — a deliberate user decision
(2026-08-22) taken over a flagged objection.** The consequence is real and visible in bitget's own
capture: its first update follows the snapshot by **22 ms**, nowhere near 600, so on the live feed
that burst is dead-lettered `sequence_gap`, empties the book via the reset, and asks the control
plane for a fresh snapshot. If the resync snapshot is itself followed by a close-behind update, ex5
can loop reset → request → snapshot → gap. The alternative offered and declined was to exempt the
snapshot→update transition by reusing the existing `baselinePending` bootstrap (what ex1/ex2 do).
Pinned as an EXPECTED result in `TypeValidateFunctionTest.capturedBurstAfterSnapshotIsAGap` so the
behaviour is documented rather than surprising; open risk in todo.md, to settle against the live
feed's real cadence.

**2026-08-23 — audit: "a snapshot is ORDERED, never jump-checked" (user-stated invariant).** The
user restated the rule for every exchange, not just ex5: an arriving snapshot (REST or WS) is
validated on **ordering alone** — is its `sequence_id` newer than the last accepted one — and
`sequence_jump` must never be applied to it. Gap detection has exactly **two legal sites**:
snapshot → next update, and update → update. An update → snapshot transition gets the ordering
check and nothing else.

**Audit result: the code already conformed — no production behaviour changed.** Verified two ways.
(a) Job 2 is the ONLY sequence validator in the platform: `getSequenceJump()` appears in exactly
one expression, `long expected = last + event.getSequenceJump()`, inside the update branch;
jobs 3–6 never read `sequence_id` for validation (job 5 only copies it onto the snapshot record).
(b) Per-exchange, every snapshot lands in a branch that cannot reach that expression:

| ex | snapshot `sequence_id` | jump | snapshot check |
| --- | --- | --- | --- |
| 1 nobitex (REST), 2 bitpin (REST), 3 wallex | `null` | 0 | event-time order only |
| 4 ramzinex | `pub.offset` | 0 | `seq > lastSeq` only |
| 5 bitget (WS `depth` **and** REST depth) | `ts` / `data.ts` | 600 (tol 10) | `seq > lastSeq` only |
| 6 bybit | `data.u` | 1 | `seq > lastSeq` only |
| 8 okx | `ts` | 300 | `seq > lastSeq` only |

**What was actually missing was PROOF.** The pre-existing snapshot tests all used
`snapshotFeed(…)`, i.e. **jump 0**, where `last + 0` is satisfied by nothing and a leaked jump
check would be invisible; every delta-feed snapshot in the suite either arrived first
(`lastSeq == null`, no check possible) or during a pending resync (`resyncPending()` exempts the
guard). So a regression that moved the window check above the snapshot branch would have passed
the whole suite. Four tests now pin it (48 → 52), all on **nonzero** jumps:
`snapshotAfterUpdatesIgnoresTheJump` (ex6 jump 1, snapshot lands far PAST `last+jump`),
`snapshotShortOfTheJumpIsStillAccepted` (ex8 jump 300, snapshot lands +7 — forward but far SHORT
of the jump, the case an inequality-style leak would miss), `bitgetResyncSnapshotIsNotWindowChecked`
(ex5, a resync ts 143 ms off the 600 grid, then the next update measured from the SNAPSHOT), and
`snapshotOrderingBoundaryIgnoresTheJump` (jump 300: equal → `stale_or_duplicate`, +1 → accepted).
Two mutations confirm they bite: making the snapshot branch enforce `seq == last + jump` fails 6
tests (all 4 new + 2 old), and making a snapshot not re-anchor `lastSeq` fails 7.

The invariant is now stated in the class javadoc's snapshot bullet and as a one-line comment on
the branch itself, so the next person to "unify" the two branches has to read it first.

**⚠ Note the interaction with the 2026-08-22 decision above, which is UNCHANGED.** The user's rule
explicitly keeps snapshot → next update as a legal gap site, which is exactly the transition that
dead-letters bitget's real +22 ms post-snapshot burst. So
`capturedBurstAfterSnapshotIsAGap` still stands and the ex5 open risk in todo.md is still open —
this audit narrowed nothing about it. **Why:** ordering-only-on-snapshots and window-on-the-next-
update are independent rules that happen to meet at the same event pair. **How to apply:** if the
ex5 risk is ever settled by exempting the post-snapshot update, that is a change to the UPDATE
branch (or a `baselinePending`-style bootstrap), never to the snapshot branch.

**2026-08-23 (2) — LIVE DIAGNOSIS on the dev server: the ex5 resync loop, and both of its causes.**
The two open risks logged above stopped being theoretical. Measured on
`tibobit-data-collector-afra` (ssh via the `asus` jump host; `/opt/data-collector`, all containers
under `sudo docker`): `control-plane` contained nothing but
`snapshot_request / sequence_gap / ex5 / pair 1`, and `ex5-p1-rejected-flink` alternated
`sequence_gap` / `awaiting_snapshot` forever.

**Evidence.** 4569 consecutive `ex5-raw` frames over 36 minutes, BTCUSDT (the only ex5 market with
`status = subscribe`, so the stream needed no de-interleaving):

- **The WS `depth` channel sends NO snapshots** — 3538 updates, **0** `action:"snapshot"` frames.
  The REST endpoint is ex5's only baseline source. That is what turned both mistakes below from
  wasteful into fatal, and it contradicts the 2026-08-23 answer "yes, both exist, keep as-is".
- **The REST `data.ts` is a different clock.** Against the WS update just before it: range
  −706..+662 ms, **behind 57.1%** of the time. The update just after it landed inside the old
  `600 ± 10` window only **9.9%** of the time.
- **update→update is bimodal**: a 575–625 mass **plus a real 725–775 cluster**. Only **93.16%**
  inside `600 ± 10`; `[540, 760]` covers **99.83%** of 3537 transitions.

**The loop**: REST snapshot accepted (only because a resync was pending) → its ts seeds the window
from the wrong clock → next update ~620–700 ms later is outside `600 ± 10` → `sequence_gap` →
reset empties the book → `snapshot_request` → NiFi answers with another REST snapshot → repeat.
Median REST→REST interval 1201 ms, with 777 of 1030 cycles being exactly 2 updates long — the
`gap, awaiting_snapshot` signature. Replaying the capture through the code: **1030 book resets and
1031 requests in 36 min (28.6/min)**, 2641/4569 events accepted.

**The fix** (user chose the widen option over ordering-only, 2026-08-23): `BitgetParser` now stamps
the REST snapshot **null-seq, jump 0** — the `baselinePending` bootstrap ex1/ex2 already use, so the
two clocks are never compared — and the WS window widened to **jump 650 ± 110**. Replaying the same
capture: **4 resets and 5 requests (0.1/min)**, 3962/4569 accepted. The 583 `out_of_order` rejects
that appear are the redundant REST bodies nobody asked for, correctly dropped.

**NO job-2 change was needed for any of this.** Both causes were job-1 stamping; job 2 is
exchange-agnostic and read the values off the event, exactly as designed.

**Why:** a wall clock is not a sequence, and two endpoints' wall clocks are not the same sequence.
Any future feed whose "sequence" is a timestamp should be assumed non-contiguous until measured.
**How to apply:** before trusting a jump/tolerance for a clock-sequenced feed, pull a few thousand
live frames and plot the interval distribution — the 2026-08-22 `600 ± 10` came from two captured
frames 22 ms apart and was wrong in both directions. And a REST body that answers a resync must be
null-seq unless its clock is provably the same one the deltas use. See [[project_pair_extractor]].

## 2026-08-31 — job 2 watches for SILENCE (staleness)

`TypeValidateFunction` stays a single-input `KeyedProcessFunction`; the rule set above is
untouched. What was added is `onTimer`: every key holds ONE processing-time deadline at
`lastArrival + staleness_threshold_seconds`, and passing it emits the same `RESET` a sequence gap
does and asks the control plane for a snapshot with `reason: "stale"`. Three new state fields
(`lastArrivalMs`, `lastSimulation`, `stalenessTimerAt`) — 4 → 7. **All 53 pre-existing tests pass
unchanged**, which is the proof the rule set was not touched; job 2 is now 66 tests.

`lastArrivalMs` is stamped on EVERY arriving event whatever its verdict — a key rejecting
everything is alive and already re-asking on the rejection path, so calling it stale too would make
one fault ask twice.

**Only markets that have already spoken are watched.** The design, the reduction that got it there
(a `no_data_received` reason and the whole tick-stream machinery were built and then removed), the
watch-list dependency and the surviving mutations are all in [[project_control_plane]] — this note
exists so nobody looks for them here.

## 2026-09-01 — the unwatched branch is no longer a bare return

`onTimer`'s "market is not on the watch list" branch used to `return` and walk away. It now emits a
`RESET` first, because stopping watching and leaving the book standing means every downstream
consumer keeps a book with no feed behind it forever. Two new states (`lastExchangeId`,
`lastPairId`, 7 → 9) exist only so that branch can name the market without parsing the key. No
`snapshot_request` on this path — dropping a market is not resyncing it. Full write-up, the race
with `REFRESH_INTERVAL_MS` and the surviving mutation are in [[project_control_plane]].
