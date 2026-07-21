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
consolidator dedups by strict `<` too). Seq-bearing snapshots (ex6/ex8) are unaffected — their old
snapshots are already caught by the seq `<= lastSeq` check. 16 harness tests (2 new). Still not run
live.

Rejects go to the `REJECTED` side output (`OutputTag<RejectedOrderBookEvent>`, a public static on
the function so job wiring + tests share it). No metrics counters (unlike job 1's drops — here the
dead-letter topic IS the audit record). Timings: stamps `type_validate_in` on entry and
`type_validate_out` before the main `collect`; rejects keep `type_validate_out` null (never emitted
onward), `rejectedAt` records the dead-letter time.

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
