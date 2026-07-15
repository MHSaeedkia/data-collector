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
rejects → dead-letter `ex{id}-p{id}-rejected-flink` (`rejected-order-book-event`). 11 harness
tests + live smoke 3/3 green.

## The rule set (the important decision)

The todo.md M3 sketch ("snapshot = unconditional baseline") was RECONCILED with the later,
authoritative revised job-2 scope in [[raw-pipeline-decision]]. **The discriminator is `type` +
`sequence_id == null`, NOT `sequence_jump` alone** — a delta feed's SNAPSHOT message also carries
jump>0 (verified in `BybitParser`/`OkxParser`: jump is hardcoded per parser, not per message
type). So `TypeValidateFunction.processElement`, with `ValueState {Long lastSeq, Boolean
awaitingSnapshot}`:

1. **`sequence_id == null`** (ex3 wallex, the ONLY feed with no ordering field) → pass through
   unchecked, no state touched. [[pair-extractor]] leaves ex3's `sequence_id` null on purpose.
2. **`type == "snapshot"`** (a fresh baseline, but out-of-order/duplicate dropped):
   `lastSeq != null && seq <= lastSeq` → reject `stale_or_duplicate`; else accept, set
   `lastSeq = seq`, clear `awaitingSnapshot`. This is the snapshot-feed staleness check AND a
   delta feed's re-sync in one branch.
3. **`type == "update"`** (delta feeds ex6/ex8 only): `lastSeq == null` → reject `no_baseline`;
   `awaitingSnapshot` → reject `awaiting_snapshot`; `seq == lastSeq + sequence_jump` → accept,
   `lastSeq = seq`; `seq <= lastSeq` → reject `stale_or_duplicate`; else (any other forward jump)
   → reject `sequence_gap` + set `awaitingSnapshot` (every update rejected until the next snapshot
   re-syncs). Reasons: `stale_or_duplicate` / `sequence_gap` / `awaiting_snapshot` / `no_baseline`.

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

## E2E smoke test

`flink/normalizer/smoke-type-validator.sh` — job 2 is STATEFUL, so the smoke must be idempotent
against persistent keyed state and repeat runs. It uses a synthetic key **ex99/p99** (collides
with no real feed) and a **monotonic `seq = now` (epoch millis via `date +%s`×1000; macOS has no
`%3N`)**, so every run's first snapshot is a fresh-or-newer baseline and the deliberately stale
case (`seq=1`) is always rejected. **8 cases run IN ORDER on the same key, walking one full delta
lifecycle** (jump 1 throughout, so each contiguous seq = prev+1): snapshot baseline→valid, two
contiguous updates (snapshot→update→update)→valid, a gap (seq=now+5, expected now+3)→dead-letter
`sequence_gap`, next update→dead-letter `awaiting_snapshot` (held until re-sync), newer snapshot
(now+10)→valid re-sync (clears awaiting), contiguous update after re-sync→valid, stale
snapshot(seq=1)→dead-letter `stale_or_duplicate`. This exercises the same gap/awaiting/recovery
path as the `gapThenAwaitingSnapshotUntilResync` unit test, but LIVE through both Avro sinks. Reads
the output topic from its pre-produce end offset (job-1 smoke's determinism trick). Decoded Avro
`pipeline_timings` union key is **namespace-qualified**:
`.pipeline_timings["io.tibobit.orderbook.PipelineTimings"].type_validate_out.long`.

**Why:** jobs 3–6 consume `-type-validated-raw-flink` and assume these events already passed
sequence validation (job 5 does NOT re-check sequences). **How to apply:** the gap/jump rule keys
off `sequence_jump` stamped by job 1's parsers — if an exchange's jump changes, fix it in the
parser (job 1), not here; job 2 is exchange-agnostic.
