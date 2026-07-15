---
name: raw-pipeline-decision
description: FINAL decision (2026-07-13) — NiFi publishes verbatim raw exchange payloads; 6 chained Flink jobs in one Maven multi-module project normalize them into the consolidator's price_level_event input stream
metadata:
  type: project
---

# Raw Data Normalization Pipeline — FINAL Decision (2026-07-13)

## Context

NiFi stops normalizing exchange data. It will publish **verbatim raw exchange payloads** to Kafka
(one topic per exchange), and a new Flink pipeline reproduces the normalization, ending in the
exact `price_level_event` stream on `ex{exchange_id}-p{pair_id}-{side}` topics that
`flink/orderbook-consolidator` already consumes ([[kafka-topic-strategy]], [[avro-schema-orderbook]]).
The consolidator and `web/` stay untouched.

## Structure decisions (FINAL, 2026-07-13)

1. **Multiple jobs, one per pipeline step, chained through Kafka topics.** Rationale (user):
   **accuracy > latency right now** — every intermediate topic is an audit point (inspect exactly
   what a job received vs emitted). Merging jobs later is a cheap refactor; splitting is hard.
2. **ONE Maven multi-module project** for the whole pipeline: parent pom + `common/` (shared
   models, Avro serde/schema loading, Kafka source/sink factories, [[bigdecimal-rules]] helpers)
   + one `job-*/` module per job, each producing its own shaded jar; build one job via
   `mvn -pl job-<x> -am package`. NOT per-job self-contained projects (would copy shared code
   3–6× and drift), NOT one-jar-many-mains (every jar carries every job's deps).
3. **Existing projects untouched**: `flink/orderbook-consolidator/` and `flink/orderbook-job/`
   keep their self-contained layout.

## Pipeline steps (FINAL, 2026-07-13)

NOTE ON NUMBERING: the user's step numbers drifted during discussion (an orderbook-build step was
added mid-decision as "step6/step7"). The topic names below are the ground truth, not the numbers.
6 Flink jobs total:

| # | Job | Consumes | Emits | Does |
|---|-----|----------|-------|------|
| 0 | (NiFi, not ours) | exchanges | `ex{id}-raw` | verbatim raw exchange payload, one topic per exchange |
| 1 | pair extraction | `ex{id}-raw` | `ex{id}-p{id}-raw-flink` | raw topic mixes many markets; parse per-exchange payload, resolve exchange symbol → `pair_id` (via `exchange_markets.market` → `market_id`), split per pair |
| 2 | type validation | job 1 | `ex{id}-p{id}-type-validated-raw-flink` | determine snapshot vs update; apply the existing `sequence_id`/`sequence_jump` rules (stale/dup drop, gap → await snapshot — semantics as in orderbook-job Phase 3, see todo.md); **invalid events → dead-letter topic** (DECIDED via Q&A 2026-07-13; suggested name `ex{id}-p{id}-rejected-flink`, final name TBD) |
| 3 | rebase | job 2 | `ex{id}-p{id}-rebased-flink` | rebase price & amount using `exchange_markets.price_amount_rebase` / `volume_amount_rebase` (verified in `postgres/01_schema.sql`, INT NOT NULL DEFAULT 0 — see [[db-schema]]) |
| 4 | precision | job 3 | `ex{id}-p{id}-applied-precision-flink` | normalize precisions per `markets.price_precision` / `markets.quantity_precision` (user wrote "applied-pricision" — ASSUMED corrected spelling "precision", confirm before provisioning topics) |
| 5 | orderbook build | job 4 | `ex{id}-p{id}-orderbook-snapshot-flink` | stateful: apply snapshot/update semantics to maintain the full per-(exchange,pair) book; emit the **full order book** on each change |
| 6 | price-level emit | job 5 | `ex{id}-p{id}-{side}` (existing consolidator input, `price_level_event.avsc`) | flatten full books into single price-level events |

## Q&A decisions (2026-07-13)

- **Raw wire format**: verbatim exchange payload, format differs per exchange; job 1 owns
  per-exchange parsing. (Also finally settles the "NiFi producer format unverified" risk for the
  NEW pipeline — the old direct-to-`ex{id}-p{id}-{side}` NiFi path is being replaced.)
- **Rejects in job 2**: dead-letter topic, fits accuracy-first goal.
- **Snapshot flattening**: solved structurally — job 5 materializes the full book first, job 6
  flattens from full books instead of flattening raw snapshot/update events.

## Implementation Q&A decisions (2026-07-13, round 2)

- **Rebase formula (job 3)**: `value × 10^rebase` — exponent shift, `rebase=0` = unchanged,
  negative shifts left. Implement with `BigDecimal.scaleByPowerOfTen(rebase)` (exact, no
  rounding, [[bigdecimal-rules]]).
- **Parse point**: **job 1 parses fully.** Each exchange's verbatim payload becomes ONE common
  structured Avro event (exchange/pair ids, type, seq fields, asks/bids levels) right in job 1;
  the `-raw` suffix on later topics just means "not yet rebased/precision-normalized". One shared
  schema serves job 1→4 output topics; per-exchange parsing logic lives ONLY in job 1.
- **Precision rounding (job 4)**: **truncate** — `setScale(precision, RoundingMode.DOWN)`; never
  invents value that wasn't on the wire.
- **DB reference data (jobs 1/3/4)**: **periodic refresh** — reload the lookup tables from
  Postgres every N minutes inside the job (no restart needed to pick up new
  markets/rebase/precision rows). Interval TBD at implementation (suggest env-configurable,
  default ~1min). Note: unlike the consolidator (deliberately Postgres-free), these jobs DO
  depend on Postgres.
- **Non-book messages in raw topics (RULE, FINAL 2026-07-14)**: `ex{id}-raw` topics carry
  snapshot and update messages, but MAY also contain other data (e.g. subscription acks,
  pings, heartbeats, other channels). Job 1 (which owns parsing) **silently discards anything
  that is not a recognized book message** — whitelist parse: drop, never crash, never
  dead-letter. User decision 2026-07-14: capturing example non-book frames per exchange is
  NOT required; the drop rule is "not a recognized book frame ⇒ discard".

## Raw payload reality check (2026-07-13, from live `ex{id}-raw` topics)

**⚠ SAMPLES RESET 2026-07-14 (user decision): the 2026-07-13 bulk capture was thrown away —
samples are being rebuilt one exchange at a time into `sample-raw-data.md` (old content
recoverable from git). Until an exchange's sample is re-captured, treat the headlines below
as UNVERIFIED leads to re-confirm, not ground truth.** Original capture source: server
kafka-ui (192.168.150.104:8080), latest-200 per topic.

- **7 live exchanges, not 3**: 1=nobitex, 2=bitpin, 3=wallex, 4=ramzinex, 5=bitget, 6=bybit,
  7=ompfinex (server `exchanges` table; 8=okx exists in DB, no topic yet). The market key inside
  each payload's channel/topic string == `exchange_markets.market` exactly.
- **Three regimes**: full-snapshot-every-msg (**ex1 nobitex + ex2 bitpin + ex4 ramzinex +
  ex5 bitget RE-CONFIRMED 2026-07-14** post-reset, samples + parsing notes in
  `sample-raw-data.md` — ex1/ex2/ex4 Centrifugo but different channel keys (ex4's is a
  NUMERIC market id `orderbook:12`; its `sells` sort descending = best ask LAST); ex5 NOT
  Centrifugo — `action`/`arg`/`data` shape with an explicit `action: "snapshot"`
  discriminator (the only exchange with one) and a real `seq`+`pseq` field; ex1 multi-doc
  records still to re-verify),
  full-snapshot-per-SIDE with NO seq
  fields (**ex3 wallex RE-CONFIRMED 2026-07-14** post-reset — buyDepth/sellDepth are separate
  messages, `["{market}@{side}", [levels]]` array envelope, sample + parsing notes in
  `sample-raw-data.md`), delta-with-seq — TWO exchanges (**ex6 bybit RE-CONFIRMED
  2026-07-14** post-reset — `type: "snapshot" | "delta"` discriminator, NOT Centrifugo
  (`topic`/`ts`/`type`/`data`/`cts` shape), sides `b`/`a` string pairs, **sequence id = `u`
  with jump 1 (user-confirmed)** while `data.seq` is non-contiguous metadata; qty="0" =
  delete still a lead for bybit — no delete frame captured yet; **ex8 okx CAPTURED
  2026-07-14** — `action: "snapshot" | "update"` discriminator in a bitget-family
  `arg`/`action`/`data`-array envelope, grouped book (`books-grouped`, `grouping: "1"` →
  integer prices), market key `arg.instId` = `BTC-USDT` (DASH — unlike every other
  exchange), string levels, **sequence id = `ts` (STRING epoch-millis inside data[0]) with
  jump 300 (user-confirmed)** — no counter field at all, and **qty="0" delete CONFIRMED on
  okx's wire** (first delete frame in the set); ex7 postponed). Job 2's per-exchange seq
  config must support both a counter (`u`/1) and a millis timestamp (`ts`/300). The shared
  Avro event + jobs 2/5 must cover all three regimes.
- **wallex + ramzinex send prices/qtys as JSON numbers** — BigDecimal must come from the decimal
  literal (Jackson `USE_BIG_DECIMAL_FOR_FLOATS`), never double ([[bigdecimal-rules]]).
  (BOTH RE-CONFIRMED 2026-07-14 — wallex and ramzinex.)
- **ex1 multi-doc records: CLOSED 2026-07-14 (user)** — ex1 records always contain ONE JSON
  document; NO multi-doc splitting in job 1 (the discarded-capture
  2-newline-concatenated-docs lead was an artifact).
- **ompfinex (ex7) POSTPONED 2026-07-14** (team decision — known issue with its raw data).
  **okx (ex8) ADDED to scope 2026-07-14** (previously in DB with no topic — caveat settled
  same day: snapshot+update samples captured, feed is live). Initial pipeline scope =
  **ex1–ex6 + ex8**. Deferred with
  ex7: initial-book fixture + `U`/`u` range-gap investigation. Job 1's `^ex[0-9]+-raw$` source
  pattern still matches `ex7-raw` — exclude-vs-drop decision noted in todo.md M2.
- **Job-2 validation scope (REVISED 2026-07-14, user — supersedes the same-day
  "no checks for snapshot feeds" decision)**: two distinct rule kinds.
  (a) **Gap/jump validation** ONLY for the two delta feeds — **ex6 bybit (`u`, jump 1)**
  and **ex8 okx (`ts`, string epoch-millis, jump 300)**; on gap: drop until the next
  snapshot re-syncs the book. bybit `u` gaps in the topic = real upstream/NiFi-side loss
  (~30% of consecutive msgs in the discarded capture), but the user decided to **SKIP the
  NiFi investigation** — the gap rule absorbs it.
  (b) **Out-of-order (staleness) check** for snapshot feeds — user: "we should be aware of
  out of order messages … using ts, seq, offset or any field we have". Per (exchange,
  pair): drop any snapshot whose ordering value is not greater than the last seen. No
  gap/jump rule — gaps self-heal on the next snapshot. Ordering fields: ex1/ex2/ex4 =
  Centrifugo `pub.offset` (ex1 fallback `lastUpdate`, ex2 fallback `event_time`);
  ex5 = `seq` (fallback inner `ts`; the discarded capture's non-monotonic `seq` is exactly
  what this check drops, not a reason to distrust the field). **ex3 wallex has NO ordering
  field at all** — no out-of-order protection possible (Kafka offset = arrival order).
- Fixtures: RESET 2026-07-14 — being rebuilt per exchange (checklist in todo.md M0); the
  earlier bybit snapshot+delta and bitget snapshot captures were discarded with the rest.
- Kafka records: key=null, 1 partition per topic.

## Per-step latency tracking (REQUIREMENT, 2026-07-15)

Goal (user): measure how long each event spends at every step, so processing delay is
observable end-to-end — exchange event → each job → final hand-off. Design decisions locked
with the user before implementation:

1. **Ingest + emit per step.** Each job records when it read the event off its input topic
   (`_in`) and when it wrote to its output topic (`_out`) — separating in-job compute from
   Kafka transit, which a single stamp could not.
2. **Named fields, NOT an array** (user rejected the array as ambiguous + query-hostile,
   2026-07-15). The pipeline is a FIXED, known 6 steps — model it as one nested record
   `PipelineTimings` with one explicitly-named nullable `timestamp-millis` field per
   step×phase, all `default: null`:

   ```
   PipelineTimings:
     pair_extract_in,  pair_extract_out
     type_validate_in, type_validate_out
     rebase_in,        rebase_out
     precision_in,     precision_out
     book_build_in,    book_build_out
     level_emit_in,    level_emit_out
   ```

   Every event carries ONE `pipeline_timings` field. **Implemented wire type (2026-07-15):
   `["null", PipelineTimings]` with `default: null`** — a nullable union was chosen over a
   non-null record so the added-field default is one token and cleanly backward-compatible;
   writers always emit a NON-null record (per-stage fields null until run), the null branch
   only appears for data written before the field existed.
   Each job fills ONLY its own two fields; the rest stay `null` until that stage runs — so
   `null` unambiguously means "not yet reached this stage" (the schema names all stages up
   front). Adding/removing a stage is then an explicit Avro schema change (add optional field
   `default: null` = backward-compatible), not the silent index-shift the array caused. A
   `map<string,…>` was also rejected — no schema guarantee of which keys exist, still needs
   key-lookup to query.
3. **Timings run through the final hand-off** — `price_level_event.avsc` (job 6 output, the
   frozen consolidator input) also gains the `pipeline_timings` field. Nullable/defaulted, so
   backward-compatible on the wire (the consolidator + `web/` ignore the added field); one
   nested field is far less intrusive on the frozen schema than 12 flat ones. This is the one
   deliberate edit to that frozen schema.

**Anchor is the existing `event_time`** (exchange-reported). `pair_extract_in` already IS
"the time the event came from the raw topic", so NO separate top-level ingest field is added.

Derived metrics (computed by any consumer/audit — direct field paths, nothing below is stored):
- exchange → pipeline lag = `pair_extract_in − event_time`
- in-job processing at a step = `<stage>_out − <stage>_in`
- Kafka transit into a step = `<stage>_in − <prev_stage>_out`
- cumulative latency at a step = `<stage>_out − event_time`
- **total end-to-end** = `level_emit_out − event_time`

Carriers: `pipeline_timings` goes on `raw_order_book_event` (jobs 1–4), `order_book_snapshot`
(job 5), the inlined event in `rejected_order_book_event` (kept field-for-field identical),
and `price_level_event` (job 6). `PipelineTimings` is duplicated field-for-field across the
avsc files (same rule as `PriceLevel`/`Type` — identical redefinitions only). Fan-out note:
job 6 flattens one book into many price-level events; they all carry the same upstream timings
+ job 6's own `level_emit_*`.

Implementation (each job, at wiring time): capture `<stage>_in` at the start of
`processElement`/`map`, set `<stage>_out = now` just before `collect`. **Clock caveat**:
`_in`/`_out` are wall-clock (`System.currentTimeMillis`) on whichever TaskManager runs the
operator — fine single-node; cross-machine clock skew could distort transit deltas (flagged,
non-blocking).

**DONE 2026-07-15** (schemas + common models + job 1): `PipelineTimings` POJO
(`model/`) + a `pipelineTimings` field on `RawOrderBookEvent`/`OrderBookSnapshot` (existing
constructors keep an empty instance, so no call site changed); shared
`serde/PipelineTimingsRecords` maps it to/from the nested Avro record, reused by all three
serde (rejected inherits it via the inline event); **job 1 stamps `pair_extract_in/out`** in
`PairExtractFunction.flatMap`. Jobs 2–6 stamp their own two when built. Read timestamps back as
raw `Long` (the Confluent generic deserializer returns raw longs for timestamp-millis, matching
the existing `event_time` handling). 47 tests green. Not yet re-verified against a live registry
compat check (do at M8).

## Design notes / open items (record before implementing)

- **Job 6 vanished-level handling**: emitting only the current book's levels would leave deleted
  prices lingering in the consolidator's stage-1 MapState forever (it only removes on qty=0).
  Job 6 likely needs state (last emitted book per key) to diff consecutive books and emit qty=0
  events for disappeared levels. Decide at job-6 implementation time.
- **Job 4 truncate-to-zero hazard**: truncating a small-but-nonzero quantity to the target
  precision can yield exactly 0 — which downstream means "delete this level". Decide whether
  that's acceptable or needs a floor. Flag at job-4 implementation time.
- Avro schemas + registry subjects for every intermediate topic (raw topic is schema-less
  verbatim bytes by definition).
- Topic provisioning/retention: `ex{id}-raw` SETTLED 2026-07-14 — `scripts/warmup.sh` creates
  one per subscribed exchange (distinct exchange_ids from the same exchange_markets query,
  1 partition), retention 7 days "for now" (user). Note this is DB-driven, so a subscribed
  ex7 would get a topic too — harmless while ex7 is postponed. Intermediate/dead-letter
  topic families + their retention still open (M8).
- Pipeline/project directory name under `flink/` — PROPOSED `flink/normalizer/`, not confirmed.
- Deploy story — PROPOSED: one parameterized `run-job.sh`/`Dockerfile` at the pipeline root
  taking the module name (all modules share the same Flink base), not per-module copies.
- Dead-letter topic — PROPOSED name `ex{id}-p{id}-rejected-flink`.
- Whether old `flink/orderbook-job/` + the old NiFi normalization path get retired after cutover.

Detailed implementation task breakdown lives in `todo.md` (rewritten 2026-07-13 to contain only
this pipeline; prior phases 1–5 history removed — recoverable from git, and the R3-postponed
block remains in [[orderbook-consolidator-decision]]).
