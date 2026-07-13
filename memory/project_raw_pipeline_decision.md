# Raw Data Normalization Pipeline — FINAL Decision (2026-07-13)

## Context

NiFi stops normalizing exchange data. It will publish **verbatim raw exchange payloads** to Kafka
(one topic per exchange), and a new Flink pipeline reproduces the normalization, ending in the
exact `price_level_event` stream on `ex{exchange_id}-p{pair_id}-{side}` topics that
`flink/orderbook-consolidator` already consumes ([[kafka-topic-strategy]], [[avro-schema]]).
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
- Topic provisioning/retention for the new topics (`scripts/warmup.sh` extension?).
- Pipeline/project directory name under `flink/` — PROPOSED `flink/normalizer/`, not confirmed.
- Deploy story — PROPOSED: one parameterized `run-job.sh`/`Dockerfile` at the pipeline root
  taking the module name (all modules share the same Flink base), not per-module copies.
- Dead-letter topic — PROPOSED name `ex{id}-p{id}-rejected-flink`.
- Whether old `flink/orderbook-job/` + the old NiFi normalization path get retired after cutover.

Detailed implementation task breakdown lives in `todo.md` (rewritten 2026-07-13 to contain only
this pipeline; prior phases 1–5 history removed — recoverable from git, and the R3-postponed
block remains in [[orderbook-consolidator-decision]]).
