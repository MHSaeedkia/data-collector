---
name: aggregator
description: 2026-07-21 — job-aggregator module (raw pipeline terminal job): consumes job 5 full books, unions across exchanges, emits the frozen p{id}-{side} web event; replaces job 6 + the consolidator
metadata:
    type: project
---

# Terminal job — cross-exchange aggregator (`flink/normalizer/job-aggregator/`, 2026-07-21)

Package `io.tibobit.normalizer.aggregate` (flat, [[normalizer-scaffold]] conventions). Consumes job
5's full per-exchange books `ex[0-9]+-p[0-9]+-orderbook-snapshot-flink` (`OrderBookSnapshot`, subject
`order-book-snapshot`), unions them across exchanges per `(pair_id, side)`, and emits the frozen
`p{id}-{side}` `consolidated-order-book-event` the web UI already consumes. **Replaces job 6
(level-emitter) + the deprecated orderbook-consolidator** — see plans/aggregator-gap-drop.md. 12
tests green (9 ported consolidator stage-2 + 3 splitter). Code+tests DONE; **not run live, not yet
deployed** (deploy wiring deferred — see below).

## Why this exists (the root-cause fix)

The old path did a **build→shred→rebuild round trip**: job 5 builds each exchange's full book, job 6
shreds it into per-level deltas, the consolidator's stage 1 reassembles the full book, stage 2
unions. The per-level delta representation has **no primitive for "drop this whole exchange"**, which
is why a sequence gap couldn't clear a diverged book ([[type-validator]] reset marker, [[book-builder]]
reset branch). This job feeds the consolidator's **stage-2 logic the full books directly**, skipping
the shred/rebuild — so job 2's reset ⇒ job 5 empty book ⇒ **empty `ExchangeBook` replaces the stored
entry and contributes nothing ⇒ that exchange drops out of the union**. That is the whole point.

## Shape

- `SnapshotSplitter` (flatMap): one `OrderBookSnapshot` → two per-side `ExchangeBook`s (asks, bids),
  each level stamped with the snapshot's `exchange_id`. Job 5 always emits both sides (possibly
  empty); a null side is defensively treated as empty. This split lets the ported stage-2 operator
  be reused near-verbatim.
- `CrossExchangeConsolidator` (ported verbatim, keyed `(pair_id, side)`): `MapState<exchange_id,
  ExchangeBook>`, replace-per-exchange, **union never-sum** (equal prices from different exchanges
  stay separate adjacent entries — the opposite of SQL GROUP BY, [[orderbook-aggregation]]), sort
  asks↑/bids↓ tie-break qty desc as BigDecimal ([[bigdecimal-rules]]), output `event_time` = max
  across contributing books. Sort direction chosen per-record from `side` (one operator serves both).
- Sink: inline `KafkaSink`, per-record topic `p{pair_id}-{side}`, `ConsolidatedOrderBookSerializer`
  (ported; now uses common `AvroSchemaLoader` + subject `consolidated-order-book-event`, the FROZEN
  web wire shape — do NOT alter). Models `ExchangeBook`/`ConsolidatedLevel`/`ConsolidatedOrderBook`
  ported into the module (not common — aggregator-local; the web consumes the Avro form, not the POJO).

## Deploy is deliberately NOT wired yet (dual-producer hazard)

The aggregator writes the SAME frozen `p{id}-{side}` topics the consolidator still writes. Adding it
to the active `docker-compose-normalizer.yml` / Makefile `refresh-normalizer` while the consolidator
is deployed would put **two producers on the web's topics**. So that wiring is deferred to the Part D
cutover, where the consolidator + job 6 are removed in the SAME change (swap, never add). `run-job.sh`
is module-driven, so `./run-job.sh job-aggregator` still runs it standalone for isolated smoke tests.

**warmup.sh no longer creates the consolidator-input topics (2026-07-22).** The old per-level
`ex{id}-p{id}-{side}` family was job 6's output = the consolidator's input; the aggregator consumes
job 5's `orderbook-snapshot-flink` directly, so those topics have no producer/consumer in the new
chain and their creation block was removed from `scripts/warmup.sh`. The FROZEN web-output family
`p{id}-{side}` (the aggregator's sink) is untouched and still created there — do not confuse the two.

**Why Java DataStream not SQL** (unchanged from the ratified consolidator decision, reinforced): R6
many dynamic output topics (SQL Kafka sinks are single-topic), reset/gap-drop is imperative stateful
retraction-like control flow, and union-never-sum is the opposite of GROUP BY's grain.

## Smoke — `smoke-aggregator.sh` (2026-07-22, replaces the stale `smoke-level-emitter.sh`)

Follows the normalizer smoke rule ([[normalizer-scaffold]]): raw ex8/OKX payloads → `ex8-raw`, whole
chain job1→aggregator (6 jobs, NOT level-emitter), assert the terminal `p1-asks`/`p1-bids`. **The
consolidated contract has NO `pipeline_timings`** (the frozen web shape drops it), so there is no
timing chain to assert — unlike the jobs 1–5 smokes. **Assertions filter by `exchange_id==8`**
because the aggregator's `MapState<exchange_id, book>` unions all exchanges and survives across
cases + across prior runs, so "p1 has exactly N levels" is not assertable; ex8's own levels are.
4 ordered cases on one accumulating book: snapshot ⇒ ex8 appears; update ⇒ ex8's 2nd ask joins;
**GAP ⇒ reset ⇒ ex8 absent from BOTH sides (the milestone's core check)**; snapshot re-sync ⇒ ex8
returns. **Case 3 depends on the `"reset"` enum symbol being registered AND the jobs resubmitted**
([[type-validator]] live-bug note) — otherwise job 2 NPEs and no p1 event arrives. Written +
syntax-checked 2026-07-22; **NOT run live yet.**
