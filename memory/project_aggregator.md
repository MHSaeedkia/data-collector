---
name: aggregator
description: job-aggregator module (raw pipeline terminal job 6): consumes job 5 full books, unions across exchanges, emits the frozen p{id}-{side} web event
metadata:
    type: project
---

# Terminal job — cross-exchange aggregator (`flink/normalizer/job-aggregator/`)

Package `io.tibobit.normalizer.aggregate` (flat, [[normalizer-scaffold]] conventions). Consumes job
5's full per-exchange books `ex[0-9]+-p[0-9]+-orderbook-snapshot-flink` (`OrderBookSnapshot`, subject
`order-book-snapshot`), unions them across exchanges per `(pair_id, side)`, and emits the frozen
`p{id}-{side}` web event — subject **`aggregated-order-book-event`** (wire shape is the frozen web
contract, see [[avro-schema-orderbook]]). 12 tests green (9 stage operator + 3 splitter). Code+tests
DONE, deployed via Makefile `refresh-normalizer`; **not yet run live.**

## Why this exists (the gap-drop root-cause fix)

The aggregator feeds on job 5's **full books directly**, so job 2's reset ⇒ job 5 empty book ⇒
**empty `ExchangeBook` replaces the stored entry and contributes nothing ⇒ that exchange drops out
of the union** ([[type-validator]] reset marker, [[book-builder]] reset branch). Consuming full
books (rather than per-level deltas) is what makes "drop this whole exchange on a gap" expressible —
a delta representation has no primitive for it. That is the whole point.

## Shape

- `SnapshotSplitter` (flatMap): one `OrderBookSnapshot` → two per-side `ExchangeBook`s (asks, bids),
  each level stamped with the snapshot's `exchange_id`. Job 5 always emits both sides (possibly
  empty); a null side is defensively treated as empty. This split lets one keyed operator serve both
  sides.
- `CrossExchangeAggregator` (keyed `(pair_id, side)`): `MapState<exchange_id, ExchangeBook>`,
  replace-per-exchange, **union never-sum** (equal prices from different exchanges stay separate
  adjacent entries — the opposite of SQL GROUP BY, [[orderbook-aggregation]]), sort asks↑/bids↓
  tie-break qty desc as BigDecimal ([[bigdecimal-rules]]), output `event_time` = max across
  contributing books. Sort direction chosen per-record from `side` (one operator serves both).
- Sink: inline `KafkaSink`, per-record topic `p{pair_id}-{side}`, `AggregatedOrderBookSerializer`
  (uses common `AvroSchemaLoader` + subject `aggregated-order-book-event`, the FROZEN web wire shape —
  do NOT alter). Models `ExchangeBook`/`AggregatedLevel`/`AggregatedOrderBook` are aggregator-local
  (not common; the web consumes the Avro form, not the POJO).

## Deploy

The aggregator writes the frozen `p{id}-{side}` topics the web consumes, so **only one producer may
write them at a time**. Makefile `refresh-normalizer`/`run-normalizer-jobs` submit `job-aggregator`
downstream-first; `manual-test-data/reset.sh` recycles `normalizer-aggregator` (stateful, keyed per
pair+side). `run-job.sh` is module-driven, so `./run-job.sh job-aggregator` runs it standalone for
isolated smoke tests. The FROZEN web-output family `p{id}-{side}` is created by `scripts/warmup.sh`.

**Why Java DataStream not SQL:** R6 many dynamic output topics (SQL Kafka sinks are single-topic),
reset/gap-drop is imperative stateful retraction-like control flow, and union-never-sum is the
opposite of GROUP BY's grain.

## Smoke — `smoke-aggregator.sh` (2026-07-22)

Follows the normalizer smoke rule ([[normalizer-scaffold]]): raw ex8/OKX payloads → `ex8-raw`, whole
chain job1→aggregator, assert the terminal `p1-asks`/`p1-bids`. **The aggregated contract has NO
`pipeline_timings`** (the frozen web shape drops it), so there is no timing chain to assert — unlike
the jobs 1–5 smokes. **Assertions filter by `exchange_id==8`** because the aggregator's
`MapState<exchange_id, book>` unions all exchanges and survives across cases + across prior runs, so
"p1 has exactly N levels" is not assertable; ex8's own levels are. 4 ordered cases on one
accumulating book: snapshot ⇒ ex8 appears; update ⇒ ex8's 2nd ask joins; **GAP ⇒ reset ⇒ ex8 absent
from BOTH sides (the milestone's core check)**; snapshot re-sync ⇒ ex8 returns. **Case 3 depends on
the `"reset"` enum symbol being registered AND the jobs resubmitted** ([[type-validator]] live-bug
note) — otherwise job 2 NPEs and no p1 event arrives. Written + syntax-checked 2026-07-22; **NOT run
live yet.**
