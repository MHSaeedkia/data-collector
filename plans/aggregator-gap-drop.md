# Plan: gap-driven book drop via a new full-book aggregator

## Context

**Problem.** When a delta feed (ex1/ex6/ex8) hits a sequence gap, job 2
([`TypeValidateFunction`](flink/normalizer/job-type-validator/src/main/java/io/tibobit/normalizer/typevalidate/TypeValidateFunction.java#L132-L135))
sets `awaitingSnapshot` and rejects the gap update **only to the dead-letter side output**.
Nothing is emitted onto the main stream, so job 5 → consolidator hear nothing and the
consolidator keeps serving that exchange's **pre-gap, known-diverged** book until the next
snapshot flows through. This regresses an explicit prior decision from the monolithic
`orderbook-job` — *"gap → clear the book, that exchange drops out of the consolidated book,
never serve a drifted book"* (memory/project_orderbook_aggregation.md).

**Root cause discovered during exploration.** The pipeline builds each exchange's full book in
job 5, **shreds it into per-level deltas** in job 6, and the consolidator **reassembles the full
book** in its stage 1 — a build→shred→rebuild round trip. In that per-level delta representation
there is no primitive for "drop this whole exchange," which is why the gap can't be expressed.
Meanwhile the consolidator's **stage 2**
([`CrossExchangeConsolidator`](flink/orderbook-consolidator/src/main/java/io/tibobit/consolidator/operator/CrossExchangeConsolidator.java))
already holds `MapState<exchange_id, full book>` and unions/sorts — exactly the desired
aggregation, only fed the wrong shape.

**Outcome.** A new terminal aggregator consumes job 5's **full per-exchange books directly**
(`OrderBookSnapshot`), unions/sorts them, and emits the existing `p{id}-{side}`
`consolidated-order-book-event` the web already consumes. Job 2 emits a **reset marker** on gap;
job 5 turns it into an **emptied book**; the aggregator drops that exchange from the union
naturally. The consolidator and job 6 (level-emitter) are deprecated. Scope: **all delta feeds**
(the fix lives in the exchange-agnostic gap branch).

Note: job 5 already emits full books over Kafka to job 6 today, so the aggregator reading that
same `orderbook-snapshot-flink` stream adds **no new amplification** — it removes job 6's compute
and its downstream topic.

## Design

```
job2 (type-validate) ──valid──► job3 rebase ► job4 precision ► job5 book-build ──► NEW aggregator ──► p{id}-{side}
        │  gap                   (stateless passthrough of reset)   │ reset⇒empty book    │ empty book ⇒ exchange
        └─ reset marker ─────────────────────────────────────────────┘                     └─ drops out of union
        └─ dead-letter the offending update (audit, unchanged)

DEPRECATED: job6 level-emitter, orderbook-consolidator
```

### A. Job 2 emits a reset marker on gap
`TypeValidateFunction.processElement`, the final `else` (true gap) branch — currently
`awaitingSnapshot.update(true); reject(SEQUENCE_GAP)`. Add: also `out.collect(...)` a synthetic
reset `RawOrderBookEvent` (new `type` sentinel `"reset"`, `sequence_id=null`, `asks=null`,
`bids=null`, same `exchange_id`/`pair_id`, `event_time` from the gap event), with
`pipeline_timings` stamped (`type_validate_in`/`out`) so downstream serializers don't NPE. Emit
**once per gap episode** — the `else` branch is only reached on the not-awaiting→awaiting
transition (subsequent updates return at the `awaitingSnapshot` reject). The offending update is
**still dead-lettered** as today. `"reset"` is a new string value, not an Avro schema change.

### B. Job 5 turns a reset into an emptied book
`BookBuildFunction` (job-book-builder): add a branch for `type == "reset"` → clear both sides'
`MapState` and emit an `OrderBookSnapshot` with empty asks+bids (event_time from the reset).
Jobs 3 (rebase) and 4 (precision) are stateless and already pass null sides through — **verify**
they forward a null-sided reset without dead-lettering (rebaser's `no_rebase_row` needs the
real exchange/market row, which exists) or erroring.

### C. New aggregator module (`flink/normalizer/job-aggregator/`)
Package `io.tibobit.normalizer.aggregate`, main `AggregatorJob`, `env.execute("normalizer-aggregator")`.
- **Source**: `KafkaSource` regex `ex[0-9]+-p[0-9]+-orderbook-snapshot-flink` (job 5's output,
  `OrderBookSnapshot`, subject `order-book-snapshot`), `OffsetsInitializer.latest()`.
- **Split**: flatMap each `OrderBookSnapshot` (both sides) into two per-side `ExchangeBook`s
  (asks, bids), so the ported stage-2 operator is reused near-verbatim.
- **keyBy** `(pair_id, side)` → **port `CrossExchangeConsolidator`** (union never-sum, sort
  asks↑/bids↓ tie-break qty desc). An empty `ExchangeBook` replaces the stored entry and
  contributes nothing → the exchange drops out. Port models `ExchangeBook`, `ConsolidatedLevel`,
  `ConsolidatedOrderBook` and `ConsolidatedOrderBookSerializer` + sink factory from
  `io.tibobit.consolidator`.
- **Sink**: `KafkaSink` dynamic topic `p{id}-{side}`, subject `consolidated-order-book-event`
  (the frozen web contract, unchanged). Reuse `normalizer-common` `AvroSchemaLoader`/serde
  conventions.
- Register the module in `flink/normalizer/pom.xml` (normalizer-parent) and add run/compose wiring
  (`run-job.sh`/`Makefile`/`docker-compose-normalizer.yml`) mirroring jobs 1–6.

### D. Deprecate consolidator + job 6
Mirror the existing "mark orderbook-job as deprecated" pattern (commit 41bdd20): add a DEPRECATED
banner to `flink/orderbook-consolidator/README` and `job-level-emitter`, and stop deploying them
(remove from the active `docker-compose-normalizer.yml` / Makefile refresh flow). **Keep the code**
(do not delete). **Before this:** confirm nothing but the consolidator consumes the frozen
`ex{id}-p{id}-{side}` per-level topics.

## Why Java DataStream, not Flink SQL
Same conclusion as the ratified consolidator decision, reinforced by the new requirements:
- **R6 — many dynamically-named output topics (`p{id}-{side}`).** SQL Kafka sinks write to one
  topic (no per-row routing); Java `KafkaSink` routes per record from one operator. This was the
  original deciding factor and is unchanged.
- **Reset / gap-drop control flow.** Clearing one exchange's contribution on a `type="reset"`
  sentinel is imperative, stateful, retraction-like logic — no clean SQL idiom; trivial in Java.
- **Union-never-sum.** Keeping equal prices from different exchanges as separate adjacent entries
  is the opposite of SQL `GROUP BY`'s grain.
- **Reuse + consistency.** `CrossExchangeConsolidator` (union + asks↑/bids↓ tie-break comparator)
  ports over with its tests; all other jobs are Java DataStream with the operator-harness test
  style. Also keeps the door open for R3 stale-time timers.

## Files (representative)
- Edit: `job-type-validator/.../TypeValidateFunction.java` (+ `RESET` constant), its test.
- Edit: `job-book-builder/.../BookBuildFunction.java` (reset branch), its test.
- Verify/maybe-touch: `job-rebaser`, `job-precision` main functions (reset passthrough).
- New: `flink/normalizer/job-aggregator/**` (job, ported operator/models/serializer, tests).
- Edit: `flink/normalizer/pom.xml`, `docker-compose-normalizer.yml`, Makefile/run scripts,
  `reset.sh` (swap consolidator → aggregator in the stateful-job reset list).
- Deprecate: `flink/orderbook-consolidator/`, `job-level-emitter/` (banners + un-deploy).

## Open items to verify first
1. Nothing besides the consolidator consumes `ex{id}-p{id}-{side}` (believed true; confirm).
2. Jobs 3/4 forward a null-sided `type="reset"` event untouched (no dead-letter, no error).
3. `RawOrderBookEvent.type` has no Avro enum constraint (it's a plain string — expected free).

## Verification
- **Unit**: job 2 — gap emits a reset on the main output **and** dead-letters the update; reset
  emitted once per episode. job 5 — reset ⇒ empty book + state cleared. aggregator — port
  stage-2 tests; add "empty book ⇒ that exchange removed from the union, others intact."
- **Manual-test-data**: extend `10-ex1-sequence-gap` to assert the consolidated `p1-{side}` book
  **drops ex1's levels** at the gap and **restores** them after the REST resync; update `reset.sh`
  to cancel/resubmit the new aggregator instead of the consolidator.
- **Live smoke**: run the whole chain (`docker-compose-normalizer.yml`), produce scenario 10,
  confirm in kafka-ui / web UI that ex1 vanishes from the consolidated book on the gap and returns
  on resync. (All ex1 scenarios remain "not yet run live" per memory — first live run is the real
  check.)
- **Memory/todo**: update `project_orderbook_consolidator_decision.md` (deprecated + why),
  `project_type_validator.md` (reset emission), `project_book_builder.md` (reset branch), add a new
  `project_aggregator.md`, `MEMORY.md` index, and `todo.md`.
