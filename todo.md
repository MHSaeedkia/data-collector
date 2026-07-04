# Todo

## Done
- [x] Kafka topic strategy: `{pair}-{side}-{exchange}` topic, null key, single partition per topic
- [x] Avro schema: `schemas/orderbook_event.avsc` registered in schema registry
- [x] JSON schema: `schemas/orderbook_event.json` registered in schema registry
- [x] Topic provisioning: pre-create topics from postgres markets table (`scripts/warmup.sh`)

## Phase 1 — Flink JSON pipeline (source: `flink/orderbook-job/`)

### Project setup
- [x] Create Maven project under `flink/orderbook-job/` with `pom.xml`
- [x] Add dependencies: `flink-streaming-java`, `flink-connector-kafka`, `jackson-databind`

### Data model
- [x] `PriceLevel` POJO — `price: String`, `quantity: String`
- [x] `OrderBookEvent` POJO — `exchange`, `pair`, `side`, `event_time`, `levels`

### Deserialization
- [x] `OrderBookEventDeserializer` — implements `DeserializationSchema<OrderBookEvent>` using Jackson

### Kafka sources
- [x] Per-pair asks source — one `KafkaSource` per pair with pattern `{pair}-asks-.*` (e.g. `BTC-USDT-asks-.*`)
- [x] Per-pair bids source — one `KafkaSource` per pair with pattern `{pair}-bids-.*` (e.g. `BTC-USDT-bids-.*`)
- [x] Pairs list read from postgres at job startup to build sources dynamically

### Job
- [x] `OrderBookJob` main class — for each pair, creates one asks stream and one bids stream
- [x] Kafka config — bootstrap servers, consumer group

### Order book aggregation (per pair + side)
Goal: for each `{pair}-{side}` stream, merge all exchanges into one price-sorted stream
WITHOUT summing quantities — each level keeps its own exchange; equal-price levels from
different exchanges are separate, adjacent entries. Output: `BTC-USDT-asks`, `BTC-USDT-bids`, …
Note: the Kafka source already merges exchanges at transport level (regex
`{pair}-{side}-.*`); this section is the stateful step that *generates* the book.
- [x] Output model: `ConsolidatedOrderBook { pair, side, levels, eventTime }` with level
      `{ exchange, price, quantity }` (exchange lives per level)
- [x] `KeyedProcessFunction` keyed by `pair`; `MapState<exchange, OrderBookEvent>`
      holds the latest snapshot per exchange (event carries levels + event_time)
- [x] On each event: replace that exchange's snapshot, rebuild the union, sort by price
      (`BigDecimal`), emit consolidated book
- [x] Sort by side: asks ascending, bids descending; tie-break equal price by larger quantity first (`BigDecimal`)
- [x] Wire operator into `OrderBookJob.addStream` between source and sink
- [x] `eventTime` on output = max of contributing exchange snapshots
- [x] Publish merged book to its own Kafka topic `{pair}-{side}` (e.g. `BTC-USDT-asks`) via
      `KafkaSink` + JSON serializer; keep `print()` to stdout as well

### Build & deploy
- [ ] Build fat JAR (`mvn package`)
- [ ] Submit job to Flink cluster via REST API or `flink run`

### Live order book web UI (`web/`)
- [x] Node.js (Express + kafkajs + ws) standalone app; consumes consolidated `{pair}-{side}` topics
- [x] WebSocket push to browser; latest book per topic kept in memory
- [x] Single-page UI: pair dropdown (multiple pairs), live asks/bids book with spread

## Phase 2 — Avro + Schema Registry (deferred)
- [ ] Migrate `OrderBookEventDeserializer` from JSON to Avro + Confluent Schema Registry

## Phase 3 — Snapshot + Update event support (in progress)

Goal: the merger must honour the `type` field instead of treating every event as a full
snapshot. Per-exchange state changes from "last event" to a *maintained* book that we mutate.

### Decided semantics (guaranteed by NiFi)
- `snapshot` → replace that exchange's book wholesale (current behaviour).
- `update`   → apply on top of the existing book, per level:
  - `quantity > 0`  → insert-or-overwrite that price level
  - `quantity == 0` → delete that price level (compare as `BigDecimal`, not string `"0"`)

### Tasks
- [x] Add `sequence_id` (long, required) to `schemas/orderbook_event.avsc` + example JSON (+ NiFi must populate it) — done 2026-06-29
- [x] Add `sequenceId` field to `OrderBookEvent` model — done 2026-06-29
- [x] Change `OrderBookMerger` state from `MapState<Integer, OrderBookEvent>` to a maintained
      per-exchange book: `{ Map<price,qty> levels, long eventTime, long lastSeq }` (new `ExchangeBook` POJO) — done 2026-06-29
- [x] `processElement`: branch on `type` (replace vs mutate); drop stale/duplicate `seq <= lastSeq` — done 2026-06-29
- [x] Rebuild-union-sort-emit stays unchanged (now iterates the maintained map) — done 2026-06-29
- [x] Add `sequence_jump` (long, required) to `schemas/orderbook_event.avsc` + example JSON (NiFi populates it) — done 2026-06-29
- [x] Add `sequenceJump` field to `OrderBookEvent` model — done 2026-06-29
- [x] Generator (`fake-data-generator/mian.go`): send only snapshots with `sequence_jump = 0` — done 2026-06-29
- [x] Gap-detection logic in `OrderBookMerger` using `sequence_jump` — done 2026-06-29
      (snapshot = unconditional baseline; update in-order iff `seq == lastSeq + sequence_jump`;
       stale/dup `seq <= lastSeq` drop; gap `seq > lastSeq + sequence_jump` → clear book +
       `ExchangeBook.awaitingSnapshot` flag, ignore updates until next snapshot resyncs)

### DEFERRED — cold start (revisit later)
Problem: no Flink checkpointing/state backend is configured (docker-compose only sets
`jobmanager.rpc.address`), so the per-exchange book is in-memory and lost on every restart.
With deltas, a restart leaves the book empty until the next snapshot — and if upstream only
snapshots occasionally, that can be a long time.
- CURRENT ASSUMPTION (to unblock Phase 3): the first event per exchange from Kafka is a
  `snapshot`, so a base book always exists before any `update`. No gating implemented yet.
- Options to fix later:
  - (A) Flink checkpoints + savepoints with a durable state backend (volume/S3) — idiomatic,
        restores keyed state automatically, no merger code. RECOMMENDED.
  - (B) Log-compact the output topic `{side}-p{pair_id}` (key = `{side}-p{pair_id}`) and reseed
        on startup — also gives downstream consumers (web UI) instant current-book on connect.
  - (C) Both: (A) for Flink state recovery, (B) for consumer bootstrap.
- Also defer the "ignore updates until first snapshot per exchange" gate (needed for a truly
  empty start / brand-new deploy with no checkpoint).

### DONE — sequence-id gap handling (2026-06-29)
Increments are NOT +1 — exchanges can jump (~300) — so each event carries `sequence_jump`, the
expected delta from the previous message for that exchange. `OrderBookMerger.processElement`:
- snapshot → accepted unconditionally (no seq check; IS the baseline), replaces book, stores
  `lastSeq`, clears `awaitingSnapshot`. Also the only way an exchange's book is first created,
  so a leading `update` is ignored (cold-start "first event is a snapshot" now enforced).
- update → `book == null` or `awaitingSnapshot` → ignore; `seq <= lastSeq` → stale/dup drop;
  `seq > lastSeq + sequence_jump` → GAP: clear book + set `awaitingSnapshot`, wait for next
  snapshot (chosen action: drop-book-and-resync, per user — safe, never serve a drifted book);
  else `seq == lastSeq + sequence_jump` → in order, apply deltas.
- New state `ExchangeBook.awaitingSnapshot` gates updates after a gap.
NOTE: still no protection for restart cold-start (no Flink checkpointing — see section above).

## Phase 4 — Unit testing (TDD, source: `flink/orderbook-job/src/test/`)

Goal: spec-based coverage of the merger — assert the documented contract, deviations = bugs.
See `memory/project_tdd_workflow.md` for the approach and stack.

- [x] Test infra in `pom.xml`: JUnit 5 + AssertJ + `flink-test-utils` (`KeyedOneInputStreamOperatorTestHarness`) + JaCoCo + surefire — done 2026-06-30
- [x] `OrderBookMergerTest` — 17 tests, **100% line + 100% branch** coverage of `OrderBookMerger`
      (snapshot replace / null levels; update upsert/delete/no-delta/stale/gap; strict-seq;
      asks↑/bids↓ sort; tie-break qty desc; equal-price-not-summed; BigDecimal scale collapse;
      event_time = max; pair/side passthrough) — done 2026-06-30
- [x] BUG FOUND & FIXED via test (red→green): updates were applied for the whole band
      `lastSeq < seq <= lastSeq + jump`; the contract is **strict** `seq == lastSeq + jump`.
      Now any non-exact `seq > lastSeq` discards the book + awaits snapshot — done 2026-06-30
- Verified: Flink harness runs on the local JDK (Maven JDK 25); no JDK-21 toolchain needed.
- [x] `OrderBookEventDeserializerTest` — 8 tests, **100% line + 100% branch**: snake_case
      field mapping, type-enum bind, nested levels, ignore-unknown, reject unknown type,
      absent-levels stays null, never-end-of-stream, produced type, lazy-mapper reuse — done 2026-06-30
- [x] `ConsolidatedOrderBookSerializerTest` — 4 tests, **100% branch** (all reachable lines):
      snake_case output keys, level order, empty book, lazy-mapper reuse. The `catch
      (JsonProcessingException)` block is unreachable defensive code (a valid
      `ConsolidatedOrderBook` never fails Jackson) — intentionally not tested — done 2026-06-30
- [x] `OrderBookEventTypeTest` — 3 tests, **100%**: wire token, case-insensitive `fromValue`,
      reject-unknown — done 2026-06-30
- [x] `PairsLoaderTest` — 2 tests: `Pair` record `p{id}` rendering + id accessor (the format
      contract behind topic/operator names). `load()` itself is integration — done 2026-06-30
- Total: **34 unit tests green** across merger + deserializer + serializer + enum + Pair record.

### Needs integration tests (external system — no meaningful unit seam)
These have no pure-logic seam to assert: they wire/drive an external system, so a unit test
would only assert "the builder returned non-null", which is worthless. They need a real broker /
DB / cluster (Testcontainers Kafka + Postgres, or an embedded equivalent):
- [ ] `PairsLoader.load()` — opens a JDBC connection and runs the `DISTINCT` subscribe query
      against the `markets`/`exchange_markets` schema. Needs a Postgres with that schema seeded.
- [ ] `OrderBookSourceFactory.create()` — the real behaviour is the regex topic subscription
      (`{side}-p{pair_id}-ex.*`) folding every exchange topic into one stream, and `latest`
      offsets. Only observable against a live Kafka broker (the builder exposes no pattern getter).
- [ ] `OrderBookSinkFactory.create()` — value-only (key=null) publish to `{side}-p{pair_id}`.
      Only observable by consuming what it writes to a live broker.
- [ ] `OrderBookJob.main()` — end-to-end topology: Postgres (pairs) + Kafka (in/out) + a Flink
      MiniCluster. Highest-value integration test; covers the wiring nothing above exercises.

## Phase 5 — Order Book Consolidator (greenfield `flink/orderbook-consolidator/`)

New Java Flink DataStream job, built **beside** `flink/orderbook-job/` (that one is left as-is).
See `memory/project_orderbook_consolidator_decision.md` for the requirements and why Java over
Flink SQL. Scope of THIS phase: **R1, R2, R4, R5, R6**. **R3 (per-(exchange,pair) `stale_time`
expiry / TTL) is POSTPONED** per the decision doc — its deferred work is captured in the
"Postponed — R3" block at the end of this phase. The consolidator is NOT a copy of the old merger
— the input model is different (one flat price level per message, no
`type`/`sequence_id`/`sequence_jump`). Output shape (`{side}-p{pair_id}`) is unchanged, so `web/`
stays untouched. Precision rules: `BigDecimal` from the wire string everywhere — see
`memory/project_bigdecimal_rules.md`.

### Step 0 — Locked design decisions (2026-07-04)
- [x] **Keying granularity.** The R1 level-state operator is keyed by
      **`(pair_id, exchange_id, side)`** (one keyed instance per exchange's side of a pair), holding
      `MapState<price, StoredLevel>`. HARD RULE — never key the level state by `(pair, side)` alone.
      Because a keyed operator sees only its own key's state, the cross-exchange union (R4) is a
      **second operator re-keyed by `(pair_id, side)`** that merges the per-exchange books.
      (Decision-doc R1 + R4 mechanisms updated to match — 2026-07-04.)
- [x] **"Retraction" = re-emit the full snapshot.** Output is a full `ConsolidatedOrderBook` per
      topic (not a Flink retract stream), so R2 removal = rebuild + emit the book minus the dropped
      level. Downstream contract stays "latest full book per topic".
- [x] **Input message = a single price level.** Lean input schema (`exchange_id`, `pair_id`,
      `side`, `event_time`, `price`, `quantity`). No `type`/`levels[]`/`sequence_*`. Display-only
      `exchange_name`/`base`/`quote` are NOT on the wire (omitted, per doc) — resolved downstream
      from ids if needed. Schema built in Step 0.5.
- [x] **No Postgres dependency.** One topic-pattern source over every `{side}-p{pair}-ex*` topic +
      dynamic sink routing (R6) → no `PairsLoader`/startup DB read.

### Step 0.5 — Schemas (`schemas/`)
Flat single-level shape confirmed by the wire example in
`memory/project_orderbook_consolidator_decision.md`. Mirror `orderbook_event.avsc` conventions.
- [x] `schemas/price_level_event.avsc` — Avro (Confluent Schema Registry, production transport):
      `exchange_id:int`, `pair_id:int`, `side` enum(asks,bids), `event_time` long timestamp-millis,
      `price:string`, `quantity:string`. Display-only fields omitted. Dropped
      `type`/`sequence_id`/`sequence_jump`/`levels[]`; `price`/`quantity` scalar — done 2026-07-04
- [x] `schemas/price_level_event_example.json` — matching example (the doc's BTC/USDT wire sample) — done 2026-07-04
- [x] Register with the schema registry (extended `scripts/warmup.sh` — subject `price-level-event`) — done 2026-07-04

### Step 1 — Project setup
- [ ] Create Maven module `flink/orderbook-consolidator/pom.xml` (copy old pom as template;
      `artifactId` = `orderbook-consolidator`, base package `io.tibobit.consolidator`, shade
      `mainClass` = `OrderBookConsolidatorJob`)
- [ ] Wire into the Flink build/deploy path (`flink/Dockerfile`, `flink/run-job.sh`,
      `docker-compose.yml`) so both jobs can be built/submitted

### Step 2 — Data model
- [ ] `PriceLevelEvent` — single-level input POJO (Jackson snake_case, ignore-unknown)
- [ ] `StoredLevel` — stage-1 keyed-state value: `quantity`, `eventTime`
- [ ] `ExchangeBook` — stage-1 → stage-2 record: `pair_id`, `exchange_id`, `side`, `levels[]`,
      `eventTime` (one exchange's maintained book, emitted after each upsert/remove)
- [ ] Reuse output shape: copy `ConsolidatedLevel` + `ConsolidatedOrderBook` into the new package
      (fixed wire shape the web UI depends on — do NOT change it)

### Step 3 — Deserialization
- [ ] `PriceLevelEventDeserializer` (`DeserializationSchema<PriceLevelEvent>`, Jackson)

### Step 4 — Sources
- [ ] Main price-level `KafkaSource` — one topic-pattern subscription over all
      `{side}-p{pair_id}-ex{exchange_id}` topics, `latest` offsets (live book, no replay)

### Step 5 — Core operators (two stages)
Stage 1 is keyed `(pair_id, exchange_id, side)` so it sees one exchange only; stage 2 re-keys by
`(pair_id, side)` to union across exchanges (R4). Both are `KeyedProcessFunction`s.
- [ ] **Stage 1 — per-exchange book** (keyed `(pair_id, exchange_id, side)`,
      `MapState<price, StoredLevel>`): `processElement` — R1 upsert-latest-by-`event_time` per
      `price`; R2 `quantity == 0` → remove; emit that exchange's maintained book (`ExchangeBook`,
      `event_time` = max of its levels) on each change.
- [ ] **Stage 2 — cross-exchange consolidation** (keyed `(pair_id, side)`,
      `MapState<exchange_id, levels>`): on each `ExchangeBook`, replace that exchange's entry; R4
      rebuild union across exchanges (never sum equal prices); R5 sort (asks↑/bids↓, tie-break qty
      desc, `BigDecimal`); `event_time` on output = max across exchanges; emit `ConsolidatedOrderBook`.

### Step 6 — Sink (R6, dynamic topic routing)
- [ ] Single `KafkaSink` whose `KafkaRecordSerializationSchema` selects topic `{side}-p{pair_id}`
      per record (no per-pair sink templating); reuse `ConsolidatedOrderBookSerializer`

### Step 7 — Job wiring
- [ ] `OrderBookConsolidatorJob.main` — price-level source → `keyBy((pair_id, exchange_id, side))`
      → stage-1 operator → `keyBy((pair_id, side))` → stage-2 operator → sink; Kafka config
      (bootstrap, group id, topic names) via env with docker-compose defaults

### Step 8 — Build & deploy
- [ ] Build fat JAR (`mvn package`)
- [ ] Submit to the Flink cluster; verify a live consolidated book on `{side}-p{pair_id}` in `web/`

### Step 9 — Tests (TDD — see `memory/project_tdd_workflow.md`)
- [ ] Test infra in pom: JUnit 5 + AssertJ + `flink-test-utils` + JaCoCo (as in old module),
      `KeyedOneInputStreamOperatorTestHarness`
- [ ] Stage-1 test — R1 upsert-latest (older `event_time` ignored); R2 qty=0 remove; per-exchange
      `event_time` = max (keyed `(pair, exchange, side)`)
- [ ] Stage-2 test — R4 union not-summed; R5 sort + tie-break + `BigDecimal` scale collapse;
      `event_time` = max across exchanges; dynamic topic per pair+side (keyed `(pair, side)`)
- [ ] `PriceLevelEventDeserializerTest`

### Infra / dev support (as needed)
- [ ] Extend `fake-data-generator/` to emit flat single-level events

### Postponed — R3 (per-(exchange,pair) `stale_time` / TTL expiry)
Deferred to a later phase per `memory/project_orderbook_consolidator_decision.md`. Captured so it
isn't lost. When R3 is picked back up it pulls in:
- Step 0 decisions: **per-level timer strategy** — store each level's `ttlDeadline` in
  `StoredLevel`, register a processing-time timer at `event_time + ttl_ms` (re-armed on every
  upsert); `onTimer(ts)` scans the pair+side's levels and evicts every `deadline <= ts`, then
  re-emits; decide cancel/re-register vs. let the scan absorb stale timers. **TTL config stream** —
  topic name + shape (`exchange_id`, `pair_id`, `ttl_ms`), compacted, consumed from EARLIEST so
  broadcast state bootstraps at startup; cold-start behaviour when a level arrives before its
  `(exchange_id,pair_id)` TTL is known (default TTL vs skip expiry until config seen).
- Schemas: `schemas/ttl_config.avsc` (+ example JSON) — `exchange_id:int`, `pair_id:int`,
  `ttl_ms:long`; register with the schema registry.
- Data model: `TtlConfig` broadcast POJO; add `ttlDeadline` to `StoredLevel`.
- Deserialization: `TtlConfigDeserializer` (`DeserializationSchema<TtlConfig>`).
- Sources: TTL config `KafkaSource` — compacted topic, EARLIEST offsets, `.broadcast(descriptor)`.
- Operator: switch `KeyedProcessFunction` → `KeyedBroadcastProcessFunction`; add
  `processBroadcastElement` (upsert `ttl_ms` into broadcast state), `onTimer` (evict
  `ttlDeadline <= now`, re-emit), and timer (re-)arm + cancel-on-remove in `processElement`.
- Job wiring: build both sources, `broadcast` the TTL stream, `connect` it to the keyed main stream.
- Tests: R3 TTL expiry via `setProcessingTime`, TTL re-arm on refresh, per-exchange TTL from
  broadcast, idle-pair still evicts (proves processing-time not event-time); broadcast/timer-capable
  harness (`KeyedBroadcastProcessFunction` + `setProcessingTime`); `TtlConfigDeserializerTest`.
- Infra: provision the compacted TTL config topic (extend `scripts/warmup.sh`); extend
  `fake-data-generator/` to emit a few TTL config records.
