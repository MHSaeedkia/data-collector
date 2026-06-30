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
