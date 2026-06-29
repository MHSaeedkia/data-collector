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
- [ ] Add `sequenceId` field to `OrderBookEvent` model
- [ ] Change `OrderBookMerger` state from `MapState<Integer, OrderBookEvent>` to a maintained
      per-exchange book: `{ Map<price,qty> levels, long eventTime, long lastSeq }`
- [ ] `processElement`: branch on `type` (replace vs mutate); drop stale/duplicate `seq <= lastSeq`
- [ ] Rebuild-union-sort-emit stays unchanged

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

### DEFERRED — sequence-id gap handling (revisit later)
Question: when the merger sees a gap (`seq > lastSeq + 1`) for an exchange, what should it do?
- CURRENT ASSUMPTION: no messages are dropped — we have every seq — so no gap handling yet
  (only stale/duplicate drop via `seq <= lastSeq`).
- Options to decide later:
  - (A) Drop that exchange's book + wait for next snapshot to resync (safe/standard; needs
        contiguous +1 sequence ids).
  - (B) Apply anyway / tolerate (simpler; book can silently drift if a message was dropped).
  - (C) Seq is only monotonic, not contiguous → can only detect stale/dups, no gap detection.
- TO CONFIRM with the NiFi/exchange team: is `sequence_id` contiguous (+1 per message) or
  merely increasing? And who generates it (exchange vs NiFi)?
