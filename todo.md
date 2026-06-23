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

## Phase 2 — Avro + Schema Registry (deferred)
- [ ] Migrate `OrderBookEventDeserializer` from JSON to Avro + Confluent Schema Registry
