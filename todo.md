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

### Build & deploy
- [ ] Build fat JAR (`mvn package`)
- [ ] Submit job to Flink cluster via REST API or `flink run`

## Phase 2 — Avro + Schema Registry (deferred)
- [ ] Migrate `OrderBookEventDeserializer` from JSON to Avro + Confluent Schema Registry
