# Todo

## Done
- [x] Kafka topic strategy: `{pair}-{side}-{exchange}` topic, null key, single partition per topic
- [x] Avro schema: `schemas/orderbook_event.avsc` registered in schema registry
- [x] JSON schema: `schemas/orderbook_event.json` registered in schema registry

## Phase 1 (In Progress)
- [ ] Flink: JSON deserializer (plain JSON, no schema registry)
- [ ] Flink: order book consumer using regex topic subscription (`{pair}-{side}-.*`)

## Phase 2 (Deferred)
- [ ] Flink: migrate from JSON to Avro deserializer wired to schema registry
- [x] Topic provisioning: pre-create topics from postgres markets table (`scripts/warmup.sh`)
