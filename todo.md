# Todo

## Done
- [x] Kafka topic strategy: `{pair}_{side}_{exchange}` topic, null key, single partition per topic
- [x] Avro schema: `schemas/orderbook_event.avsc` registered in schema registry

## In Progress
- [ ] Flink: Avro deserializer wired to schema registry
- [ ] Flink: order book consumer using regex topic subscription (`{pair}_{side}_.*`)

## Deferred
- [ ] Topic provisioning: pre-create topics from postgres markets table
