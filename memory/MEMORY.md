# Memory Index

- [Kafka Topic Strategy](project_kafka_topic_strategy.md) — ID-based topics: input `{side}-p{pair_id}-ex{exchange_id}`, output `{side}-p{pair_id}`, key=null; pre-provisioned; warmup+schema+Flink migrated, web still pending
- [Avro Schema: OrderBookEvent](project_avro_schema.md) — Schema for normalized per-side order book events (`schemas/orderbook_event.avsc`); identity is `pair_id`/`exchange_id`, while `base`/`quote`/`exchange_name` are display-only (no logic)
- [Order Book Aggregation](project_orderbook_aggregation.md) — Generate consolidated order book per pair+side by merging all exchanges; stateful step on top of the transport-level regex merge
- [Order Book Web UI](project_orderbook_web.md) — Standalone Node.js app consuming consolidated topics and rendering a live order book in the browser (`web/`)
