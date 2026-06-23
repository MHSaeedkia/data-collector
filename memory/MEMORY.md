# Memory Index

- [Kafka Topic Strategy](project_kafka_topic_strategy.md) — Topic=`{pair}_{side}`, Key=`{exchange}`; pre-provisioned; NiFi→Kafka→Flink order book pipeline
- [Avro Schema: OrderBookEvent](project_avro_schema.md) — Schema for normalized per-side order book events; file at `schemas/orderbook_event.avsc`
- [Order Book Aggregation](project_orderbook_aggregation.md) — Generate consolidated order book per pair+side by merging all exchanges; stateful step on top of the transport-level regex merge
