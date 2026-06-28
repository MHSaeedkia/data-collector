# Memory Index

- [Kafka Topic Strategy](project_kafka_topic_strategy.md) — ID-based topics: input `{side}-p{pair_id}-ex{exchange_id}`, output `{side}-p{pair_id}`, key=null; pre-provisioned; migration COMPLETE (warmup+schema+Flink+web)
- [Avro Schema: OrderBookEvent](project_avro_schema.md) — Schema for normalized per-side order book events (`schemas/orderbook_event.avsc`); identity is `pair_id`/`exchange_id`, while `base`/`quote`/`exchange_name` are display-only (no logic)
- [Order Book Aggregation](project_orderbook_aggregation.md) — Generate consolidated order book per pair+side by merging all exchanges; stateful step on top of the transport-level regex merge
- [Order Book Web UI](project_orderbook_web.md) — Standalone Go app (net/http + gorilla/websocket + franz-go + pgx) consuming consolidated `{side}-p{pair_id}` topics, resolving ids→display from postgres, rendering a live order book (`web/`); embedded UI, vendored deps, dockerized as `web` service
- [NiFi over HTTP](project_nifi_http.md) — NiFi runs unsecured HTTP at localhost:8081/nifi; custom `nifi/start-http.sh` entrypoint patches nifi.properties (clears keystore/https) because apache/nifi 2.x image is HTTPS-only
