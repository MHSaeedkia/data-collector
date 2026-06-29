# Memory Index

- [DB Schema](project_db_schema.md) — postgres `markets` db: normalized `currencies` lookup + `markets.base_id`/`quote_id` FKs (was base/quote VARCHAR); 3 exchanges (wallex added), 81 markets, all exchange_markets seeded `unsubscribe`
- [Kafka Topic Strategy](project_kafka_topic_strategy.md) — ID-based topics: input `{side}-p{pair_id}-ex{exchange_id}`, output `{side}-p{pair_id}`, key=null; pre-provisioned; migration COMPLETE (warmup+schema+Flink+web)
- [Avro Schema: OrderBookEvent](project_avro_schema.md) — Schema for normalized per-side order book events (`schemas/orderbook_event.avsc`); identity is `pair_id`/`exchange_id`, while `base`/`quote`/`exchange_name` are display-only (no logic)
- [Order Book Aggregation](project_orderbook_aggregation.md) — Generate consolidated order book per pair+side by merging all exchanges; stateful step on top of the transport-level regex merge
- [Order Book Web UI](project_orderbook_web.md) — Standalone Go app (net/http + gorilla/websocket + franz-go + pgx) consuming consolidated `{side}-p{pair_id}` topics, resolving ids→display from postgres, rendering a live order book (`web/`); embedded UI, vendored deps, dockerized as `web` service
- [Fake Data Generator](project_fake_data_generator.md) — Go dev tool (segmentio/kafka-go) that publishes synthetic per-exchange input events to `{side}-p{pair_id}-ex{exchange_id}`, standing in for NiFi; hardcoded pairs/exchanges
- [NiFi over HTTPS](project_nifi_https.md) — NiFi runs HTTPS single-user on 8443 (stock apache/nifi 2.x, no custom entrypoint); raw-IP access fails with `400 Invalid SNI` → fix via `NIFI_WEB_PROXY_HOST`
