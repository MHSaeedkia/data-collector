---
name: fake-data-generator
description: Go dev tool that publishes synthetic per-exchange order book input events straight to Kafka, bypassing NiFi
metadata:
  type: project
---

## Fake data generator (`fake-data-generator/mian.go`)

Standalone Go tool that streams randomized order book snapshots straight to Kafka for
local dev/testing — it stands in for the NiFi → Kafka producer side, so you can exercise
the Flink job + [[orderbook-web]] without running NiFi.

- Uses `github.com/segmentio/kafka-go` (note: **different** Kafka lib than the web app's
  franz-go). On start it creates every input topic (1 partition, RF 1, ignores
  "already exists"), then loops emitting snapshots until `DURATION_SECONDS` elapses or Ctrl-C.
- Emits to the **ID-based input topics** `{side}-p{pair_id}-ex{exchange_id}` (e.g. `asks-p2-ex1`),
  matching [[kafka-topic-strategy]]. Each event carries the full payload
  (`exchange_id`, `exchange_name`, `base`, `quote`, `pair_id`, `side`, `type`, `event_time`, `levels`)
  per [[avro-schema-orderbook]]; `type` is always `"snapshot"` (it generates fresh full books).
- Pairs/exchanges are **hardcoded** in `var` blocks (`Pair{ID,Base,Quote}`, `Exchange{ID,Name}`),
  NOT loaded from postgres — keep these IDs consistent with the DB if you want web enrichment to
  resolve to real labels. Default seed: pair `{ID:2, BTC/USDT}`, exchanges `{1 nobitex, 2 bitpin}`.
- Filename is literally `mian.go` (typo, kept as-is). Config via env: `KAFKA_BOOTSTRAP`
  (default `localhost:9092`), `DURATION_SECONDS` (default 600).

Aligned to the ID-based scheme + new `pair_id`/`type` fields on 2026-06-28 (previously emitted
old `{pair}-{side}-{exchange}` topics with no `pair_id`/`type`).
