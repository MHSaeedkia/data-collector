---
name: avro-schema-orderbook
description: Avro schema design for normalized order book events (OrderBookEvent, PriceLevelEvent, ConsolidatedOrderBookEvent) in schema registry
metadata:
    type: project
---

## Schema: OrderBookEvent

File: `schemas/orderbook_event.avsc`
Namespace: `io.tibobit.orderbook`
Registered in schema registry; NiFi and Flink both reference it by name.
Example payload: `schemas/orderbook_event_example.json`.

## Field summary (in schema order)

| Field           | Avro type                 | Notes                                                         |
| --------------- | ------------------------- | ------------------------------------------------------------- |
| `exchange_id`   | int (required)            | DB `exchanges.id`, e.g. 1 — the exchange identity            |
| `exchange_name` | `["null","string"]`=null  | **Display only, no logic** — e.g. `bitpin`, `nobitex`        |
| `base`          | `["null","string"]`=null  | **Display only, no logic** — base asset, e.g. `BTC`          |
| `quote`         | `["null","string"]`=null  | **Display only, no logic** — quote asset, e.g. `USDT`        |
| `pair_id`       | int (required)            | DB `markets.id`, e.g. 2 — the pair identity, used in topics  |
| `side`          | enum `asks`\|`bids` (req)  | Mirrors topic prefix; included for self-describing messages   |
| `type`          | enum `snapshot`\|`update` (req) | Event type. Flink honours it (Phase 3, 2026-06-29): `snapshot` replaces the exchange book, `update` mutates it — see [[orderbook-aggregation]] |
| `event_time`    | long timestamp-millis (req) | Exchange-reported UTC timestamp in ms                       |
| `sequence_id`   | long (required)            | Monotonic per `(pair_id, exchange_id)` stream; ordering/dedup token. Added 2026-06-29 for Phase 3 — see [[orderbook-aggregation]] |
| `levels`        | array of PriceLevel (req)  | Price + quantity both as string to preserve decimal precision |

`pair_id` was added 2026-06-28; the pipeline already moved from a single `pair` string to separate `base`/`quote` fields earlier.

Required vs optional (set 2026-06-28; `sequence_id` added 2026-06-29): `exchange_id`, `pair_id`, `side`, `type`, `event_time`, `sequence_id`, `levels` are **required** (non-nullable, no default). The three display-only fields `exchange_name`/`base`/`quote` are **optional** — `["null","string"]` with `default: null` — since no logic depends on them.

`sequence_id` is the per-stream ordering token for snapshot+update support. Flink uses it now only to drop stale/duplicate events (`seq <= lastSeq`); gap handling is deferred. Whether it is contiguous (+1) or merely increasing, and who generates it (exchange vs NiFi), is still TO CONFIRM with the NiFi team — see [[orderbook-aggregation]].

## `base`, `quote`, `exchange_name` are display-only — NO logic depends on them

These three fields are **purely human-readable labels with no behavioural value**. Nothing in the
pipeline routes, keys, joins, filters, or branches on them — identity is always `pair_id` and
`exchange_id`. Treat them as carry-along display strings; **never add logic that reads them**. If a
future need to map id → name arises, look it up from postgres (`markets.base_id`/`quote_id` joined to
`currencies`, and `exchanges`; see [[db-schema]]), don't build logic on the event field.

Consequence in code: the Flink job ignores them — `OrderBookEvent` deserializes only `exchange_id`,
`pair_id`, `side`, `type`, `event_time`, `levels` and is `@JsonIgnoreProperties(ignoreUnknown=true)`.
The wire schema still includes them (unchanged) for consumers that want to display them (NiFi/web).
See [[kafka-topic-strategy]] and [[orderbook-aggregation]].

## NiFi responsibility before publishing

NiFi is handled by a separate team and is not implemented in this repo. Documented here for readers to understand the contract this schema depends on.

- Provide `base` and `quote` separately (e.g. raw `BTCUSDT` → `base=BTC`, `quote=USDT`) plus `pair_id` (DB `markets.id`) and `exchange_id`/`exchange_name`
- Split raw exchange message (which contains both sides) into two separate events
- Route each event to the correct input topic: `{side}-p{pair_id}-ex{exchange_id}` (e.g. `asks-p2-ex1`) — see [[kafka-topic-strategy]]
- Kafka message key is null (one exchange per topic already guarantees ordering)
- Populate `sequence_id` as a monotonically increasing value per `(pair_id, exchange_id)` stream so Flink can drop stale/duplicate events

## Why price/qty are strings

Exchange APIs return price and quantity as strings to avoid floating-point precision loss. Keeping them as strings in Avro preserves this exactly. Flink converts to `BigDecimal` at processing time.

**Why:** Schema registry contract between NiFi and Flink for the order book pipeline.
**How to apply:** Any new exchange integration must produce events conforming to this schema after NiFi normalization.

## Schema: ConsolidatedOrderBookEvent (added 2026-07-11)

File: `schemas/consolidated_order_book_event.avsc`, record name `ConsolidatedOrderBookEvent`,
namespace `io.tibobit.orderbook`. Example payload: `schemas/consolidated_order_book_event_example.json`.
Registered in `scripts/warmup.sh` as subject `consolidated-order-book-event`, same pattern as
`price-level-event`.

This documents the **output** wire shape of `flink/orderbook-consolidator/`'s
`ConsolidatedOrderBook`/`ConsolidatedLevel` model (see
[[orderbook-consolidator-decision]]), published to `{side}-p{pair_id}` and consumed by `web/`.
That output shape is fixed/frozen — do not change it; this schema is a documentation/contract
mirror of it, not a driver of it.

| Field        | Avro type                          | Notes                                              |
| ------------ | ----------------------------------- | --------------------------------------------------- |
| `pair_id`    | int (required)                      | DB `markets.id`                                     |
| `side`       | enum `asks`\|`bids` (required)      | Matches topic suffix                                 |
| `event_time` | long timestamp-millis (required)    | Max `event_time` across contributing exchange books  |
| `levels`     | array of record (required)          | Each: `exchange_id:int`, `price:string`, `quantity:string` — union across exchanges, never summed; equal prices from different exchanges stay as separate adjacent entries |

**Update 2026-07-11 — now TRUE Avro wire encoding, not just documentation.** The caveat that used
to live here (and still applies to `orderbook_event.avsc`/`orderbook-job` above — untouched) is
now **stale for this schema and for `price_level_event.avsc`**: `flink/orderbook-consolidator/`
was refactored to real Confluent Avro binary on both source and sink —
`ConsolidatedOrderBookSerializer` now emits Confluent-wire-format bytes (magic byte + schema id +
Avro payload) via `ConfluentRegistryAvroSerializationSchema`, and `PriceLevelEventDeserializer`
decodes the same way via `ConfluentRegistryAvroDeserializationSchema`. All Jackson/JSON code and
`@JsonProperty`/`@JsonIgnoreProperties` annotations were removed from that module. Full details,
including two **known deploy-blocking downstream risks** (`web/` still expects JSON;
NiFi's actual producer format on the input topics is unverified), are in
[[orderbook-consolidator-decision]] under "Wire format: JSON → true Confluent Avro (2026-07-11)".

[[kafka-topic-strategy]]
