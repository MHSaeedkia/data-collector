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
| `sequence_id`   | long (required)            | Monotonic per `(pair_id, exchange_id)` stream; ordering/dedup token — see [[orderbook-aggregation]] |
| `sequence_jump` | long (required)            | Expected delta from the previous message's `sequence_id` (increments are NOT +1 — some exchanges jump ~300). In-order iff `seq == lastSeq + sequence_jump`; anything else is a gap — see [[orderbook-aggregation]] |
| `levels`        | array of PriceLevel (req)  | Price + quantity both as string to preserve decimal precision |

Required vs optional: `exchange_id`, `pair_id`, `side`, `type`, `event_time`, `sequence_id`, `sequence_jump`, `levels` are **required** (non-nullable, no default). The three display-only fields `exchange_name`/`base`/`quote` are **optional** — `["null","string"]` with `default: null` — since no logic depends on them.

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
- Route each event to the correct input topic: `ex{exchange_id}-p{pair_id}-{side}` (e.g. `ex1-p2-asks`) — see [[kafka-topic-strategy]]
- Kafka message key is null (one exchange per topic already guarantees ordering)
- Populate `sequence_id`/`sequence_jump` per `(pair_id, exchange_id)` stream so Flink can drop stale/duplicate events and detect gaps

**NOTE:** this whole NiFi-normalizes contract is being replaced by [[raw-pipeline-decision]]
(NiFi will publish verbatim raw payloads to `ex{id}-raw`; a new Flink pipeline reproduces the
normalization). Kept until cutover.

## Why price/qty are strings

Exchange APIs return price and quantity as strings to avoid floating-point precision loss. Keeping them as strings in Avro preserves this exactly. Flink converts to `BigDecimal` at processing time.

**Why:** Schema registry contract between NiFi and Flink for the order book pipeline.
**How to apply:** Any new exchange integration must produce events conforming to this schema after NiFi normalization.

## Schema: PriceLevelEvent

File: `schemas/price_level_event.avsc` (+ `_example.json`), record `PriceLevelEvent`, namespace
`io.tibobit.orderbook`, registry subject `price-level-event`. The consolidator's **input** wire
shape on `ex{exchange_id}-p{pair_id}-{side}` topics — one flat price level per message:
`exchange_id:int`, `pair_id:int`, `side:enum(asks|bids)`, `event_time:timestamp-millis`,
`price:string`, `quantity:string`. No `type`/`sequence_*`/`levels[]`/display fields — see
[[orderbook-consolidator-decision]] for the semantics.

## Schema: ConsolidatedOrderBookEvent

File: `schemas/consolidated_order_book_event.avsc`, record name `ConsolidatedOrderBookEvent`,
namespace `io.tibobit.orderbook`. Example payload: `schemas/consolidated_order_book_event_example.json`.
Registered in `scripts/warmup.sh` as subject `consolidated-order-book-event`, same pattern as
`price-level-event`.

This documents the **output** wire shape of `flink/orderbook-consolidator/`'s
`ConsolidatedOrderBook`/`ConsolidatedLevel` model (see
[[orderbook-consolidator-decision]]), published to `p{pair_id}-{side}` and consumed by `web/`.
That output shape is fixed/frozen — do not change it; this schema is a documentation/contract
mirror of it, not a driver of it.

| Field        | Avro type                          | Notes                                              |
| ------------ | ----------------------------------- | --------------------------------------------------- |
| `pair_id`    | int (required)                      | DB `markets.id`                                     |
| `side`       | enum `asks`\|`bids` (required)      | Matches topic suffix                                 |
| `event_time` | long timestamp-millis (required)    | Max `event_time` across contributing exchange books  |
| `levels`     | array of record (required)          | Each: `exchange_id:int`, `price:string`, `quantity:string` — union across exchanges, never summed; equal prices from different exchanges stay as separate adjacent entries |

## Which schemas are real wire encoding vs documentation-only

`price_level_event.avsc` and `consolidated_order_book_event.avsc` are **TRUE Confluent Avro wire
encoding** (magic byte + schema id + Avro payload): the consolidator reads/writes them via the
Confluent registry classes, and `web/` decodes the output via `hamba/avro/v2` + the registry (see
[[orderbook-consolidator-decision]] and [[orderbook-web]]). `orderbook_event.avsc` is still
**documentation-only** — the old `flink/orderbook-job` speaks plain JSON matching it. The one
open risk: NiFi's actual producer format on the consolidator's input topics is unverified
(moot after [[raw-pipeline-decision]] cutover).

[[kafka-topic-strategy]]
