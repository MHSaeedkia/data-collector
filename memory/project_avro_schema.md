---
name: avro-schema-orderbook
description: Avro schema design for normalized order book events in schema registry, used by NiFi (producer) and Flink (consumer)
metadata:
    type: project
---

## Schema: OrderBookEvent

File: `schemas/orderbook_event.avsc`  
Namespace: `io.tibobit.orderbook`  
Registered in schema registry; NiFi and Flink both reference it by name.

## Field summary

| Field       | Avro type               | Notes                                                       |
| ----------- | ----------------------- | ----------------------------------------------------------- |
| `exchange`  | string                  | Normalized, e.g. `okex`, `nobitex`                          |
| `pair`      | string                  | Normalized with `_` separator, e.g. `BTC_USDT`              |
| `side`      | enum `asks\|bids`       | Mirrors topic suffix; included for self-describing messages |
| `eventTime` | long (timestamp-millis) | Exchange-reported UTC timestamp in ms                       |
| `levels`    | array of PriceLevel     | Price + qty both as string to preserve decimal precision    |

## NiFi responsibility before publishing

NiFi is handled by a separate team and is not implemented in this repo. Documented here for readers to understand the contract this schema depends on.

- Normalize pair name to `{BASE}_{QUOTE}` format (e.g. `BTCUSDT` → `BTC_USDT`)
- Split raw exchange message (which contains both sides) into two separate events
- Route each event to the correct topic: `{market}_{side}` (e.g. `BTC_USDT_asks`)
- Set Kafka message key to `{exchange}`

## Why price/qty are strings

Exchange APIs return prices as strings to avoid floating-point precision loss. Keeping them as strings in Avro preserves this exactly. Flink converts to `BigDecimal` at processing time.

**Why:** Schema registry contract between NiFi and Flink for the order book pipeline.  
**How to apply:** Any new exchange integration must produce events conforming to this schema after NiFi normalization.

[[kafka-topic-strategy]]
