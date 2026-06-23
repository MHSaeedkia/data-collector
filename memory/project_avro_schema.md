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
| `pair`      | string                  | Normalized with `-` separator, e.g. `BTC-USDT`              |
| `side`      | enum `asks\|bids`       | Mirrors topic suffix; included for self-describing messages |
| `event_time` | long (timestamp-millis) | Exchange-reported UTC timestamp in ms                       |
| `levels`    | array of PriceLevel     | Price + quantity both as string to preserve decimal precision |

## NiFi responsibility before publishing

NiFi is handled by a separate team and is not implemented in this repo. Documented here for readers to understand the contract this schema depends on.

- Normalize pair name to `{BASE}-{QUOTE}` format (e.g. `BTCUSDT` → `BTC-USDT`)
- Split raw exchange message (which contains both sides) into two separate events
- Route each event to the correct topic: `{pair}-{side}-{exchange}` (e.g. `BTC-USDT-asks-nobitex`)
- Set Kafka message key to `{exchange}`

## Why price/qty are strings

Exchange APIs return price and quantity as strings to avoid floating-point precision loss. Keeping them as strings in Avro preserves this exactly. Flink converts to `BigDecimal` at processing time.

**Why:** Schema registry contract between NiFi and Flink for the order book pipeline.  
**How to apply:** Any new exchange integration must produce events conforming to this schema after NiFi normalization.

[[kafka-topic-strategy]]
