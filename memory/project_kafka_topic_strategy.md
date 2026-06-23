---
name: kafka-topic-strategy
description: Technical decision on Kafka topic naming, key, and partitioning strategy for the NiFi → Kafka → Flink order book pipeline
metadata:
  type: project
---

## Decision: Topic per pair+side+exchange

**Topic** → `{pair}_{side}_{exchange}` (e.g. `BTC_USDT_asks_nobitex`, `BTC_USDT_bids_bitpin`)  
**Key** → null (irrelevant; one exchange per topic guarantees ordering)  
**Body** → price, qty, timestamp, exchange, pair, side

## Rationale

- One exchange publishes to one topic — ordering is guaranteed with a single partition, no key needed
- Partition count can be 1 per topic; no skew, no idle partitions, no repartitioning when exchanges are added or removed
- Flink uses regex subscription to aggregate across exchanges: `BTC_USDT_asks_.*` picks up all exchanges for that pair+side automatically — new exchanges are picked up with no Flink config change
- Topic count at scale: 10 exchanges × 200 pairs × 2 sides = 4000 topics — fine for modern Kafka (KRaft)

## What was rejected and why

| Option | Rejected because |
|---|---|
| `asks` / `bids` as topics, pair as key | Flink streams not separated by pair |
| `{pair}` as topic, side as key | Flink still needs `filter()` to split sides; only 2 effective partitions |
| `{pair}_{side}` as topic, exchange as key | Partition count doesn't align with exchange count per pair; varies per topic and changes over time |

## Out of scope (deferred)

Processing all pairs for one exchange (e.g. everything from `nobitex`) is not a current requirement. If needed later, a separate stream can be defined for that use case.

## Operational note

Topics are pre-provisioned from the postgres markets table — not auto-created. Topic provisioning is deferred until the markets table structure is finalized.

**Why:** NiFi → Kafka → Flink pipeline for collecting and normalizing exchange order book data (asks + bids) across up to 200 trading pairs.  
**How to apply:** Use this structure for all Kafka topic definitions, NiFi routing logic, and Flink source configurations in this project.
