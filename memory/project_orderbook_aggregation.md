---
name: orderbook-aggregation
description: Requirement to generate consolidated order books per pair+side by merging all exchanges into one stream in Flink
metadata:
  type: project
---

## Requirement: consolidated order book per pair+side

For each `{pair}-{side}` we must **generate a consolidated order book** that merges
the order book data of all exchanges for that pair+side into a single stream.

Desired output streams (one per pair+side, exchanges merged in):

```
BTC-USDT-asks
BTC-USDT-bids
TON-USDT-asks
TON-USDT-bids
...
```

## Two layers of "merge"

1. **Transport merge (done)** — the Kafka source already pulls all exchange topics
   for a pair+side into one DataStream via regex `{pair}-{side}-.*`. See
   [[kafka-topic-strategy]].
2. **Stateful generation (this requirement)** — on top of that merged stream, build
   and emit a consolidated order book. Plan: `keyBy(pair+side)` → keep latest snapshot
   per exchange in keyed state → merge into one book on each event → emit
   `{pair}-{side}` stream.

## Resolved: merge semantics (UNION, do NOT combine quantities)

Union all exchanges' levels and sort by price; do **not** sum quantities at equal price.
Each output level keeps its own exchange. Equal-price levels from different exchanges
appear as separate, adjacent entries.

Example — BTC-USDT asks:
- nobitex: qty 10 @ 100000
- wallex:  qty 4  @ 100000

Output is NOT `qty 14 @ 100000`; it is two adjacent entries:
- `{ exchange: nobitex, qty: 10, price: 100000 }`
- `{ exchange: wallex,  qty: 4,  price: 100000 }`

Rules:
- Asks sorted ascending by price; bids descending (highest first).
- Tie-break at equal price (different exchanges): larger quantity first.
- Price and quantity compared as `BigDecimal` (decimal strings; avoid float error).
- Output model: new `ConsolidatedOrderBook { pair, side, levels }` where each level is
  `{ exchange, price, quantity }`. `OrderBookEvent`/`PriceLevel` can't be reused as-is
  because exchange must live on each level, not the envelope.

**Why:** This is the core business value of the pipeline — a single normalized,
cross-exchange order book view per trading pair and side, rather than fragmented
per-exchange snapshots.
**How to apply:** Implement as a keyed stateful operator (`KeyedProcessFunction` keyed by
`pair`, `MapState<exchange, OrderBookEvent>` holding the latest snapshot per exchange —
the full event is stored so `event_time` is available for the max) between the Kafka
sources and the sink in `OrderBookJob`. On each event, replace that exchange's snapshot,
rebuild the union, sort by price, and emit a `ConsolidatedOrderBook`.

## Decision: DataStream (Java), not Flink SQL

**Q:** Why is `OrderBookMerger` implemented in pure Java (DataStream API) instead of Flink SQL?

**A:** The merge produces, per `(pair, side)`, a single row holding a **nested array of
`{exchange, price, quantity}` levels sorted by a custom comparator**, recomputed on every
update. That is three things Flink SQL handles poorly on a streaming changelog:

1. **Nested array output** — you'd `UNNEST` each exchange's `levels`, then re-collect into one
   sorted array per group; Flink SQL has no clean "sorted `ARRAY_AGG`" for continuous streams.
2. **Custom sort** — price asc/desc by side + tie-break by quantity desc, as `BigDecimal`;
   expressible row-wise but not as "sort the array inside the row".
3. **Latest-per-exchange state** — SQL *can* do this (`ROW_NUMBER() … PARTITION BY exchange`
   + dedup), but it's the easy part.

The `KeyedProcessFunction` does all three in ~30 lines with explicit state and a `Comparator`.
SQL would be more code, less readable, and hit engine limits — the opposite of what SQL buys you.

Rule of thumb for the whole pipeline: **imperative / stateful / array-shaped → DataStream;
declarative / windowed / analytical → SQL.** Flink SQL would earn its place later for analytics
(OHLC/candles, VWAP, windowed depth/liquidity, top-N) as *separate* jobs, not for this merge.

## Implemented (Phase 1)

- `model/ConsolidatedLevel.java` — `{ exchange, price, quantity }`
- `model/ConsolidatedOrderBook.java` — `{ pair, side, levels, eventTime }` (eventTime = max of contributing snapshots)
- `aggregation/OrderBookMerger.java` — `KeyedProcessFunction`; `MapState<exchange, OrderBookEvent>`;
  comparator = price (asc asks / desc bids) then quantity desc, both `BigDecimal`
- `OrderBookJob.addStream` wires `source → keyBy(pair) → OrderBookMerger(side)` then fans out to
  TWO sinks: a Kafka sink to topic `{pair}-{side}` (e.g. `BTC-USDT-asks`) AND `print(name)` to stdout
- `serializer/ConsolidatedOrderBookSerializer.java` — Jackson `SerializationSchema`, transient ObjectMapper
- `sink/OrderBookSinkFactory.java` — `KafkaSink<ConsolidatedOrderBook>`; builder default `DeliveryGuarantee.NONE`
  (we don't reference `DeliveryGuarantee` directly — `flink-connector-base` isn't on the compile classpath)
- No feedback loop: output topic `{pair}-{side}` does NOT match source pattern `{pair}-{side}-.*`
  (the trailing `-` after side is required), so the job won't re-consume its own output
- `ConsolidatedOrderBook.eventTime` serializes as `event_time` (matches `OrderBookEvent` snake_case convention)
- Flink 2.x note: operator uses `open(OpenContext)` (the 1.x `open(Configuration)` was removed in Flink 2.0)
- Functional test: `scripts/produce-test-data.sh` sends curated snapshots (BTC-USDT, TON-USDT;
  both sides; nobitex/wallex/bitpin) with intentional same-price collisions to verify union+tie-break
