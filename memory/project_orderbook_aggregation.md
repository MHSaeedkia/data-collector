---
name: orderbook-aggregation
description: Business rules for the consolidated order book per pair+side (union semantics, snapshot/update handling, sequence-gap rules) — and the status of the two Flink jobs implementing them
metadata:
  type: project
---

## Requirement: consolidated order book per pair+side

For each `(pair, side)` we **generate a consolidated order book** that merges the order book
data of all exchanges for that pair+side into a single stream, published to topic
`p{pair_id}-{side}` (e.g. `p2-asks`, `p2-bids` — [[kafka-topic-strategy]]).

## Merge semantics (UNION, do NOT combine quantities)

Union all exchanges' levels and sort by price; do **not** sum quantities at equal price.
Each output level keeps its own exchange. Equal-price levels from different exchanges
appear as separate, adjacent entries.

Example — BTC-USDT asks: nobitex qty 10 @ 100000, wallex qty 4 @ 100000 → output is NOT
`qty 14 @ 100000`; it is two adjacent entries `{exchange_id:1, qty:10, price:100000}` and
`{exchange_id:3, qty:4, price:100000}`.

Rules:
- Asks sorted ascending by price; bids descending (highest first).
- Tie-break at equal price (different exchanges): larger quantity first.
- Price and quantity compared as `BigDecimal` built from the wire string ([[bigdecimal-rules]]).
- Output shape: `ConsolidatedOrderBook { pair_id, side, event_time, levels }`, each level
  `{ exchange_id, price, quantity }` (`exchange_id` = int DB id, not the name) —
  frozen wire contract, see [[avro-schema-orderbook]].

Implementation rule of thumb for the whole pipeline: **imperative / stateful / array-shaped →
DataStream (Java); declarative / windowed / analytical → Flink SQL** (as separate jobs, e.g.
future OHLC/VWAP analytics). Full Java-vs-SQL rationale lives in
[[orderbook-consolidator-decision]].

## Snapshot + update event semantics (`type` field, [[avro-schema-orderbook]])

Within one exchange's stream (NiFi guarantees these):
- `snapshot` → replace that exchange's book wholesale.
- `update`   → apply on top of the existing book, per level: `quantity > 0` → insert-or-overwrite
  that price level; `quantity == 0` → delete it. Compare zero via `BigDecimal.compareTo(ZERO)`,
  never string `"0"`.

Price levels must never be keyed by raw wire string (NiFi gives no formatting guarantee —
`"97240.50"` vs `"97240.5"`); see the equality caveat in [[bigdecimal-rules]] for the two
accepted keying patterns.

## Sequence-id gap handling (strict contract)

Events carry `sequence_id` (monotonic per `(pair_id, exchange_id)` stream) and `sequence_jump`
(the expected delta from the previous message — increments are NOT +1, some exchanges jump ~300).
Per-exchange continuity rules:

- **snapshot** → accepted unconditionally (NO seq check — it IS the baseline); replaces the book,
  stores `lastSeq`, clears `awaitingSnapshot`. The only way to recover from a gap and the only way
  a book is first created (a leading `update` is ignored).
- **update** → requires a trusted baseline:
  - no book yet or `awaitingSnapshot` → ignore.
  - `seq <= lastSeq` → stale/duplicate, drop.
  - `seq != lastSeq + sequence_jump` (strict — any other forward value, gap or unexpected
    intermediate) → **resync**: clear the book's levels + set `awaitingSnapshot`, re-emit (that
    exchange drops out of the consolidated book), wait for the next snapshot. Chosen action is
    drop-book-and-resync (user decision — never serve a drifted book), not apply-anyway.
  - `seq == lastSeq + sequence_jump` → in order: apply deltas, advance `eventTime`/`lastSeq`.

The strictness (`!=`, not a band) was pinned by TDD — see [[tdd-workflow]].

These semantics are reused by job 2 (type validation) of [[raw-pipeline-decision]].

## Where this is implemented

- **`flink/orderbook-job/`** — the original job: consumes batched `OrderBookEvent`s (JSON),
  `OrderBookMerger` (`KeyedProcessFunction` keyed by pairId, `MapState<exchange_id, ExchangeBook>`,
  `TreeMap<BigDecimal,…>` levels) implements all of the above including snapshot/update + gap
  handling. **Superseded by the consolidator and still on the OLD side-first topic naming**
  ([[kafka-topic-strategy]]); may be retired after [[raw-pipeline-decision]] cutover.
- **`flink/orderbook-consolidator/`** — the current job: consumes flat per-level
  `PriceLevelEvent`s (no `type`/`sequence_*` — upstream is trusted to have applied them),
  implements the union/sort/no-summing rules — see [[orderbook-consolidator-decision]].

## Deferred: cold start / checkpointing

**No Flink checkpointing/state backend is configured** (compose files only set
`jobmanager.rpc.address` + `taskmanager.numberOfTaskSlots`), so keyed state is in-memory and lost
on every restart; books only rebuild once each exchange sends a fresh snapshot / fresh levels.
Fix options for later: (A) Flink checkpoints + durable state backend (recommended, idiomatic);
(B) log-compacted output topics + reseed on startup (also gives downstream consumers the current
book instantly); (C) both.
