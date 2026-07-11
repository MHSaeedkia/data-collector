---
name: kafka-topic-strategy
description: Technical decision on Kafka topic naming, key, and partitioning strategy for the NiFi → Kafka → Flink order book pipeline
metadata:
  type: project
---

## Decision: Topic per side+pair+exchange (ID-based names)

**Input topic** → `{side}-p{pair_id}-ex{exchange_id}` (e.g. `asks-p2-ex1`, `bids-p2-ex1`)
**Output topic** → `{side}-p{pair_id}` (e.g. `asks-p2`) — Flink writes the consolidated book here
**Key** → null (one exchange per input topic guarantees ordering)
**Body** → see [[avro-schema-orderbook]] (base, quote, pair_id, exchange_id, exchange_name, side, type, event_time, levels)

`pair_id` = `markets.id`, `exchange_id` = `exchanges.id`. Topic names use the **DB integer IDs**, not the human-readable base/quote/exchange_name. Side comes first.

## Rationale

- One exchange publishes to one input topic — ordering is guaranteed with a single partition, no key needed
- Partition count is 1 per topic; no skew, no idle partitions, no repartitioning when exchanges are added or removed
- Flink aggregates across exchanges via regex on input topics: `{side}-p{pair_id}-ex.*` (e.g. `asks-p2-ex.*`) picks up all exchanges for that side+pair automatically — new exchanges need no Flink config change
- Output topic `{side}-p{pair_id}` is a prefix of the input names, but the required `-ex` segment keeps it out of the input regex, so the job won't re-consume its own output (no feedback loop)
- IDs keep topic names compact and decoupled from display strings (avoids casing/charset issues). `exchanges.name` is unique and immutable (README RULE) so names would also be stable, but IDs are shorter
- Topic count at scale: 10 exchanges × 200 pairs × 2 sides = 4000 topics — fine for modern Kafka (KRaft)

## Migration note (changed 2026-06-28)

Topic naming was changed from human-readable to ID-based. **Migration is COMPLETE end to end:** `scripts/warmup.sh`, `schemas/orderbook_event.avsc`, the Flink job, and the web app are all on the ID-based scheme.

Old scheme: input `{base}-{quote}-{side}-{exchange_name}` (e.g. `BTC-USDT-asks-nobitex`), output `{base}-{quote}-{side}` (e.g. `BTC-USDT-asks`).

Flink job migration (done): Flink works **only with `pair_id` and `exchange_id`** — `base`, `quote`, `exchange_name` were removed from all Flink models. `OrderBookEvent` keeps `exchange_id`, `pair_id`, `side`, `type`, `event_time`, `levels` and is annotated `@JsonIgnoreProperties(ignoreUnknown=true)` so it tolerates the descriptive fields the wire JSON still carries (the avsc schema is unchanged). `PairsLoader` selects only `m.id` into `Pair(id)`. `OrderBookSourceFactory` subscribes by regex `{side}-p{pairId}-ex.*`. `OrderBookJob` keys by `pairId` and names operators + sink topic `{side}-p{pair_id}`. `OrderBookMerger` is `KeyedProcessFunction<Integer,…>` with `MapState<Integer,…>` keyed by `exchange_id`. `ConsolidatedOrderBook` = `{ pair_id, side, levels, event_time }`; `ConsolidatedLevel` = `{ exchange_id, price, quantity }`.

Web app migration (done): rewritten in Go (replacing Node `server.js`), subscribes by regex `^(asks|bids)-p\d+$` (output topics), and resolves `pair_id`/`exchange_id` → display labels from postgres since the output carries no base/quote/exchange_name. See [[orderbook-web]].

## What was rejected and why

| Option | Rejected because |
|---|---|
| `asks` / `bids` as topics, pair as key | Flink streams not separated by pair |
| `{pair}` as topic, side as key | Flink still needs `filter()` to split sides; only 2 effective partitions |
| `{pair}-{side}` as topic, exchange as key | Partition count doesn't align with exchange count per pair; varies per topic and changes over time |

## Out of scope (deferred)

Processing all pairs for one exchange (e.g. everything from `nobitex`) is not a current requirement. If needed later, a separate stream can be defined for that use case.

## Operational note

Topics are pre-provisioned by `scripts/warmup.sh` from the postgres `markets` + `exchange_markets` + `exchanges` tables (rows where `exchange_markets.status = 'subscribe'`) — not auto-created. warmup.sh first registers the Avro schema, then creates input + output topics (single partition, replication-factor 1).

**Retention (added 2026-07-11):** input topics `{side}-p{pair_id}-ex{exchange_id}` get `retention.ms=3600000` (1 hour); output topics `{side}-p{pair_id}` get `retention.ms=21600000` (6 hours), passed via `--config retention.ms=...` in `create_topic()`. Input topics are raw per-exchange firehose (short-lived, high volume); output topics are the consolidated book consumers care about (longer window for replay/late web-UI connects). **Caveat:** topic creation uses `--if-not-exists`, so retention is only applied when a topic is first created — topics already provisioned on the server before this change keep their old (unset/broker-default) retention and need `kafka-configs --alter --entity-type topics --entity-name <topic> --add-config retention.ms=...` to update in place; not yet run against the deployed server.

**Performance (2026-07-11):** `create_topic()` was slow because each call does `docker exec ... kafka-topics --create`, and `kafka-topics` is a JVM CLI tool — every invocation pays JVM startup cost (~1-3s) on top of the broker round-trip, and the old script ran these one at a time in a bash `while` loop (input-topic loop then output-topic loop), so total time scaled linearly with topic count (pairs × exchanges × 2 + pairs × 2). Fixed by building the full `topic retention_ms` list up front (both loops merged) and fanning the `docker exec` calls out via `xargs -P "$PARALLEL_JOBS" -n2 bash -c 'create_topic "$1" "$2"' _` (default `PARALLEL_JOBS=16`, override via env var) so many JVMs start concurrently instead of sequentially. `kafka-topics --if-not-exists` is safe under this concurrency since every topic name in the list is distinct. Kept the CLI-based approach (no new AdminClient script/dependency) — surgical fix, not a rewrite.

Its query selects only `m.id, em.exchange_id, e.name` (topic names are ID-based, so base/quote symbols are not needed) — updated 2026-06-29 when [[db-schema]] dropped `markets.base`/`quote`; the old query `SELECT m.id, m.base, m.quote, …` now errors. NOTE: all `exchange_markets` rows are seeded `unsubscribe`, so warmup creates zero topics until some are flipped to `subscribe`.

**Why:** NiFi → Kafka → Flink pipeline for collecting and normalizing exchange order book data (asks + bids) across up to 200 trading pairs.
**How to apply:** Use this structure for all Kafka topic definitions, NiFi routing logic, and Flink source configurations in this project.
