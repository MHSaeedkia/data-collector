---
name: orderbook-consolidator-decision
description: Final decision for the greenfield flink/orderbook-consolidator/ rebuild — build the Order Book Consolidator as a Java Flink DataStream job; requirements R1–R6, the decisive one being R3 (per-(exchange,pair) wall-clock TTL expiry via processing-time timers re-armed on every upsert)
metadata:
  type: project
---

# Order Book Consolidator — Final Decision

## Decision

Build the Order Book Consolidator as a **Java job on the Flink DataStream API** (not Flink SQL).

## What the job does

A Flink streaming job that produces a live, consolidated cryptocurrency order book per
`(pair, side)` by merging price levels from multiple exchanges in real time.

**Inputs:**

- **Price-level stream (high volume).** One Kafka topic per `(side, pair, exchange)`, subscribed
  via a topic-pattern. Each message is one price level, already cleaned, validated, ordered and
  de-duplicated upstream (NiFi): `exchange_id`, `pair_id`, `side`, `event_time`, `price`,
  `quantity` (plus display-only `exchange_name`/`base`/`quote`).
- **TTL config stream (low volume).** A compacted stream carrying `ttl_ms` per
  `(exchange_id, pair_id)`. TTL is uniform within one exchange-pair; different exchanges can have
  different TTLs.

**Output:** one consolidated book snapshot per `(pair, side)`, published to Kafka topic
`{side}-p{pair_id}`, in a fixed shape (`pair_id`, `side`, `event_time`, `levels[]` with
`exchange_id`/`price`/`quantity` per entry) that a downstream consumer already depends on and
cannot change.

**Scale:** many pairs × 2 sides × several exchanges, high-frequency level updates; TTL updates are
rare; prices/quantities must be handled as exact decimals, never floating point.

## Requirements and how Java satisfies each one

### R1 — Maintain latest levels (state model)

**Requirement:** for each `(pair, side, exchange, price)`, keep only the latest `quantity` for
that exact price, where "latest" is determined by `event_time`. This is an **upsert keyed on
price**, not an append — a new event for a price that's already known replaces that price's entry
entirely (both `quantity` and `event_time`), it never creates a second entry for the same price.

**Worked example.** Say the current state for one `(pair, side, exchange)` is:

```
price 10 -> qty 1
price 11 -> qty 2
price 12 -> qty 3
```

A new event arrives for price 10 with `quantity = 4` (and a newer `event_time`). The result is:

```
price 10 -> qty 4      (replaced, not added alongside the old qty 1)
price 11 -> qty 2
price 12 -> qty 3
```

**Mechanism:** `KeyedProcessFunction` keyed on `(pair, side, exchange, price)`, backed by
`MapState<price, (quantity, event_time)>`. On each incoming event, if its `event_time` is newer
than the stored one for that price, overwrite both fields in place — this is exactly the upsert
behavior in the example above (price 10's entry is replaced, not duplicated).

### R1 ↔ R3 interaction — TTL is per price-level entry, and resets on every refresh

Each price-level entry carries its own `event_time`, and that entry's TTL deadline is always
`event_time + ttl_ms` for **that entry's own, most recent `event_time`** — not the first time the
price was ever seen. Continuing the example: when price 10 is refreshed from qty 1 to qty 4, its
expiry deadline resets to the new event's `event_time + ttl_ms`. If no further refresh for price
10 arrives before that new deadline passes (in real wall-clock time), price 10 is dropped from the
resulting order book — independent of prices 11 and 12, which keep their own independent
deadlines from their own last `event_time`. This is why the TTL timer (R3) must be **re-armed on
every upsert**, not just set once when a price is first seen.

### R2 — Explicit removal

**Requirement:** a message with `quantity = 0` removes that price level.

**Mechanism:** in the same process function, `quantity == 0` → `state.remove(price)`, cancel that
price's pending TTL timer (it's no longer needed), and emit a retraction for that level.

### R3 — Time-based, per-exchange expiry

**Requirement:** a level not refreshed within its `ttl_ms` must drop out, judged against **real
wall-clock time**: keep the level while `event_time + ttl_ms >= current time`, drop it once
that's false — independent of whether new events keep arriving for that exchange-pair.

**Mechanism:** on every level upsert (R1), register a **processing-time timer** at
`System.currentTimeMillis() + ttl_ms`, with the TTL for that `(exchange_id, pair_id)` read from
**broadcast state** fed by the TTL stream. If the level is refreshed before the timer fires,
cancel the old timer and register a new one (see R1 ↔ R3 above). If the timer fires with no
refresh since, remove the level from state and emit a retraction. A **processing-time** timer
(not an event-time timer) is required: event-time timers only fire once the Flink watermark
passes the deadline, and the watermark for a key can stall indefinitely if that exchange-pair
goes idle — leaving a stale level in the book forever. A processing-time timer fires purely off
the wall clock, matching the actual requirement exactly.

### R4 — Cross-exchange consolidation

**Requirement:** union all exchanges for a `(pair, side)` into one book; never sum quantities
across exchanges (equal prices from different exchanges stay separate, adjacent entries).

**Mechanism:** rebuild the union list from the keyed state for all exchanges under that
`(pair, side)` on each event, emit as an array of `(exchange_id, price, quantity)` entries —
never aggregated/summed.

### R5 — Ordering & precision

**Requirement:** asks ascending by price, bids descending; tie-break equal price by larger
quantity first; all arithmetic on exact decimals, never floating point.

**Mechanism:** sort the union list with a comparator implementing that ordering; use
`BigDecimal` built directly from the wire string for all price/quantity values and arithmetic.

### R6 — Output to many topics

**Requirement:** publish to Kafka topic `{side}-p{pair_id}` per `(pair, side)`, in a fixed shape
that must not change.

**Mechanism:** a single `KafkaSink` whose serialization schema selects the target topic per
record — native dynamic topic routing from one operator, no per-pair job templating needed.

## Why Java over Flink SQL

Flink SQL is more concise for R1, R2, R4, and R5 — deduplication, `WHERE quantity > 0`,
`ARRAY_AGG` with `ORDER BY`, and `DECIMAL` types all have native, few-line SQL expressions for
those requirements. But R3 and R6 are where SQL falls short:

- **R3 (expiry) — the decisive requirement.** SQL's only relevant lever, `table.exec.state.ttl`,
  is:
    - **A single global value** — it cannot vary per exchange-pair, but R3 requires a different TTL
      per `(exchange_id, pair_id)`, driven by the TTL stream.
    - **Silent** — it frees idle state internally but never emits a retraction downstream, so an
      expired level would keep sitting in the last-published book. That is a correctness break, not
      a cosmetic gap.

    Because of both points, pure SQL cannot express this expiry at all. It can only fall back to
    trusting the upstream to always send an explicit `quantity = 0` removal (R2) whenever a level
    dies — which is not something the job itself can guarantee or verify. If that trust is not
    acceptable — and it is not, per the confirmed requirement that expiry inside the job be a
    correctness guarantee — SQL cannot satisfy R3.

- **R6 (many output topics) — a secondary, structural wrinkle.** A SQL Kafka sink writes to one
  topic; the connector has no per-row topic routing. Emitting all `{side}-p{pair_id}` topics in
  SQL would require generating one `INSERT` statement per `(pair, side)` at startup and bundling
  them into a single `StatementSet` — workable, but the job becomes many templated inserts rather
  than one query, and needs the active-pairs catalog to be known at startup. Java's `KafkaSink`
  routes per record natively from a single operator, with no such constraint.

Additional trade-offs accepted by choosing Java over SQL:

- **More code** for R1/R2/R4/R5 — explicit operators instead of a few lines of declarative SQL.
- **No hot logic changes** — changing processing logic requires a recompile and redeploy of the
  job jar, unlike a SQL script that could be changed via a SQL client/gateway without a new build.
- **Test style shifts to unit-style** — operator test harnesses (fast, isolated) rather than
  integration-style `TableEnvironment` + MiniCluster tests. This is a net positive for iteration
  speed, but a different testing approach than SQL would use.
- Java gives **full low-level stream control** (custom windows, side outputs, complex state,
  timers) for any future requirements beyond R1–R6, whereas SQL abstracts that away.

## Bottom line

Every requirement (R1–R6) is satisfied correctly in Java. R3 (per-exchange, wall-clock expiry) is
the requirement that actually forces the choice: it needs a TTL that varies per exchange-pair and
a mechanism that reliably emits a removal when a level goes stale, and Flink SQL has no primitive
that does both — it can only lean on trusting the upstream, which is not acceptable given expiry
is a correctness guarantee, not a best-effort safety net. R6 reinforces the same direction, since
SQL needs per-pair statement templating to reach many output topics where Java routes natively.
Combined with full low-level control for future needs, the Order Book Consolidator should be built
in Java on the Flink DataStream API, with **processing-time timers (re-armed on every level
refresh)** as the core mechanism for R3.
