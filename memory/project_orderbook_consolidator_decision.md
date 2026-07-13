---
name: orderbook-consolidator-decision
description: Final decision for the greenfield flink/orderbook-consolidator/ rebuild — build the Order Book Consolidator as a Java Flink DataStream job; requirements R1, R2, R4, R5, R6 for now, with R3 (per-(exchange,pair) stale_time expiry) postponed to a later phase
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
  `quantity`. Display-only fields (`exchange_name`, `base`, `quote`) are not carried on the wire —
  see the wire schema section below.
- **Stale-time config stream (low volume, postponed — see "Postponed" section below).** A
  compacted stream carrying `stale_time` per `(exchange_id, pair_id)`, for use once that feature
  is built.

**Output:** one consolidated book snapshot per `(pair, side)`, published to Kafka topic
`p{pair_id}-{side}`, in a fixed shape (`pair_id`, `side`, `event_time`, `levels[]` with
`exchange_id`/`price`/`quantity` per entry) that a downstream consumer already depends on and
cannot change.

**Clarification ratified by user (2026-07-04):** re-reviewed the required behaviour against the
running implementation — it matches. Two points confirmed explicitly:
- **Topic separator stays hyphen** (`ex1-p2-asks` → `p2-asks`). The user's underscore notation
  was informal; hyphen is the live convention ([[kafka-topic-strategy]]).
- **Output level price is the CANONICAL stripped form**, not the raw wire string — e.g. input
  `"97240.50"` is emitted as `"97240.5"` (stage-1 keys/emits `stripTrailingZeros().toPlainString()`).
  This was previously only a noted side-effect; it is now a ratified requirement. **Do NOT "fix" the
  output price back to the original wire string.** ([[bigdecimal-rules]])

**Scale:** many pairs × 2 sides × several exchanges, high-frequency level updates; stale-time
config updates are rare; prices/quantities must be handled as exact decimals, never floating point.

## Input event wire schema (one price level per message)

Each price-level message is a single flat level — **not** the old batched
`levels[]`/`type`/`sequence_*` shape from `flink/orderbook-job/`. Concrete JSON:

```json
{
	"exchange_id": 1,
	"pair_id": 2,
	"side": "asks",
	"event_time": 1750680000000,
	"price": "97240.50",
	"quantity": "0.42"
}
```

- `exchange_id`, `pair_id`, `side`, `event_time` — identity + freshness (the operator's key
  material and R1's "latest" tie-break).
- `price`, `quantity` — **strings**, parsed to `BigDecimal` from the wire string, never a float
  (see `project_bigdecimal_rules`). `quantity == "0"` is the R2 removal signal.

Display-only fields (`exchange_name`, `base`, `quote`) are not part of this wire schema — they are
ignored by the job's logic and, if needed for display purposes downstream, should be resolved from
`exchange_id`/`pair_id` via a lookup rather than carried on every message.

This shape exists as `schemas/price_level_event.avsc` + `schemas/price_level_event_example.json`
(registry subject `price-level-event`) — see [[avro-schema-orderbook]].

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

**Mechanism:** `KeyedProcessFunction` keyed on `(pair, exchange, side)`, backed by
`MapState<price, (quantity, event_time)>`. The key is **always** `(pair, exchange, side)` — one
keyed instance per exchange's side of a pair — with `price` as the map key inside that state
(locked Step 0, 2026-07-04; hard rule — never key the level state by `(pair, side)` alone). On each
incoming event, if its `event_time` is newer than the stored one for that price, overwrite both
fields in place — this is exactly the upsert behavior in the example above (price 10's entry is
replaced, not duplicated). Because keyed state is scoped to its own key, this operator sees only
one exchange, so the cross-exchange union (R4) happens in a **second operator re-keyed by
`(pair, side)`** — see R4.

### R2 — Explicit removal

**Requirement:** a message with `quantity = 0` removes that price level.

**Mechanism:** in the same process function, `quantity == 0` → `state.remove(price)`, and emit a
retraction for that level.

### R4 — Cross-exchange consolidation

**Requirement:** union all exchanges for a `(pair, side)` into one book; never sum quantities
across exchanges (equal prices from different exchanges stay separate, adjacent entries).

**Mechanism:** a **second operator re-keyed by `(pair, side)`** consumes the per-exchange books
emitted by the R1 operator (which is keyed by `(pair, exchange, side)` and so cannot see other
exchanges). It holds each exchange's latest levels in `MapState<exchange_id, levels>` and, on each
update, rebuilds the union across all exchanges, emitting an array of `(exchange_id, price,
quantity)` entries — never aggregated/summed.

### R5 — Ordering & precision

**Requirement:** asks ascending by price, bids descending; tie-break equal price by larger
quantity first; all arithmetic on exact decimals, never floating point.

**Mechanism:** sort the union list with a comparator implementing that ordering; use
`BigDecimal` built directly from the wire string for all price/quantity values and arithmetic.

### R6 — Output to many topics

**Requirement:** publish to Kafka topic `p{pair_id}-{side}` per `(pair, side)`, in a fixed shape
that must not change.

**Mechanism:** a single `KafkaSink` whose serialization schema selects the target topic per
record — native dynamic topic routing from one operator, no per-pair job templating needed.

## Implementation status & non-obvious decisions (built, tested)

The job is fully built in `flink/orderbook-consolidator/` (base package
`io.tibobit.consolidator`, main class `OrderBookConsolidatorJob`, 25/25 spec-based tests green —
[[tdd-workflow]]). Decisions a future session would otherwise re-derive:

- **Each flink project is fully self-contained** — `flink/orderbook-job/` and
  `flink/orderbook-consolidator/` each hold their own `run-job.sh`/`Makefile`/`Dockerfile`/
  `confluent-deps-pom.xml`; the repo root has one compose file per stack
  (`docker-compose-orderbook-job.yml` / `docker-compose-orderbook-consolidator.yml`). **Run ONE
  stack at a time** — they use identical container_names/ports.
- **Single Kafka source over ALL input topics** via pattern `ex[0-9]+-p[0-9]+-(asks|bids)` @
  latest offsets; route by the event's own pair/exchange/side through `keyBy`. No per-pair
  sources, no PairsLoader/Postgres dependency — new pairs/exchanges are picked up automatically.
  The leading `ex` segment is what excludes the output topics (no feedback loop).
- **Stage-1 price MapState key is CANONICALIZED** —
  `new BigDecimal(price).stripTrailingZeros().toPlainString()` — because Flink MapState is
  hash-based and won't collapse scale like orderbook-job's `TreeMap` did ([[bigdecimal-rules]]).
- **Stage-2 sort direction is chosen per-record from `book.getSide()`** (both comparators built
  in `open()`), not per-operator-instance — one operator serves both sides in the unified topology.
- **Both `keyBy`s use anonymous `KeySelector` classes, NOT lambdas** — Flink can't infer the
  String key type from a concat lambda.
- Stage-1 drops stale events (`event_time < stored`) without emitting; qty=0 on an absent price
  is also a no-emit; an emitted `ExchangeBook`'s `event_time` = max over its levels (or the
  trigger event's time when the book empties).

## Postponed — R3, stale-level expiry (`stale_time`)

This requirement is **not being built in this phase**. It's captured here so it isn't lost, and
will be picked up later.

**What it is:** every price level needs a way to disappear from the order book on its own if
nothing refreshes it for a while — even if the source never explicitly tells us to remove it.
Each `(exchange, pair)` will have its own configurable `stale_time`: the maximum duration a price
level is allowed to go without a new update before it's considered stale and dropped from the
book. Different exchange-pairs can have different `stale_time` values — some sources may be
expected to update more frequently than others, so what counts as "gone quiet for too long"
differs per exchange-pair.

**Why it matters:** without this, a price level could be shown as still active in the
consolidated book indefinitely, even though the exchange that posted it has stopped sending any
updates for it (e.g. a disconnect, an outage, or simply because that price is no longer being
quoted) and never sent an explicit removal message. `stale_time` is what keeps the book honest in
that situation — a level is only ever shown if it has been refreshed recently enough, according to
its own exchange-pair's definition of "recently enough."

**What's deferred, specifically:** the mechanics of _how_ this gets implemented (timers, clocks,
state management, etc.) are intentionally left out of this decision for now. That will be
revisited when this requirement is picked back up.

## Why Java over Flink SQL

Flink SQL is more concise for R1, R2, R4, and R5 — deduplication, `WHERE quantity > 0`,
`ARRAY_AGG` with `ORDER BY`, and `DECIMAL` types all have native, few-line SQL expressions for
those requirements. For the requirements being built now, the deciding factor is R6:

- **R6 (many output topics) — the deciding requirement for now.** A SQL Kafka sink writes to one
  topic; the connector has no per-row topic routing. Emitting all `p{pair_id}-{side}` topics in
  SQL would require generating one `INSERT` statement per `(pair, side)` at startup and bundling
  them into a single `StatementSet` — workable, but the job becomes many templated inserts rather
  than one query, and needs the active-pairs catalog to be known at startup. Java's `KafkaSink`
  routes per record natively from a single operator, with no such constraint.

- **General flexibility.** Java gives full low-level stream control (custom windows, side
  outputs, complex state, timers) for requirements beyond what SQL expresses declaratively. This
  keeps the door open for future needs — including R3 once it's picked back up — without having
  to re-platform the job later.

Additional trade-offs accepted by choosing Java over SQL:

- **More code** for R1/R2/R4/R5 — explicit operators instead of a few lines of declarative SQL.
- **No hot logic changes** — changing processing logic requires a recompile and redeploy of the
  job jar, unlike a SQL script that could be changed via a SQL client/gateway without a new build.
- **Test style shifts to unit-style** — operator test harnesses (fast, isolated) rather than
  integration-style `TableEnvironment` + MiniCluster tests. This is a net positive for iteration
  speed, but a different testing approach than SQL would use.

## Bottom line

For the requirements being built now (R1, R2, R4, R5, R6), Java is chosen mainly because of R6:
Flink SQL has no native way to route output to many dynamically-named topics without per-pair
statement templating, while Java's `KafkaSink` does this natively from a single operator. Java
also preserves full low-level control for requirements not yet built. R3 (stale-level expiry via
`stale_time`) is postponed to a later phase — once it's picked back up, it is expected to further
reinforce the choice of Java, since SQL has no native way to express a per-exchange-pair,
independently-configurable expiry duration with a guaranteed removal signal. For now, the Order
Book Consolidator should be built in Java on the Flink DataStream API, covering R1, R2, R4, R5,
and R6.

## Deployment: docker-compose volumes & restart policy

`docker-compose-orderbook-consolidator.yml`:

- `jobmanager`/`taskmanager` mount named log volumes at `/opt/flink/log`
  (`data-collector-jobmanager-logs`/`-taskmanager-logs`). `kafka-ui`/`web` are deliberately
  volume-free (stateless; schema-registry's state lives in the Kafka `_schemas` topic). No
  checkpoint/savepoint volume because checkpointing is not enabled (`DeliveryGuarantee.NONE`,
  fire-and-forget sink) — add one if checkpointing is ever turned on.
- All 9 base-image `VOLUME` mount points (nifi ×6, schema-registry secrets, postgres
  `/var/lib/postgresql` parent dir, kafka secrets) are explicitly named
  (`data-collector-<service>-<purpose>` convention) so future container recreation doesn't spawn
  anonymous volumes. **The pre-existing anonymous volumes on the deploy server were deliberately
  NOT touched** — user wants to decide separately whether/how to migrate their data.
- **`restart: on-failure` on every service** (commit `1d08353`). Reason: schema-registry crashed
  on a cold start while kafka was `healthy` — kafka's healthcheck (`kafka-topics --list`) only
  proves the broker responds, not that the freshly auto-created `_schemas` topic has an elected
  leader; schema-registry's one-shot init write hit `NotLeaderOrFollowerException` and the
  Confluent image treats that as fatal (no internal retry). This "dependency reports healthy but
  isn't fully ready" race can hit any service on cold start. **Not yet deployed to the server** —
  user will `git pull` + recreate there themselves when ready.

## Wire format: true Confluent Avro, schema from the registry ONLY

The module speaks real Confluent Schema Registry Avro binary (magic byte + schema id + payload)
on both source and sink — no Jackson/JSON anywhere in it (the old `flink/orderbook-job` is the
one still on JSON — [[avro-schema-orderbook]]).

**User rule: every event MUST be validated using the schema in the Schema Registry, and nowhere
else.** Current state honors it:

- **Deserializer** (`PriceLevelEventDeserializer`): wraps
  `ConfluentRegistryAvroDeserializationSchema.forGeneric(schema, schemaRegistryUrl)`, maps the
  `GenericRecord` onto `PriceLevelEvent` via a package-private static
  `toPriceLevelEvent(GenericRecord)` (pure/testable without a live registry).
- **Serializer** (`ConsolidatedOrderBookSerializer`): wraps
  `ConfluentRegistryAvroSerializationSchema.forGeneric(subject, schema, schemaRegistryUrl)`,
  subject `consolidated-order-book-event`; builds the `GenericRecord` via a package-private
  static `toGenericRecord(book, schema)`.
- **Schema objects are fetched live from the registry at runtime** —
  `AvroSchemaLoader.loadLatest(schemaRegistryUrl, subject)` via `CachedSchemaRegistryClient
  .getLatestSchemaMetadata(subject)`, lazily on first use, subjects `price-level-event` /
  `consolidated-order-book-event`. The shaded jar contains **no `.avsc` files** (an earlier
  iteration bundled classpath copies via maven-resources-plugin — rejected as violating the rule;
  drift risk vs what `warmup.sh` registered). `AvroSchemaLoader.load(classpathResource)` survives
  **test-only**: the `copy-avro-schemas` pom execution copies the canonical `schemas/*.avsc` onto
  the **test** classpath so unit tests can build fixture `GenericRecord`s without a registry.
- **Consequence:** hard runtime dependency on the registry being reachable with both subjects
  registered *before* the job starts (true today — `warmup.sh` runs first in the deploy flow).
  Registry down / subject missing → `IllegalStateException` on first (de)serialize — correct
  fail-fast for the rule, not a regression.
- `warmup.sh` is exempt from the rule — it's the schema *deployment* tool (the only place the
  repo-root `schemas/*.avsc` files feed into the registry for this module).
- **`SCHEMA_REGISTRY_URL` env var** on `OrderBookConsolidatorJob`, default
  `http://schema-registry:8082` (same pattern as `KAFKA_BOOTSTRAP_SERVERS`).
- Tests build `GenericRecord`s via `GenericRecordBuilder` and assert on the pure mapping
  functions — the Confluent encode/decode itself is library code, not re-tested.

**⚠️ Open deploy-blocking risk:** NiFi's producer format for the input topics
(`ex{exchange_id}-p{pair_id}-{side}`) is unknown/unverified — NiFi is owned by a separate team.
If NiFi publishes plain JSON, `PriceLevelEventDeserializer` fails on every message. Must be
confirmed before deploying — though the whole NiFi-normalizes path is slated for replacement by
[[raw-pipeline-decision]], which makes this moot after cutover. (`web/` was already matched to
Avro — see [[orderbook-web]].)
