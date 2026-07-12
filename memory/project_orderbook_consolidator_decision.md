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
`{side}-p{pair_id}`, in a fixed shape (`pair_id`, `side`, `event_time`, `levels[]` with
`exchange_id`/`price`/`quantity` per entry) that a downstream consumer already depends on and
cannot change.

**Clarification ratified by user (2026-07-04):** re-reviewed the required behaviour against the
running implementation — it matches. Two points confirmed explicitly:
- **Topic separator stays hyphen** (`asks-p2-ex1` → `asks-p2`). The user's underscore notation
  (`asks_p2`) was informal; hyphen is the live convention across web/orderbook-job/warmup ([[kafka-topic-strategy]]).
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

Both an **Avro** schema (Confluent Schema Registry, the production transport) and a matching
**JSON** example must be created for this shape under `schemas/` — e.g.
`schemas/price_level_event.avsc` + `schemas/price_level_event_example.json` — mirroring the
existing `orderbook_event.avsc` conventions but flat (drop `type`, `sequence_id`, `sequence_jump`,
`levels[]`; add scalar `price`/`quantity` strings, and omit display-only fields).

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

**Requirement:** publish to Kafka topic `{side}-p{pair_id}` per `(pair, side)`, in a fixed shape
that must not change.

**Mechanism:** a single `KafkaSink` whose serialization schema selects the target topic per
record — native dynamic topic routing from one operator, no per-pair job templating needed.

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
  topic; the connector has no per-row topic routing. Emitting all `{side}-p{pair_id}` topics in
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

## Deployment: docker-compose volumes (2026-07-08)

`docker-compose-orderbook-consolidator.yml` `jobmanager`/`taskmanager` now mount named volumes at
`/opt/flink/log` (`data-collector-jobmanager-logs`, `data-collector-taskmanager-logs`) so Flink
logs survive container recreation. `schema-registry`/`kafka-ui`/`web` were deliberately left
without volumes — they hold no local persisted state (schema-registry's state lives in the Kafka
`_schemas` topic; kafka-ui and web are stateless and log to stdout), so an empty bind mount there
would be a no-op. No checkpoint/savepoint volume was added because checkpointing is not enabled in
the job (`DeliveryGuarantee.NONE`, fire-and-forget sink — see source comments in
`ConsolidatedOrderBookSinkFactory`); add one if checkpointing is ever turned on.

**Anonymous volumes eliminated (2026-07-08):** SSH'd into the deploy server (`tibobit-data-collector`,
requires `sudo docker ...` — [[server-build-env]]) and found 9 anonymous volumes, all coming from
`VOLUME` instructions baked into base images, not from the compose file: `nifi` had 6
(`python_extensions`, `database_repository`, `nar_extensions`, `provenance_repository`,
`content_repository`, `flowfile_repository`), `schema-registry` had 1 (`/etc/schema-registry/secrets`),
`postgres` had 1 (`/var/lib/postgresql` — the parent dir; distinct from the already-named
`/var/lib/postgresql/18/docker`), `kafka` had 1 (`/etc/kafka/secrets`). Added explicit named volumes
for all 9 mount points in `docker-compose-orderbook-consolidator.yml` (`data-collector-nifi-*`,
`data-collector-schema-registry-secrets`, `data-collector-postgres-lib`, `data-collector-kafka-secrets`),
following the existing `data-collector-<service>-<purpose>` naming convention. `kafka-ui`/`web`
confirmed still volume-free (stateless, consistent with the note above). **Deliberately did NOT
touch the existing anonymous volumes on the server** (user wants to decide separately whether/how
to migrate any data from them) — this change only affects what happens on future container
recreation; a `docker compose up` on the current containers won't retroactively adopt the new named
volumes for already-running containers.

**schema-registry startup race + `restart: on-failure` (2026-07-08):** on a fresh `docker compose up`
on the server (after the volume changes above), `schema-registry` exited (1) while `kafka` was
`healthy`. Root cause confirmed via `docker logs schema-registry`: Kafka's healthcheck
(`kafka-topics --list`) only proves the broker responds — it doesn't guarantee the freshly
auto-created `_schemas` topic already has an elected partition leader. schema-registry's one-shot
init write (`Noop` record) hit `NotLeaderOrFollowerException` and the Confluent image treats this
as fatal (no internal retry) rather than backing off. Confirmed unrelated to the volume changes
(`_schemas` had a healthy leader moments later; `/etc/kafka/secrets` / `/etc/schema-registry/secrets`
played no part). Fixed live via `docker start schema-registry` (came up healthy on retry), and
**added `restart: on-failure` to every service** in `docker-compose-orderbook-consolidator.yml`
(not just schema-registry) since this class of "dependency reports healthy but isn't fully ready"
race can in principle hit any service here on a cold start — commit `1d08353`. **Not yet deployed
to the server** — user will `git pull` + recreate there themselves when ready.

## Wire format: JSON → true Confluent Avro (2026-07-11)

`flink/orderbook-consolidator/` refactored end-to-end from Jackson JSON to real Confluent
Schema Registry Avro binary encoding, on both source and sink. This supersedes the
[[avro-schema]] note that `.avsc` registration was "documentation only" — that is now **only
still true for `orderbook_event.avsc`/`orderbook-job`**, which was NOT touched. For this module:

- **Deserializer** (`PriceLevelEventDeserializer`): wraps
  `ConfluentRegistryAvroDeserializationSchema.forGeneric(schema, schemaRegistryUrl)`, maps the
  resulting `GenericRecord` onto the existing `PriceLevelEvent` POJO via a package-private static
  `toPriceLevelEvent(GenericRecord)` (kept pure/testable without a live registry).
- **Serializer** (`ConsolidatedOrderBookSerializer`): wraps
  `ConfluentRegistryAvroSerializationSchema.forGeneric(subject, schema, schemaRegistryUrl)`,
  subject `consolidated-order-book-event` (matches `warmup.sh`); builds the `GenericRecord` from
  `ConsolidatedOrderBook` via a package-private static `toGenericRecord(book, schema)`.
- **Schema source of truth stays `schemas/*.avsc`** — no duplication. A `maven-resources-plugin`
  `copy-resources` execution (bound to `generate-resources`) copies
  `price_level_event.avsc`/`consolidated_order_book_event.avsc` from the repo-root `schemas/`
  dir into `target/classes/avro/`, so they end up bundled in the shaded jar at `avro/*.avsc` —
  required because only the jar (not the repo) exists in the Flink containers at runtime. Loaded
  via `io.tibobit.consolidator.avro.AvroSchemaLoader.load("/avro/...")`, a tiny
  `Schema.Parser().parse(classpath-resource)` wrapper.
- **`SCHEMA_REGISTRY_URL` env var added** to `OrderBookConsolidatorJob`, default
  `http://schema-registry:8082` (in-network hostname:port from `docker-compose-orderbook-consolidator.yml`,
  same pattern as the existing `KAFKA_BOOTSTRAP_SERVERS` default) — not set explicitly in the
  compose file, matching how bootstrap-servers is handled too.
- **pom.xml**: added `provided`-scope `avro`, `flink-avro`, `flink-avro-confluent-registry`,
  `kafka-schema-registry-client` (+ confluent maven repo) — versions match what the Dockerfile
  already `wget`s into `/opt/flink/lib/` (this infra was pre-staged in the Docker image before
  this refactor but unused — see the Dockerfile's "Avro format + Confluent Schema Registry
  support" block, present since the image was first built). Removed the `jackson-databind`
  dependency entirely — nothing in this module uses Jackson anymore.
- **Jackson annotations removed** (`@JsonProperty`, `@JsonIgnoreProperties`) from the model POJOs
  (`PriceLevelEvent`, `ConsolidatedLevel`, `ConsolidatedOrderBook`) — they did nothing once Jackson
  left the wire path. `ExchangeBook`/`StoredLevel` were untouched (never had Jackson annotations —
  they're inter-operator/state records, never touch Kafka).
- Tests rewritten to build `GenericRecord`s via `GenericRecordBuilder` and assert on the
  `toPriceLevelEvent`/`toGenericRecord` pure-mapping functions directly, rather than feeding raw
  JSON strings — the Confluent registry encode/decode itself is Flink/Confluent library code, not
  re-tested here. 25/25 tests green (`mvn -o test`); shaded jar verified to contain
  `avro/price_level_event.avsc` + `avro/consolidated_order_book_event.avsc` (`jar tf`).

**⚠️ Known blast radius NOT yet addressed (deliberately out of scope for this refactor — user
asked only to refactor this module):**

1. **`web/internal/ingest/ingest.go` (`HandleRecord`) still does `json.Unmarshal(value, &rb)`** on
   the raw Kafka record bytes from the `{side}-p{pair_id}` output topics. Now that
   `ConsolidatedOrderBookSerializer` emits Confluent-wire-format Avro binary (magic byte + 4-byte
   schema id + Avro payload) instead of JSON, every message will fail `json.Unmarshal` and get
   silently dropped (`"Skipping bad message on %s: %v"`) — **the web UI will stop receiving any
   live order book updates** until `web/` is updated to decode Avro via the schema registry (e.g.
   `hamba/avro` + a schema-registry-aware wire-format reader) instead of `encoding/json`. See
   [[orderbook-web]].
2. **NiFi's producer format for the input topics (`{side}-p{pair_id}-ex{exchange_id}`) is
   unknown/unverified** — NiFi is owned by a separate team and not implemented in this repo (see
   [[avro-schema]]). If NiFi is still publishing plain JSON (as `price_level_event.avsc`'s prior
   "documentation only" status implied it might be), `PriceLevelEventDeserializer` will now fail
   to decode every incoming message (`ConfluentRegistryAvroDeserializationSchema` expects the
   Confluent magic-byte-prefixed wire format, not raw JSON). **This must be confirmed with the
   NiFi team before deploying this refactor**, or the consolidator will receive nothing but decode
   errors on its input side.

Both are real, deploy-blocking risks, not just cleanup items — flagged here rather than silently
left for a future session to rediscover the hard way.

## Schema source of truth: registry-only at runtime, not the bundled jar copy (2026-07-12)

User rule: every event to producers/consumers **MUST be validated using the schema in the Schema
Registry, and nowhere else**. The 2026-07-11 refactor above technically violated this — both
(de)serializers loaded their Avro `Schema` object from a classpath resource
(`avro/price_level_event.avsc`/`avro/consolidated_order_book_event.avsc`) bundled into the shaded
jar at build time. That local copy, not the registry, was the actual source of the schema shape:
on serialize it was pushed to the registry (drift risk if the jar was stale vs. what `warmup.sh`
had registered); on deserialize it was used as the Avro *reader* schema for projection. Fixed:

- **`AvroSchemaLoader.loadLatest(schemaRegistryUrl, subject)`** (new method) fetches the schema
  live via `io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
  .getLatestSchemaMetadata(subject).getSchema()`, parsed with `Schema.Parser().parse(...)`. Both
  `PriceLevelEventDeserializer` and `ConsolidatedOrderBookSerializer` now call this (lazily, same
  transient-field-on-first-use pattern as before) instead of `AvroSchemaLoader.load(classpathResource)`.
  Subjects used are the existing fixed ones `price-level-event` / `consolidated-order-book-event`
  (unchanged, already registered by `warmup.sh` — see [[kafka-topic-strategy]]), **not** the
  per-topic `<topic>-value` subjects added there on 2026-07-12 — those exist only so kafka-ui
  auto-defaults to AVRO serde per topic; the Flink job itself always reads/writes via the one
  fixed logical subject per event type, same content, same global schema ID either way.
- **`AvroSchemaLoader.load(classpathResource)` kept, but test-only now.** Production code no
  longer calls it. `pom.xml`'s `copy-avro-schemas` `maven-resources-plugin` execution was
  retargeted from `generate-resources`/`${project.build.outputDirectory}/avro` to
  `generate-test-resources`/`${project.build.testOutputDirectory}/avro` — the canonical
  `schemas/*.avsc` files now land only on the **test** classpath (so unit tests can build fixture
  `GenericRecord`s without a live registry), never in `target/classes` or the shaded jar. Verified
  via `jar tf target/orderbook-consolidator-1.0-SNAPSHOT.jar | grep avro/` → only
  `AvroSchemaLoader.class`, no `.avsc` files. 25/25 tests still green (`mvn -o test`).
- **Consequence:** the Flink job now has a hard runtime dependency on the Schema Registry being
  reachable and already having both subjects registered *before* the job starts consuming/producing
  (true today since `warmup.sh` runs before the job in the deploy flow) — if the registry is down
  or the subject is missing, the first `deserialize`/`serialize` call throws
  `IllegalStateException` (lazy-init failure), which is the correct fail-fast behavior for "schema
  must come from the registry and nowhere else," not a regression.
- `warmup.sh` was **not** changed for this rule — it's the schema *deployment* tool (pushes the
  canonical `.avsc` files into the registry in the first place); it doesn't validate/encode/decode
  business events itself, so the rule doesn't apply to it. It remains the only place the
  repo-root `schemas/*.avsc` files feed into the registry for this module.
