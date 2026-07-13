---
name: kafka-topic-strategy
description: Technical decision on Kafka topic naming, key, and partitioning strategy for the NiFi → Kafka → Flink order book pipeline
metadata:
  type: project
---

## Decision: Topic per side+pair+exchange (ID-based names)

**Input topic** → `ex{exchange_id}-p{pair_id}-{side}` (e.g. `ex1-p2-asks`, `ex1-p2-bids`)
**Output topic** → `p{pair_id}-{side}` (e.g. `p2-asks`) — Flink writes the consolidated book here
**Key** → null (one exchange per input topic guarantees ordering)
**Body** → see [[avro-schema-orderbook]] (base, quote, pair_id, exchange_id, exchange_name, side, type, event_time, levels)

`pair_id` = `markets.id`, `exchange_id` = `exchanges.id`. Topic names use the **DB integer IDs**, not the human-readable base/quote/exchange_name. Exchange comes first on input topics.

## Rationale

- One exchange publishes to one input topic — ordering is guaranteed with a single partition, no key needed
- Partition count is 1 per topic; no skew, no idle partitions, no repartitioning when exchanges are added or removed
- Flink aggregates across exchanges via regex on input topics: `ex.*-p{pair_id}-{side}` (e.g. `ex.*-p2-asks`) picks up all exchanges for that side+pair automatically — new exchanges need no Flink config change
- Output topic `p{pair_id}-{side}` does not collide with the input naming shape (`ex{exchange_id}-p{pair_id}-{side}` always starts with `ex`), so the job won't re-consume its own output (no feedback loop)
- IDs keep topic names compact and decoupled from display strings (avoids casing/charset issues). `exchanges.name` is unique and immutable (README RULE) so names would also be stable, but IDs are shorter
- Topic count at scale: 10 exchanges × 200 pairs × 2 sides = 4000 topics — fine for modern Kafka (KRaft)

## Migration note (changed 2026-06-28)

Topic naming was changed from human-readable to ID-based. **Migration is COMPLETE end to end:** `scripts/warmup.sh`, `schemas/orderbook_event.avsc`, the Flink job, and the web app are all on the ID-based scheme.

Old scheme: input `{base}-{quote}-{side}-{exchange_name}` (e.g. `BTC-USDT-asks-nobitex`), output `{base}-{quote}-{side}` (e.g. `BTC-USDT-asks`).

Flink job migration (done): Flink works **only with `pair_id` and `exchange_id`** — `base`, `quote`, `exchange_name` were removed from all Flink models. `OrderBookEvent` keeps `exchange_id`, `pair_id`, `side`, `type`, `event_time`, `levels` and is annotated `@JsonIgnoreProperties(ignoreUnknown=true)` so it tolerates the descriptive fields the wire JSON still carries (the avsc schema is unchanged). `PairsLoader` selects only `m.id` into `Pair(id)`. `OrderBookSourceFactory` subscribes by regex `{side}-p{pairId}-ex.*`. `OrderBookJob` keys by `pairId` and names operators + sink topic `{side}-p{pair_id}`. `OrderBookMerger` is `KeyedProcessFunction<Integer,…>` with `MapState<Integer,…>` keyed by `exchange_id`. `ConsolidatedOrderBook` = `{ pair_id, side, levels, event_time }`; `ConsolidatedLevel` = `{ exchange_id, price, quantity }`.

Web app migration (done): rewritten in Go (replacing Node `server.js`), subscribes by regex `^(asks|bids)-p\d+$` (output topics), and resolves `pair_id`/`exchange_id` → display labels from postgres since the output carries no base/quote/exchange_name. See [[orderbook-web]].

## Naming scheme change (2026-07-12): segment order flipped

**Input topic**: `{side}-p{pair_id}-ex{exchange_id}` → **`ex{exchange_id}-p{pair_id}-{side}`** (e.g. `asks-p2-ex1` → `ex1-p2-asks`).
**Output topic**: `{side}-p{pair_id}` → **`p{pair_id}-{side}`** (e.g. `asks-p2` → `p2-asks`).

Same identifiers, same separators, only the ordering of segments changed (exchange-first on input, side-last everywhere; output drops the exchange segment as before). Updated in this pass: this doc, `scripts/warmup.sh` (`create_topic` calls + retention comments), `flink/orderbook-consolidator` (`PriceLevelSourceFactory`'s input-topic regex, `ConsolidatedOrderBookSinkFactory`'s topic-selector, doc comments), `docker-compose-orderbook-consolidator.yml`'s kafka-ui `TOPICVALUESPATTERN` serde bindings, and (2026-07-13, follow-up pass) `web/` — `internal/kafka/consumer.go`'s subscribe regex `^(asks|bids)-p\d+$` → `^p[0-9]+-(asks|bids)$`, plus matching example topic strings in `internal/hub/hub_test.go`/`internal/ingest/ingest_test.go` (`asks-p1` → `p1-asks`, illustrative only — HandleRecord/Hub.Publish treat topic as an opaque string, not parsed) and `web/README.md`'s regex/example docs. `go build ./... && go test ./...` all green after the web change.

**Still not updated (deliberately, out of scope for both passes):** `flink/orderbook-job/` (the old JSON-pipeline job, superseded by `orderbook-consolidator` — see [[orderbook-consolidator-decision]]) still uses the old scheme in `OrderBookSourceFactory`/`OrderBookJob`. `fake-data-generator/mian.go` (dev stand-in for NiFi, `topicName()`/`emit()`) still builds `{side}-p{pair_id}-ex{exchange_id}` names — it will publish to topics the new consolidator regex no longer matches until updated. NiFi's producer side (owned by a separate team, not in this repo) is also unverified against the new input-topic names — same caveat as already flagged in [[orderbook-consolidator-decision]] for the Avro migration.

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

**Performance (2026-07-11, later reverted same day):** `create_topic()` was slow because each call does `docker exec ... kafka-topics --create`, and `kafka-topics` is a JVM CLI tool — every invocation pays JVM startup cost (~1-3s) on top of the broker round-trip. Tried fanning the `docker exec` calls out via `xargs -P` to start many JVMs concurrently, but this was **reverted** (commit `81c18de`, "revert back parallel topic creation") — reason for the revert wasn't captured at the time. Current script is back to the original sequential `while` loop, one `create_topic` call at a time per topic.

**Per-topic schema binding — tried 2026-07-12, reverted same day.** Briefly added `register_schema "${topic}-value" "AVRO" "$SCHEMA_FILE"` after every `create_topic` call (one registry subject per topic, Confluent `TopicNameStrategy`) so kafka-ui would auto-default its Produce-Message serde to AVRO per topic. **Reverted at user's request:** with dozens/hundreds of topics (pairs × exchanges × 2 sides) this created one duplicate subject per topic in the registry on top of the 3 canonical ones (`orderbook-event`, `price-level-event`, `consolidated-order-book-event`) — cluttering the Schema Registry / kafka-ui "Schemas" view with lookalike entries, even though they all resolved to the same underlying schema ID by content. **Current stance: only the 3 canonical fixed-name subjects exist**, `warmup.sh` back to its Step-9 state.

**kafka-ui AVRO-per-topic without registry clutter — solved 2026-07-12 via custom serde config in `docker-compose-orderbook-consolidator.yml`'s `kafka-ui` service** (not warmup.sh — this is pure kafka-ui config, registry is untouched, still exactly 3 subjects). First attempt (manually picking a serde in kafka-ui's produce screen) turned out to be **impossible**, not just inconvenient: `GET /api/clusters/local/topic/{topic}/serdes` showed `valueSerde: null` for these topics — kafka-ui's produce dropdown only lists serdes whose own applicability check passes for that specific topic, so with no matching subject there was nothing to pick, confirmed via the running stack before concluding this. Fix: two named custom serde instances of the built-in `SchemaRegistrySerde` class, each bound via `topicValuesPattern` to one topic shape and pinned via `properties.schemaNameTemplate` (no `%s`) to one fixed canonical subject:

```
KAFKA_CLUSTERS_0_SERDE_0_NAME: PriceLevelEventAvro
KAFKA_CLUSTERS_0_SERDE_0_CLASSNAME: com.provectus.kafka.ui.serdes.builtin.sr.SchemaRegistrySerde
KAFKA_CLUSTERS_0_SERDE_0_TOPICVALUESPATTERN: ^ex[0-9]+-p[0-9]+-(asks|bids)$
KAFKA_CLUSTERS_0_SERDE_0_PROPERTIES_URL: http://schema-registry:8082
KAFKA_CLUSTERS_0_SERDE_0_PROPERTIES_SCHEMANAMETEMPLATE: price-level-event
KAFKA_CLUSTERS_0_SERDE_1_NAME: ConsolidatedOrderBookEventAvro
KAFKA_CLUSTERS_0_SERDE_1_CLASSNAME: com.provectus.kafka.ui.serdes.builtin.sr.SchemaRegistrySerde
KAFKA_CLUSTERS_0_SERDE_1_TOPICVALUESPATTERN: ^p[0-9]+-(asks|bids)$
KAFKA_CLUSTERS_0_SERDE_1_PROPERTIES_URL: http://schema-registry:8082
KAFKA_CLUSTERS_0_SERDE_1_PROPERTIES_SCHEMANAMETEMPLATE: consolidated-order-book-event
```

(`TOPICVALUESPATTERN` values above updated same day for the segment-order rename described earlier in this doc — originally `^(asks|bids)-p[0-9]+-ex[0-9]+$` / `^(asks|bids)-p[0-9]+$`.)

Two gotchas that cost debugging time, worth remembering:
1. **`name: SchemaRegistry` is reserved** for the single cluster-auto-configured instance — a second serde entry reusing that name crashes kafka-ui at startup (`ValidationException: Multiple serdes with same name`). Each extra instance needs a unique `name` + explicit `className`.
2. **`schemaNameTemplate`/`url`/etc. must live under `properties.`** (`KAFKA_CLUSTERS_0_SERDE_n_PROPERTIES_SCHEMANAMETEMPLATE`), not as a top-level serde key — only `name`/`className`/`topicKeysPattern`/`topicValuesPattern` are top-level. Got this wrong on the first pass (put `schemaNameTemplate` top-level) — it silently no-opped (defaulted to `%s-value`, i.e. `TopicNameStrategy`, no error) rather than failing loudly, so it looked configured but wasn't. Confirmed the correct shape against kafka-ui's own `documentation/compose/kafka-ui-serdes.yaml` example in its GitHub repo before trusting it.

Verified live end-to-end against the running stack (not just config-shape reasoning): `GET /api/clusters/local/topic/asks-p1-ex1/serdes?use=SERIALIZE` → `PriceLevelEventAvro` listed `preferred: true`; produced a real message via kafka-ui's `POST .../messages` API with `valueSerde: PriceLevelEventAvro`; consumed the raw bytes off Kafka and hand-decoded the Confluent wire format (magic byte `0x00` + schema id `2` + Avro payload) — schema id 2 resolves to the `price-level-event` subject's `PriceLevelEvent` schema, and all 6 field values round-tripped correctly. Test message deleted afterward (topic delete+recreate). Registry subject count confirmed unchanged (still exactly 3) after all of this.

kafka-ui API note for future debugging: the per-topic serde-listing endpoint is `/api/clusters/{cluster}/topic/{topic}/serdes` — **singular** `topic`, not `topics` (the plural form 404s silently through the SPA static-resource fallback, easy to misdiagnose as "endpoint doesn't exist").

Its query selects only `m.id, em.exchange_id, e.name` (topic names are ID-based, so base/quote symbols are not needed) — updated 2026-06-29 when [[db-schema]] dropped `markets.base`/`quote`; the old query `SELECT m.id, m.base, m.quote, …` now errors. NOTE: all `exchange_markets` rows are seeded `unsubscribe`, so warmup creates zero topics until some are flipped to `subscribe`.

**Why:** NiFi → Kafka → Flink pipeline for collecting and normalizing exchange order book data (asks + bids) across up to 200 trading pairs.
**How to apply:** Use this structure for all Kafka topic definitions, NiFi routing logic, and Flink source configurations in this project.
