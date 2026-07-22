---
name: kafka-topic-strategy
description: Technical decision on Kafka topic naming, key, and partitioning strategy for the NiFi → Kafka → Flink order book pipeline
metadata:
    type: project
---

## Decision: Topic per side+pair+exchange (ID-based names)

**Input topic** → `ex{exchange_id}-p{pair_id}-{side}` (e.g. `ex1-p2-asks`, `ex1-p2-bids`)
**Output topic** → `p{pair_id}-{side}` (e.g. `p2-asks`) — Flink writes the aggregated book here
**Key** → null (one exchange per input topic guarantees ordering)
**Body** → see [[avro-schema-orderbook]]

`pair_id` = `markets.id`, `exchange_id` = `exchanges.id`. Topic names use the **DB integer IDs**, not the human-readable base/quote/exchange_name. Exchange comes first on input topics.

## Rationale

- One exchange publishes to one input topic — ordering is guaranteed with a single partition, no key needed
- Partition count is 1 per topic; no skew, no idle partitions, no repartitioning when exchanges are added or removed
- Flink aggregates across exchanges via regex on the pipeline topics: the aggregator subscribes `ex[0-9]+-p[0-9]+-orderbook-snapshot-flink` — new exchanges/pairs need no Flink config change
- Output topic `p{pair_id}-{side}` does not collide with the input naming shape (input always starts with `ex`), so the job won't re-consume its own output (no feedback loop)
- IDs keep topic names compact and decoupled from display strings (avoids casing/charset issues). `exchanges.name` is unique and immutable (README RULE) so names would also be stable, but IDs are shorter
- Topic count at scale: 10 exchanges × 200 pairs × 2 sides = 4000 topics — fine for modern Kafka (KRaft)

## Segment order flipped 2026-07-12 — two components deliberately NOT migrated

The scheme used to be `{side}-p{pair_id}-ex{exchange_id}` / `{side}-p{pair_id}` (side-first).
Everything current is on the new order — `scripts/warmup.sh`, `flink/normalizer` (the aggregator's
source regex + sink topic selector), the kafka-ui serde bindings in `docker-compose.yml`, and `web/`
(`internal/kafka/consumer.go` regex `^p[0-9]+-(asks|bids)$`).

- NiFi's producer side (owned by a separate team, not in this repo) publishes verbatim raw payloads
  to `ex{id}-raw` ([[raw-pipeline-decision]]); its exact per-exchange formats are the risk tracked
  in the pair-extractor.

## What was rejected and why

| Option                                    | Rejected because                                                                                   |
| ----------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `asks` / `bids` as topics, pair as key    | Flink streams not separated by pair                                                                |
| `{pair}` as topic, side as key            | Flink still needs `filter()` to split sides; only 2 effective partitions                           |
| `{pair}-{side}` as topic, exchange as key | Partition count doesn't align with exchange count per pair; varies per topic and changes over time |

## Out of scope (deferred)

Processing all pairs for one exchange (e.g. everything from `nobitex`) is not a current requirement. If needed later, a separate stream can be defined for that use case.

## Operational note

Topics are pre-provisioned by `scripts/warmup.sh` from the postgres `markets` + `exchange_markets` + `exchanges` tables (rows where `exchange_markets.status = 'subscribe'`) — not auto-created. warmup.sh first registers the Avro schemas, then creates input + output topics (single partition, replication-factor 1). Its query selects only `m.id, em.exchange_id, e.name` (topic names are ID-based, so base/quote symbols are not needed — [[db-schema]]). NOTE: all `exchange_markets` rows are seeded `unsubscribe`, so warmup creates zero topics until some are flipped to `subscribe`.

**Retention:** input topics `ex{exchange_id}-p{pair_id}-{side}` get `retention.ms=3600000` (1 hour); output topics `p{pair_id}-{side}` get `retention.ms=21600000` (6 hours), passed via `--config retention.ms=...` in `create_topic()`. Input topics are raw per-exchange firehose (short-lived, high volume); output topics are the aggregated book consumers care about (longer window for replay/late web-UI connects). **Caveat:** topic creation uses `--if-not-exists`, so retention is only applied when a topic is first created — topics already provisioned on the server before this change (2026-07-11) keep their old (unset/broker-default) retention and need `kafka-configs --alter --entity-type topics --entity-name <topic> --add-config retention.ms=...` to update in place; not yet run against the deployed server.

**Tried and reverted — don't redo:**

- **Parallel topic creation** (`xargs -P` around the per-topic `docker exec ... kafka-topics --create`, each paying ~1-3s JVM startup): reverted same day, commit `81c18de`; script is back to the sequential `while` loop. Reason for the revert wasn't captured at the time.
- **Per-topic `<topic>-value` schema subjects** (Confluent `TopicNameStrategy`, one registry subject per topic so kafka-ui would auto-default to AVRO): reverted same day at user's request — with pairs × exchanges × 2 sides it cluttered the registry with dozens of duplicate subjects on top of the canonical ones. **Current stance: only the canonical fixed-name subjects exist** (`raw-order-book-event`, `order-book-snapshot`, `rejected-order-book-event`, `aggregated-order-book-event`). The kafka-ui goal was solved via serde config instead (below).

## Normalizer intermediate topics (2026-07-19)

The raw pipeline's stage topics are `ex{exchange_id}-p{pair_id}-{stage}-flink`, one family per job
output: `raw` (job 1), `type-validated-raw` (2), `rebased` (3), `applied-precision` (4),
`orderbook-snapshot` (5), plus `rejected-flink` — a _shared_ dead-letter written by both jobs 2 and 3. The terminal aggregator (job 6) has no `-flink` family of its own: it writes the frozen web-output
topics `p{id}-{side}` (no `ex` prefix, no `-flink` suffix). The `-flink` suffix marks "intermediate,
ours to delete"; its absence on the aggregator's output is deliberate — that is the web contract.

**warmup.sh creates these BEFORE the existing input/output blocks, at the user's request.** Reason:
every normalizer source uses `OffsetsInitializer.latest()`. A topic that doesn't exist when its job
starts gets discovered by the source's periodic partition-discovery only _later_, and everything
produced in the gap is silently lost. Provisioning up front removes the race. This is the same
constraint that makes `make refresh-normalizer` submit jobs downstream-first.

**Retention:** intermediates get the 1h `INPUT_RETENTION_MS` (transient, high volume, fully
reproducible by replaying `ex{id}-raw`). `rejected-flink` gets 7 days — it is an audit point read by
hand via kafka-ui, often long after the rejection, so 1h would make it useless. Same
`--if-not-exists` caveat as above: retention only lands at first creation.

## kafka-ui AVRO-per-topic without registry clutter (serde config)

Solved via custom serde config in `docker-compose.yml`'s `kafka-ui` service (pure kafka-ui config, registry untouched). Manually picking a serde in kafka-ui's produce screen is **impossible** without this: the dropdown only lists serdes whose applicability check passes for that topic, and with no matching subject there's nothing to pick (`valueSerde: null`). Fix: a named custom serde instance of the built-in `SchemaRegistrySerde` class, bound via `topicValuesPattern` to the output topic shape and pinned via `properties.schemaNameTemplate` (no `%s`) to the fixed canonical subject:

```
KAFKA_CLUSTERS_0_SERDE_0_NAME: AggregatedOrderBookEventAvro
KAFKA_CLUSTERS_0_SERDE_0_CLASSNAME: com.provectus.kafka.ui.serdes.builtin.sr.SchemaRegistrySerde
KAFKA_CLUSTERS_0_SERDE_0_TOPICVALUESPATTERN: ^p[0-9]+-(asks|bids)$
KAFKA_CLUSTERS_0_SERDE_0_PROPERTIES_URL: http://schema-registry:8082
KAFKA_CLUSTERS_0_SERDE_0_PROPERTIES_SCHEMANAMETEMPLATE: aggregated-order-book-event
```

Two gotchas that cost debugging time, worth remembering:

1. **`name: SchemaRegistry` is reserved** for the single cluster-auto-configured instance — a second serde entry reusing that name crashes kafka-ui at startup (`ValidationException: Multiple serdes with same name`). Each extra instance needs a unique `name` + explicit `className`.
2. **`schemaNameTemplate`/`url`/etc. must live under `properties.`** (`KAFKA_CLUSTERS_0_SERDE_n_PROPERTIES_SCHEMANAMETEMPLATE`), not as a top-level serde key — only `name`/`className`/`topicKeysPattern`/`topicValuesPattern` are top-level. Getting this wrong silently no-ops (defaults to `%s-value`, i.e. `TopicNameStrategy`, no error) rather than failing loudly. Correct shape confirmed against kafka-ui's own `documentation/compose/kafka-ui-serdes.yaml` example.

Verified live end-to-end (produced a real message via the serde, hand-decoded the Confluent wire bytes off Kafka, registry still at only the canonical subjects). kafka-ui API note for future debugging: the per-topic serde-listing endpoint is `/api/clusters/{cluster}/topic/{topic}/serdes` — **singular** `topic` (the plural form 404s silently through the SPA static-resource fallback).

**Why:** NiFi → Kafka → Flink pipeline for collecting and normalizing exchange order book data (asks + bids) across up to 200 trading pairs.
**How to apply:** Use this structure for all Kafka topic definitions, NiFi routing logic, and Flink source configurations in this project.
