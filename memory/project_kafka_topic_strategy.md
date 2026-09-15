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
reproducible by replaying `ex{id}-raw`). `rejected-flink` gets 2 days (lowered from 7 days on
2026-08-24) — it is an audit point read by hand via kafka-ui, often long after the rejection, so 1h
would make it useless. Same `--if-not-exists` caveat as above: retention only lands at first
creation.

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

## Emptying topics without destroying the stack — `scripts/purge-topics.sh` (2026-08-19)

The counterpart to `warmup.sh`: warmup CREATES the topics, purge EMPTIES them. Written because the
only reset that existed was `make refresh-normalizer`, whose `docker compose down -v` takes the
registry, the postgres volume and the Kafka data with it — far more than "clear the topics".

Uses **`kafka-delete-records`**, which moves each partition's low watermark up to its high
watermark. That is an immediate, real deletion — no `retention.ms=1` trick, no topic recreate, no
broker restart, and partition count / retention config / registry subjects all survive.

- **It matches the LIVE topic list against a regex, it does NOT re-derive from postgres** like
  warmup does. Deliberate: that way it also catches topics for markets that have since been
  unsubscribed, which are exactly the ones sitting on stale data nothing will ever clean up.

Three bugs the first version shipped with, all worth not repeating (2026-08-19):

- **One `docker exec` per topic is unusable.** Each starts a JVM in the container, so on a full
  market list the script sat silent for minutes and read as a hang — which is how the user found
  it. Every broker query is now ONE bulk call (`kafka-get-offsets --topic-partitions '.*'`)
  filtered locally, and every command is echoed with elapsed time so silence is never ambiguous.
- **Logging to stdout from a function whose stdout is captured corrupts the data.** `kafka_run`
  returns broker output via `$(...)`; its progress lines were being counted as topics (a 12-topic
  broker reported 14). All diagnostics now go to stderr. This bites any `run()`-style helper.
- **`kafka-delete-records` raises the START offset, it does NOT move the end offset.** A purged
  topic reports the same large latest offset forever, so counting `latest` counts records deleted
  long ago — the script claimed 40 736 records to delete on an already-empty broker. Readable
  records are `latest - earliest`; the *delete* offset is still `latest`. Same trap applies to any
  "how much is in this topic" check anywhere else.
- `NORMALIZER_STAGES` is duplicated here too — a **third** copy after `warmup.sh` and the
  exporter's ([[staleness-exporter]]). A stage missing from this array is silently NOT purged.
- **⚠ Purging Kafka does NOT reset Flink.** The jobs keep their keyed state — job 2's `lastSeq` /
  `awaitingSnapshot` ([[type-validator]]), job 5's `MapState` books — so after a purge the
  pipeline still believes everything it saw before, and a "clean" run is anything but. Resubmit
  with `make run-normalizer-jobs` for a true reset. The script prints this on exit.
- `--dry-run` reports the plan and record counts; the interactive prompt is skipped with `--yes`.

## 2026-09-12 — warmup is a Go project now (`warmup/`), configured by `.env`

`scripts/warmup.sh` took the user **about an hour** at 9 exchanges × 50 markets, and is now
**DELETED** (user, same day — "i do not need it any more"). The cause was never Kafka: ~3010 topics
(1 control + 450×6 stage/rejected + 9 raw + 50×6 output) × one `docker exec` + a fresh JVM for
`kafka-topics` ≈ 1.2 s each. **This is the exact lesson `purge-topics.sh` already learned on
2026-08-19 — it was never carried back to warmup.** Rewritten in Go as `warmup/`:

- **Top-level project, not a script.** It first landed under `scripts/warmup/` as a flat package;
  the user rejected that ("why project structure is like high school project… use a better project
  structure like web service"), so it moved to `warmup/` alongside the other Go projects and now
  mirrors `web/`: `main.go` at the module root, `internal/{config,domain,postgres,topics,kafka,
  registry}`, its own `Makefile` (help/test/vet/fmt/check/build/run) and `README.md`, and the same
  `godotenv` + `testify` the web service uses. **When adding a Go project here, copy `web/`'s shape
  — that is the house style, and a flat package will be sent back.**
- Own module with a committed `vendor/` because the dev server cannot reach proxy.golang.org.
- **Batching is the whole point.** One `ListTopics`, then `DescribeTopicConfigs`/`CreateTopics`/
  `AlterTopicConfigs` in batches of `TOPIC_BATCH_SIZE` (500) grouped by retention value — a handful
  of requests instead of 3000 process spawns.
- **✅ VERIFIED LIVE 2026-09-12** on the laptop stack (352 `exchange_markets` rows, 9 exchanges,
  54 markets → 2446 topics). Numbers, for the next person who wonders whether it is worth it:
  **full reconcile of 2446 existing topics = 295 ms**; creating 6 new topics = 275 ms total;
  **retuning 1765 topics = 374 ms total** (the alter itself ~140 ms, 4 batched requests). The shell
  version's equivalent was ~1 hour. Three paths were each checked against the broker, not just the
  log line: create (a temporary `exchange_markets` row → the 6 expected topics, 1 partition, RF 1,
  `retention.ms=3600000`, row and topics removed afterwards), alter (`RETENTION_INPUT_MS=7200000`
  → `kafka-configs --describe` shows 7200000 on a stage topic **and 3600000 still on `ex1-raw`**,
  which is what proves the per-family grouping does not spray one value over everything, then
  restored), and idempotence (a second run reports `to_create=0 to_retune=0` and does nothing).
  Schema registration returned the EXISTING ids (1, 2, 4, 9, 11, 12, 13) — re-registering an
  identical schema is idempotent in the registry and does NOT add a version.
- **Runs from the HOST, no `docker exec` at all** — both compose files publish `9092` and `5432`,
  and `pgx` replaces `docker exec postgres psql`. It needs no docker CLI and no container names.
- **Retention is RECONCILED, not just set at creation.** This is the second half of the user's ask.
  `--create --if-not-exists` left an existing topic on whatever retention it was born with forever,
  so changing a retention used to mean deleting topics. The tool now compares the broker's
  `retention.ms` against `.env` and issues IncrementalAlterConfigs for the difference.
  ⚠ Lowering a retention deletes whatever is already past the new limit.
  ⚠ A topic whose `retention.ms` the broker does not report is deliberately left alone, otherwise
  every run would "change" it.
- **⚠ ALL FIVE retentions are now 1 HOUR** (user, 2026-09-12: "change .env.example default values to
  1 houre"), in `warmup/.env.example` **and** in the Go fallback defaults so the two can never
  disagree. This REPLACES the values recorded above in this file: raw 2 d, rejected 2 d and output
  6 h are gone. Note the knock-on — `web/`'s aggregated consumer reads `AtEnd()` since 2026-09-06,
  so the 6 h→1 h output cut costs nothing there, but anything that expected 2 days of `ex{id}-raw`
  for replay now has one hour.
- `warmup/.env` is created from `.env.example` by `make warmup` on the first run (a Make file target
  with no prerequisites, so an existing `.env` is never overwritten) and is gitignored. A real
  environment variable beats the file: `RETENTION_INPUT_MS=600000 make warmup`. Retentions are
  parsed as integers **before** anything is created, so a typo like `1h` fails immediately.
- **Schemas: the whole `schemas/*.avsc` directory is registered** (subject = file name, `_` → `-`),
  the same rule the e2e harness uses. That kills the trap recorded in [[avro-schema-orderbook]] and
  [[control-plane]] — a missing `register_schema` line was invisible to a green e2e run and hid
  `control-command` for two days. All 7 current files map exactly onto the 7 subjects; **a future
  `.avsc` whose subject is not its file name would silently be registered under the wrong subject.**
- ⚠ **`exchange_markets` is UNIQUE on `(exchange_id, market)` — the exchange's own symbol string —
  NOT on `(exchange_id, market_id)`.** Two rows can therefore name the same exchange+pair, so the
  plan must de-duplicate: a name appearing twice inside one `CreateTopics` batch is rejected by the
  broker. The shell version never hit this because it created one topic at a time with
  `--if-not-exists`. A mutation test pins it.
- The subscription query still does **not** filter `exchange_markets.status`, so topics are created
  for unsubscribed rows too. Kept identical to the shell version on purpose — flagged, not fixed.
- With `warmup.sh` gone, the retention constants and `NORMALIZER_STAGES` survive in three more
  places — `purge-topics.sh`, `e2e/topics/topics.go` and [[staleness-exporter]] — and `.env` is the
  source of truth only for warmup. Nothing enforces the rest.
- `scripts/diagnose-stuck-markets.sh` was deleted the same day on the same instruction; it had never
  been run.
- **Nothing else in the repo depended on the shell script** (checked 2026-09-12): `e2e` provisions
  through its OWN `e2e/topics` + `schemaregistry` packages and only ever shells out to
  `docker compose`, the exporter derives names itself, and the Flink jobs read `latest`. What the
  deletion DID leave behind was dangling comments in `web/internal/kafka/consumer.go`,
  `web/README.md`, `e2e/topics/topics.go` and `lpa-staleness-exporter/exporter.py` — one of them
  ("warmup.sh keeps 6 hours on these topics") now factually wrong. All repointed.
- ⚠ **e2e and warmup now set DIFFERENT retentions on the same topic names.** `e2e/topics/topics.go`
  still hardcodes 1h/2d/6h/2d/1h and deletes+recreates its topics per scenario, so a scenario runs
  against its own values and a later `make warmup` retunes them to 1h. Harmless for test outcomes,
  but the two are no longer the same numbers — the file says so now.

**Why:** NiFi → Kafka → Flink pipeline for collecting and normalizing exchange order book data (asks + bids) across up to 200 trading pairs.
**How to apply:** Use this structure for all Kafka topic definitions, NiFi routing logic, and Flink source configurations in this project.

## 2026-09-14 — `scripts/watch-topic.sh <topic>` (user request)

Live tail of partition 0 through `kafka-console-consumer` inside the `kafka` container, printing partition, offset and
CreateTime with the value hidden (Avro). Written because Kafka UI's live mode looked like it showed offsets out of order.
That is not possible within one partition, and every warmup topic has 1 partition. CreateTime being non-monotonic is
expected, since Flink inherits the input timestamp (**before 2026-09-14; the broker is now `LogAppendTime`, see § 2026-09-14 broker-wide `LogAppendTime`**). Bootstrap `kafka:29092` / `KAFKA_CONTAINER` env overrides match
`purge-topics.sh`. Hard-codes partition 0 because topics are single-partition. Only the syntax and usage path were checked; it was
NOT run against a broker.
Same day it gained a human-readable prefix (`YYYY-MM-DD HH:MM:SS.mmm +zzzz`). The conversion happens in the HOST shell, so
the time is in the server's timezone rather than the container's (usually UTC). It uses bash `printf '%(...)T'` (bash ≥ 4.2,
fine on Debian 12, NOT macOS's /bin/bash 3.2) so there is no `date` fork per record at hundreds of rec/s. `-t` is now keyed
on stdin (`-t 0`), because stdout is the pipe into the formatter, and `\r` is stripped from TTY lines. Lines without a numeric
CreateTime pass through. Verified with a stub `docker` feeding sample lines (incl. `\r\n` and `NO_TIMESTAMP`), not against a broker.
Also same day: it STOPS on an out-of-order offset (`n <= previous`, duplicates included), exits **2**, and kills the consumer. A
forward jump is deliberately NOT an error, because gaps are legal in Kafka (transaction markers, deleted/compacted records).
The consumer runs on fd 3 via `exec 3< <(docker exec ...)`, not in a pipeline, so `$!` is its PID (it gets killed on
violation instead of lingering until its next write), and `wait "$consumer"` passes on its exit status when it ends by
itself. That needs bash ≥ 4.4. Verified with stub `docker`s: a duplicate offset while the consumer is still alive → exit 2
in <1 s with no leftover process; consumer exits 1 → exit 1; a jump 5→9 → no stop.
**Revised same day after the first live run: the output staircased.** `docker exec -t` puts the local terminal in raw mode (a
newline without a carriage return) and the remote TTY echoed `^C` into the data, so `-t` is GONE. The trap that replaces it,
**verified with a throwaway alpine container**: without a TTY, killing the docker client does NOT stop the process inside the
container, even after it writes again. So the consumer is tied to stdin EOF: `sh -c` runs it in the background plus a
`(cat <&3; kill $c)` watcher, and **`exec 3<&0` + `<&3` is load-bearing**, because a non-interactive `sh` points a background
job's stdin at /dev/null (without it the consumer was killed instantly). The script runs docker as a `coproc`, so it holds
the stdin pipe and it closes on ANY exit. Output is now `... partition=N  offset=N  create_ms=N`. Verified against real
`docker exec` with a stub consumer: out-of-order → exit 2, consumer exits 3 → exit 3, real Ctrl-C through a pty → clean lines
and 0 consumers left in the container in all three cases. Still NOT run against the real broker since the fix.

## 2026-09-14 — broker-wide `LogAppendTime` (user decision, NOT deployed)

**Problem:** under the default `CreateTime`, every Flink output topic carried the input record's timestamp, copied
from job 1's input all the way to `-adjusted`. The cause: `KafkaSource` puts the input record's timestamp on the
Flink record, and `KafkaSink` passes it to the `ProducerRecord`. `KafkaRecordSerializationSchemaBuilder` (5.0.0-2.2)
has no timestamp setter, which was checked in the jar.

**Rejected, by the user:** wrapping each sink's record serializer in Java to stamp `currentTimeMillis()`. It was
written, tested, and then fully reverted: *"let Kafka do it, not us in Java"*. **Don't re-propose it.**

**Chosen:** `KAFKA_LOG_MESSAGE_TIMESTAMP_TYPE: LogAppendTime` on the broker in BOTH `docker-compose.yml` and
`docker-compose.prod.yml`. The broker overwrites the producer's timestamp with its append time, so every hop gets a
fresh one with no code change. warmup sets only `retention.ms` per topic, so this broker default reaches EVERY topic,
including NiFi's `ex{id}-raw`, `control-plane`, e2e topics, and internal topics.

- **The payload `event_time` is NOT touched.** The user said explicitly that it stays as it is.
- Kafka has no timestamp HEADER. `timestamp` plus `timestampType` are built-in record fields, and headers are user k/v.
- Only a broker restart (recreating the container) applies it. A topic-level `message.timestamp.type` override would
  beat it, and nothing sets one today.
- No code reads record timestamps except [[staleness-exporter]] (`records[-1].timestamp`). It now measures when the
  broker last appended to a topic, which is NOT how far behind a job is: a job writing steadily but minutes behind looks fresh.
- Time-based retention now runs on append time, which is more accurate than inherited old CreateTimes.
- Not verified that `LogAppendTime` on `__consumer_offsets`/`__transaction_state` is harmless. Kafka reads offset
  commit/expiry times from the record VALUE, so it is expected to be fine, but that is unconfirmed.
- `orderbook-viewer/internal/e2e/docker-compose.e2e.yml` was left on CreateTime; nothing there reads timestamps.
- **NOT run against a broker.** Only `docker compose config` was checked. Verify after deploy:
  `kafka-configs --bootstrap-server kafka:29092 --entity-type brokers --entity-name 1 --describe --all | grep log.message.timestamp.type`,
  then confirm that `scripts/watch-topic.sh p1-asks-adjusted` shows a `create_ms` close to wall clock
  (the console consumer prints the label `LogAppendTime:` instead of `CreateTime:`).
- **`watch-topic.sh` fixed the same day (user request):** it used to strip only `CreateTime:`, so under `LogAppendTime`
  the readable date was lost and the raw line passed through (the offset check did NOT break, it is independent).
  It now strips `*Time:`, so both labels work. Verified with a stub `docker` (both labels, `NO_TIMESTAMP` passthrough, duplicate offset → exit 2)
  under Homebrew bash, not against a broker. The output label is still `create_ms` even when the value is append time.

## 2026-09-15 — `latency-monitor/` replaces `watch-topic.sh` (user request)

A Go module at the repo root (`orderbook-latency`), same shape as `warmup/`: `main.go` +
`internal/` + vendored deps + its own Makefile. `make watch TOPIC=p1-asks` from the root, or
`go run . <topic>` from the module. `--strict-order` exits **2** on a non-increasing offset,
deliberately the same code the shell script used so anything wrapping it still works; without the
flag it warns and keeps tailing (the script always died).

**Why the rewrite was unavoidable, not a preference:** the timings live INSIDE the Avro value.
`watch-topic.sh` could reach the record timestamp and the offset because the console consumer
prints those as text, but decoding Confluent-wire-format Avro in bash was never going to happen —
and the one attempt at reading `event_time` out of the printed JSON ran into bash reading a pipe one
byte at a time, which cannot keep up with a 30 KB book at ~600 rec/s.

Stack matches [[orderbook-viewer]]: `franz-go` (tail, no consumer group — an observer must not split
records with another watcher), `hamba/avro` (decode), schema resolved by wire-header id and cached
forever.

Three decisions worth keeping:
- **ONE `event.Record` struct covers every record shape.** hamba drives decoding from the WRITER
  schema, so a field the schema lacks is left zero rather than failing. That is what lets the same
  binary read a job-1 event, a job-5 snapshot and a `p{id}-{side}` record.
- ⚠ **hamba rejects a `*time.Time` for a NON-union field** — `event_time` and `max_event_time` are
  required, so they must be values, and `event.Optional()` maps the zero time back to nil. Only the
  genuinely nullable ones (`exchange_event_time`, `min_event_time`, every `PipelineTimings` field)
  are pointers. A test caught this; it is not obvious from hamba's docs.
- **Every duration is a `*time.Duration`.** nil = "cannot know", and is deliberately never collapsed
  to 0, which would read as "instant" and drag any average towards zero. Negative durations are
  PRINTED, not clamped: they are clock skew between TaskManagers, and clamping would turn a clock
  problem into a latency mystery.

`source` and `end-to-end` measure from **`exchange_event_time`**, never `event_time` — see
[[avro-schema]] § 2026-09-15. For ex3/ex4/ex7-updates that is null and both print `n/a`, which is the
entire point of that field existing.

**Between-job delay is ADJACENT ONLY** — the `wait` column, `stages[i].In - stages[i-1].Out`. The
user first asked for "`type_validate_in - pair_extract_out`, `rebase_in - pair_extract_out` and so
on", two examples that are different measurements (the second skips a job and measures from job 1
again). That was asked rather than guessed, the user chose a full out→in matrix, and then **after
seeing it rendered, rejected it and kept only the adjacent delay**. Built and removed the same day.
The lesson is about the medium, not the requirement: the choice was made from a described option and
reversed on sight of the real thing, so a rendered sample is worth more than a preview here.

⚠ **Go's Duration formatting drops trailing zeros**, putting `5.6s` next to `1.597s` in one column,
so `dur()` prints three decimals past a second. (Kept from the matrix work; it applies to any column
of durations.) A second trap died with the matrix but is worth knowing: **printf pads by BYTES**, so
a `→` — three of them — inside a `%-16s` cell shifts the whole column.

**Endpoints come from `.env`** (user request, 2026-09-15), same machinery and the SAME VARIABLE NAMES
as `warmup/`: `KAFKA_BOOTSTRAP` + `SCHEMA_REGISTRY_URL`, godotenv, `.env.example` committed and `.env`
gitignored and created by a prerequisite-less `make` file target so it is never overwritten. The
first draft used `KAFKA_BROKERS`; renamed to match warmup and the deleted shell script, because a
second spelling of "where is Kafka" is a trap, not a feature.

⚠ **The two endpoint flags default to the EMPTY string on purpose.** Go evaluates flag defaults
before `.env` has been read, so a flag that defaulted to the env value would freeze the wrong
precedence; empty means "whatever config says", and main overrides only when non-empty. Order is
default → .env → real env var → flag.

⚠ **godotenv skips any key already PRESENT in the environment — an empty string counts as present.**
That is exactly what makes "a real env var beats the file" work, and it also means `t.Setenv(k, "")`
in a test does NOT simulate "unset": the file then looks ignored. The config tests unset properly
(`t.Setenv` for the restore, then `os.Unsetenv`); the first draft of them failed for this reason.

⚠ **NOT run against a real broker.** The decode tests encode against the real
`schemas/order_book_snapshot.avsc` (not a mirror, so schema drift fails there), and the metric maths
is unit-tested on the record the user captured — but nothing has consumed a live topic yet.
**`scripts/watch-topic.sh` was DELETED on 2026-09-15 at the user's instruction, before that live
run** — the sections above describing it are history; `git show feat/latency-monitor~1:scripts/watch-topic.sh`
is where it went if it is ever needed back.

**End-to-end on every topic (2026-09-15, user request).** `Render` used to `return` early when a
record had no `pipeline_timings`, which silently dropped `end-to-end` on the whole `p{id}-{side}`
family (and on any timing-less record that DID carry `exchange_event_time`). It now always prints.
Those topics have no exchange clock, so the user was asked what their end-to-end should be and chose
**`stalest` = kafka write − `min_event_time` ONLY** — not freshest (`max_event_time`) as well, and
not "keep it exchange-clock-only". This is a deliberate, scoped exception to "never measure from
`event_time`": min/max are derived from `event_time`, so a clockless feed in the union reads low —
documented in the README rather than hidden. The line appears only when `max_event_time` is set
(required on that family), so an empty union prints `stalest n/a` instead of losing the line.
