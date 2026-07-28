---
name: e2e-harness
description: e2e/ is a standalone Go app (`go run .`); it registers the Avro schemas, creates the Kafka topics for one exchange/pair, and builds and submits the normalizer Flink jobs.
metadata:
    type: project
---

# `e2e/` — warmup

`e2e/` is a standalone Go application (`go run .` from inside `e2e/`), not a `go test` package.
Module path is `orderbook-e2e`, so internal imports are `orderbook-e2e/<pkg>`.

## Layout

- `e2e/config/` — `config.Load(".env")` returns
  `Config{SchemaRegistryURL, SchemasDir, KafkaBroker, FlinkAPI, NormalizerDir}`.
- `e2e/schemaregistry/` — `schemaregistry.RegisterDir(registryURL, dir)` globs `*.avsc` from `dir`
  and POSTs each to `/subjects/{subject}/versions` as `{"schemaType":"AVRO","schema":...}`.
- `e2e/topics/` — `topics.Create(ctx, broker, exchangeID, pairID)` creates the 9 topics one
  exchange/pair pipeline needs, over the Kafka admin API (`kadm`), 1 partition / 1 replica each.
  `topics.Delete(ctx, broker, exchangeID, pairID)` removes the same 9 and waits until the broker
  stops listing them.
- `e2e/flink/` — `flink.CancelJobs(ctx, api)` cancels every running job and waits for each to reach
  a terminal state; `flink.RunJobs(ctx, api, normalizerDir)` builds the normalizer modules with
  `mvn`, then uploads and starts the 6 job jars over the Flink REST API. `RunJobs` does not cancel:
  the caller must have run `CancelJobs` first.
- `e2e/producer/` — `producer.SendJSON(ctx, broker, topic, doc)` compacts the document and produces
  it as one record (`kgo.ProduceSync`).
- `e2e/events/` — Go mirrors of the Avro records the pipeline carries, for decoding what comes
  back off the topics: `OrderbookSnapshot`, `PriceLevel`, `PipelineTimings`, `StepTimings`,
  `AvroTime`.
- `e2e/warmup/` — `warmup.Run(ctx, cfg, exchangeID, pairID)` is the whole pre-scenario sequence:
  `flink.CancelJobs` → `topics.Delete` → `topics.Create` → `flink.RunJobs`.
- `e2e/main.go` — wiring only: load config, `RegisterDir`, then `runTest(cfg, pairID, exchangeID,
  TestPayload{SourceData, WantedData})`, which calls `warmup.Run`, then produces
  `payload.SourceData` to `ex{exchangeID}-raw`.

Verified 2026-07-28 against the live stack: 4 subjects (ids 1–4) and the 9 `ex1-p1-*` / `ex1-raw` /
`p1-asks` / `p1-bids` topics, retentions confirmed via `kafka-topics --describe`; with `Delete`
in front, a second run deletes the 9 and recreates them empty. All 6 normalizer jobs reach RUNNING; a second run cancels
the 6 from the first (all reach CANCELED) before deleting the topics and resubmitting.

## Decisions

- **Each concern is its own package, `main.go` stays wiring only.** Config and registry logic are
  packages so the remaining warmup steps (topics, payloads, assertions) can land as sibling packages
  without growing `main`.
- **Topics are created over the Kafka admin API, never `docker exec kafka-topics`.** `warmup.sh` is
  only the reference for *which* topics exist and with what retention; the harness must not shell
  out to a container. `kadm.CreateTopic` returning `kerr.TopicAlreadyExists` is treated as success,
  which is the `--if-not-exists` behaviour.
- **Bringing the stack to a clean start is one call, `warmup.Run`, not four in `runTest`.** The four
  steps are a single unit with an order that must not drift, so they live behind one entry point in
  their own package; `runTest` is then just warmup → produce → verify. `warmup` imports `config`,
  `flink` and `topics`, so nothing below it may import `warmup`.
- **Teardown order is jobs, then topics: cancel → delete → create → submit.** The jobs go down
  first because a running job holds the topics it consumes open while they are being deleted, and
  one left alive would attach to the recreated topics mid-setup and read the harness's own
  provisioning as pipeline input. That is why `RunJobs` no longer cancels — cancelling is a
  separate, earlier step (`flink.CancelJobs`), not part of submission.
- **Every run starts from an empty broker.** `runTest` deletes the 9 topics before creating them, so
  no record from a previous run can be read as a result of this one — offsets, retained payloads and
  half-consumed stages all go away with the topic. Deletion is asynchronous: `DeleteTopics` returns
  before the topics are gone and creating one still marked for deletion fails, so `Delete` polls
  `ListTopics` until none of the names are listed. `UnknownTopicOrPartition` per topic is success —
  it is already absent.
- **Creation order matters.** The five normalizer stage topics are created before anything else:
  normalizer sources read from `latest`, so a topic missing when its job starts is discovered late
  and whatever was produced in between is lost. The same rule fixes job submission order:
  DOWNSTREAM-FIRST (aggregator → book-builder → precision → rebaser → type-validator →
  pair-extractor), so no job is started before the one it feeds.
- **Jobs are submitted over the Flink REST API; only the build shells out.** `flink.RunJobs` does
  `GET /jobs` + `PATCH /jobs/{id}?mode=cancel`, `POST /jars/upload` (multipart) and
  `POST /jars/{jarID}/run`, polling `GET /jobs/{id}` for the state. `exec` is used for `mvn` alone —
  no `docker`, no `curl`, no `jq`. The entry point is each jar manifest's `Main-Class`, so no
  per-module class mapping is needed.
- **One reactor build, not six.** `run-job.sh` builds each module with `-pl <module> -am`, which
  rebuilds `common` six times for the same jars; the harness runs `mvn -f <dir>/pom.xml package -q
  -DskipTests` once and then globs each `<module>/target/*-1.0-SNAPSHOT.jar`, skipping shade's
  `original-*` copy.
- **The scenario payload goes in raw, as the exchange sends it.** `runTest` produces
  `payload.SourceData` verbatim (compacted) to `ex{exchangeID}-raw` — the same topic NiFi writes to —
  so the run exercises the whole pipeline from the pair-extractor down. No key, no Avro encoding:
  the raw topic carries plain exchange JSON. Auto topic creation is off on the broker, so producing
  before `topics.Create` fails with `UNKNOWN_TOPIC_OR_PARTITION`.
- **The event structs keep the Avro JSON union wrappers.** A nullable record arrives nested under
  its own name and a nullable `long` under `"long"`, so `PipelineTimings` is a wrapper holding one
  `StepTimings` field and timestamps are `AvroTime{Long string}` — the timestamps come through as ISO-8601
  strings, not epoch millis, even though the `.avsc` types them `long`/`timestamp-millis`.
  Verified by round-tripping a real book-builder payload: unmarshal + re-marshal is byte-identical.
- **Subjects are derived, not hardcoded.** The subject is the file name with `_`→`-`
  (`aggregated_order_book_event.avsc` → `aggregated-order-book-event`). A new `.avsc` needs no
  code change — but a file whose subject is not the dashed file name would register under the
  wrong subject.
- **`.env` is parsed by hand (~20 lines), no `godotenv`.** Real env vars win over the file;
  a missing `.env` is not an error.
- **Defaults**: `SCHEMA_REGISTRY_URL=http://localhost:8082`, `SCHEMAS_DIR=../schemas`,
  `KAFKA_BROKER=localhost:9092`, `FLINK_API=http://localhost:7070`,
  `NORMALIZER_DIR=../flink/normalizer` (paths relative to `e2e/`). `e2e/.env` is not in the repo —
  only `.env.example`.
