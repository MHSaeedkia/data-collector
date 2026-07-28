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
- `e2e/flink/` — `flink.RunJobs(ctx, api, normalizerDir)` cancels every running job, builds the
  normalizer modules with `mvn`, then uploads and starts the 6 job jars over the Flink REST API.
- `e2e/main.go` — wiring only: load config, `RegisterDir`, then `runTest(cfg)`, which pins
  `exchangeID`/`pairID` for the scenario and calls `topics.Create` then `flink.RunJobs`.

Verified 2026-07-28 against the live stack: 4 subjects (ids 1–4) and the 9 `ex1-p1-*` / `ex1-raw` /
`p1-asks` / `p1-bids` topics, retentions confirmed via `kafka-topics --describe`; a second run
reports every topic as already existing. All 6 normalizer jobs reach RUNNING; a second run cancels
the 6 from the first (all reach CANCELED) before resubmitting.

## Decisions

- **Each concern is its own package, `main.go` stays wiring only.** Config and registry logic are
  packages so the remaining warmup steps (topics, payloads, assertions) can land as sibling packages
  without growing `main`.
- **Topics are created over the Kafka admin API, never `docker exec kafka-topics`.** `warmup.sh` is
  only the reference for *which* topics exist and with what retention; the harness must not shell
  out to a container. `kadm.CreateTopic` returning `kerr.TopicAlreadyExists` is treated as success,
  which is the `--if-not-exists` behaviour.
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
