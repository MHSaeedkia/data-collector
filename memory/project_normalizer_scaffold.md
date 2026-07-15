---
name: normalizer-scaffold
description: flink/normalizer/ multi-module Maven scaffold (M1) — module conventions, common/ library contents, run-job.sh + compose decisions
metadata:
    type: project
---

# `flink/normalizer/` scaffold (Milestone 1, done 2026-07-15)

The Maven multi-module home of the 6 raw-pipeline jobs ([[raw-pipeline-decision]]).

## Module conventions (follow these when adding job modules in M2–M7)

- Parent: `io.tibobit:normalizer-parent` (packaging `pom`), versions copied from the
  consolidator pom (Flink 2.2.0, kafka connector 5.0.0-2.2, avro 1.11.4, confluent client
  7.5.3, JUnit 5.10.2, AssertJ 3.25.3, Java 21). All shared deps live in
  `dependencyManagement`; plugins (resources/surefire/jacoco/shade) in `pluginManagement` —
  job modules declare them version-less.
- **Modules are added one per milestone** (user decision): `<modules>` starts with `common`
  only. New job module = dir name `job-*` listed in parent `<modules>`, shade plugin with its
  own `mainClass` manifest entry (run-job.sh relies on the manifest — it submits with no
  entry-class), artifactId SHOULD match the dir name (run-job.sh finds the jar by glob
  `*-1.0-SNAPSHOT.jar` excluding `original-*`, so a mismatch works but don't).
- `common` module: artifactId `normalizer-common`, plain jar (no shade), package root
  `io.tibobit.normalizer`. Job modules depend on it compile-scope; Flink/avro/confluent deps
  are `provided` (in `/opt/flink/lib/` via the Dockerfile).
- Test-classpath schemas: the resources-plugin copies the 3 raw-pipeline `.avsc` files from
  repo-root `schemas/` to `target/test-classes/avro/` (test-only; runtime is registry-only).

## What's in `common/` (all tested, 18 tests)

- `model/`: `PriceLevel`, `RawOrderBookEvent` (nullable `Long sequenceId`, nullable
  `List<PriceLevel> asks/bids` — null ≠ empty, see [[avro-schema-orderbook]]),
  `OrderBookSnapshot`, `RejectedOrderBookEvent`. Consolidator-style mutable POJOs.
- `avro/AvroSchemaLoader`: verbatim port from the consolidator (`loadLatest` = registry,
  `load` = classpath, tests only).
- `serde/`: `RawOrderBookEventSerializer`/`Deserializer`, `OrderBookSnapshotSerializer`/
  `Deserializer`, `RejectedOrderBookEventSerializer` (**serializer only** — nothing consumes
  dead-letter; add a deserializer only if a consumer appears). Mapping statics are
  package-private (`toGenericRecord`/`fromGenericRecord`), lazy transient Confluent
  serializer init, fixed subjects. Shared `PriceLevels` package-private helper does level-list
  + null-vs-empty mapping and union-unwrapping; the rejected serializer reuses
  `RawOrderBookEventSerializer.toGenericRecord` for the nested event (identical inline
  definition).
- `lookup/RefreshingLookup<K,V>`: generic periodic-refresh reference reader with a
  serializable `Loader<K,V>` fn (fake in tests; **the real JDBC exchange_markets closure is
  built in M2**, not in common). Contract: `open()` fail-fast on initial load, daemon-thread
  scheduled reload, failed refresh keeps last-good + WARN, `get()` returns null for unknown.
- `decimal/Decimals`: `canonicalize` (zero → "0", stripTrailingZeros + toPlainString to dodge
  1E+3), `rebase` (`scaleByPowerOfTen`), `truncate` (`setScale(p, DOWN)`).

## Deploy tooling

- `flink/normalizer/run-job.sh <module>`: consolidator's script parameterized — builds with
  `-pl <module> -am`, uploads to `FLINK_API` (default `http://localhost:7070`), submits via
  manifest Main-Class, waits for RUNNING, dumps root cause + taskmanager logs on failure.
  No-arg run prints available modules from the parent pom.
- `Makefile`: `make run-local MODULE=job-x` / `run-remote` (192.168.150.104) / `test`.
- `Dockerfile` + `confluent-deps-pom.xml`: byte-identical copies of the consolidator's — ONE
  cluster image hosts consolidator + normalizer jobs. If the consolidator Dockerfile changes,
  mirror it here.
- `docker-compose-normalizer.yml` (repo root): **the full replacement stack** (user decision
  2026-07-15) — identical to `docker-compose-orderbook-consolidator.yml` (same container
  names/ports/volumes, so only ONE of the two composes runs at a time) except jobmanager/
  taskmanager build from `./flink/normalizer` and the taskmanager has **8 task slots** so the
  single cluster fits the consolidator job + 6 normalizer jobs.

**Why:** every job module M2–M7 builds on these conventions; deviating breaks run-job.sh or
duplicates common code.
**How to apply:** when adding a job module, copy the conventions above; when touching the
cluster image or compose, keep the consolidator/normalizer copies in sync.
