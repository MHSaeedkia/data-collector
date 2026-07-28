---
name: e2e-harness
description: Go end-to-end harness replacing the 6 smoke-*.sh + manual-test-data/{produce,reset}.sh; raw in, aggregated book asserted out. It is a standalone APPLICATION (user, 2026-07-28 — reverses the original `go test` design), owning its stack via testcontainers (user, 2026-07-27). Design decisions and the sharp edges. No task breakdown — the user assigns work one item at a time.
metadata:
    type: project
---

# Go e2e test harness — PLAN, 2026-07-25

**Status: PROVISIONING + APP SHELL + DIAGNOSTICS IMPLEMENTED (2026-07-27, 2026-07-28); everything
else is still design.** `go run .` boots a stack, registers schemas, creates the scope's topics,
builds the jars in a container, submits all six jobs, asserts they are RUNNING, reports, and tears
down. Producing payloads, decoding Avro and asserting the book are NOT written.

Decided with the user 2026-07-25, **amended 2026-07-27: the suite owns its stack via
testcontainers** and the dev compose stack is out of the picture — see "The suite owns its stack"
below, which supersedes anything here that assumes a long-lived stack on fixed host ports.

**`todo.md` M13 carries the description only — no task breakdown.** The user removed the phase plan
2026-07-27: tasks get added one at a time, on their instruction. Do not reconstruct a phase list,
here or there, and do not start work off the strength of this document — see [[scope-discipline]].

## Why

Two disconnected test surfaces today, neither closes the loop:

- **`flink/normalizer/smoke-*.sh`** — 6 scripts, ~1200 lines, ~200 of which are byte-identical
  boilerplate copy-pasted five times. Each stops at its OWN job's topic. Coverage is ex8/OKX only
  (plus job 1's 12-fixture sweep).
- **`manual-test-data/`** ([[manual-test-data]]) — the valuable asset: 17 scenarios of real raw
  payloads across ex8/ex3/ex1/ex2. But **every expectation is prose** in a 629-line README, so the
  oracle is a human reading markdown and eyeballing the web UI. **Nothing in it has ever been run
  live**, and `reset.sh`'s Flink REST flow is unproven.

The user wants: raw in → real chain runs → assert the aggregated book on `p{pair}-{side}`. What
happens in the middle is explicitly not the priority right now. Language is **Go** — the user's
strong preference, and every dependency is already vendored in `web/`.

## User decisions (2026-07-25 Q&A — do not re-litigate)

1. ~~**A `go test` package**, build-tagged.~~ **SUPERSEDED 2026-07-28 — it is a standalone
   application.** See "The harness is an application" below.
2. **All four oracles**: aggregated book, dead-letter records, **stage topics asserted ALWAYS**, and
   a full stage dump on failure.
3. **Expectations as a Go table in the test file**, not data files beside the payloads.
   Compile-time checked. (Rejected: `expect.json` per dir — drift risk; parsing the README — brittle.)
4. **All the bash is deleted** — `produce.sh`, `reset.sh`, all 6 `smoke-*.sh`.

Decision 2 is what makes decision 4 survivable: the per-stage contracts move into the harness rather
than vanishing. **I advised against deleting the smokes before the Go harness runs green; the user
reaffirmed. The deletion must still come last — the smokes are the only working oracle while the Go
harness is debugged against a pipeline that has itself never been verified live.**

## The harness is an APPLICATION, not a `go test` package (user, 2026-07-28, REVERSES decision 1)

`e2e/` is a `main` package. `go run .` provisions a stack per scenario, asserts, reports, tears
down, and exits nonzero if anything failed. Flags: `-scenario <name>` (exact match; an unknown
name is an ERROR, not an empty run), `-keep` (skip teardown for triage), `-timeout` (default 40m).

**I recommended keeping `go test`; the user chose the application — do not re-litigate.** My
argument was that the free machinery (discovery, `-run`, `t.Cleanup`, CI reports, testify) IS the
~200 lines of duplicated bash the project set out to delete, so an app rebuilds it by hand. What
the module now owns explicitly:

| Lost with `go test` | Replaced by |
| --- | --- |
| discovery | `scenarios()` in `main.go` — a slice of `domain.Scenario` literals |
| `-run` regex | `-scenario` exact-name flag + `runner.Filter` |
| `t.Cleanup` | `defer` + `signal.NotifyContext` — **strictly better**, Ctrl-C now tears down where a killed test binary leaked 5 containers + a network |
| testify assertions | `domain.Check` funcs returning `[]domain.Failure` — **better for Clean Arch**, assertion logic has no `testing` import |
| pass/fail + exit code | `internal/runner` + `internal/report` |

**No JUnit/XML** — nothing consumes it; the exit code is what CI needs. Add a format when asked.
**Unit tests are still `go test ./...`** and need no Docker; there is no build tag any more,
because a `main` package cannot be reached by `go test` at all.

**`domain.Result` keeps `Err` and `Failures` separate and they must not be collapsed:** `Err` =
the environment never came up (nothing was asserted); `Failures` = the pipeline behaved
differently than expected (everything was asserted). The report prints them as ERROR vs FAIL.

## Clean Architecture — a requirement for THIS module (user, 2026-07-27)

**Scope: the `e2e/` harness only.** Not a repo-wide mandate — the user corrected me on that.

"This project should be all in Clean Arch, so testability and portability of all parts are
important." Concretely, for the harness:

- Dependencies point inward. The expectation/assertion logic depends on nothing; Kafka, the Flink
  REST API, the schema registry and postgres are adapters at the edge.
- Depend on narrow interfaces the harness owns, not on franz-go / hamba types.
- **Everything except the adapters must be unit-testable with no stack running** — no broker, no
  cluster, no clock, no filesystem. Swapping an adapter must not touch the logic.
- `web/` is the reference shape ([[orderbook-web]]): `internal/domain` plain structs,
  `internal/ports` interfaces, pure inner packages tested with hand-written fakes, thin adapters
  (`internal/kafka`, `internal/postgres`) deliberately NOT unit-tested, composition root at the
  module root.

## The suite owns its stack — testcontainers (user, 2026-07-27, REVERSES the earlier design)

**Superseded:** the 2026-07-25 design attached to the long-lived `docker-compose.yml` dev stack and
reset job state between scenarios. The user's decision: **testcontainers boots the environment for
the integration tests, the harness deploys the Flink jobs into it, then produces raw events and
asserts. The dev compose stack is not used.** I had recommended the opposite (boot cost); the user
overrode it — do not re-litigate.

**The strongest argument for it, which I underweighted at the time:** the harness becomes the ONLY
writer of `ex{id}-raw`. On a shared long-lived stack a live collector feed on the exchange under
test silently corrupts results, and exchange-scoped assertions do not save you. A purpose-built
stack deletes that whole class of problem by construction rather than by a "remember to stop X"
line in a checklist.

**The e2e stack is exactly 5 services** — `jobmanager`, `taskmanager`, `kafka`, `schema-registry`,
`postgres` — and nothing else. **Data collection is out of scope for this project entirely (user,
2026-07-27): no collector service in `docker-compose.e2e.yml`, and no mention of one anywhere in
`e2e/`'s documentation.** The suite produces every byte it asserts on.

**Warmup runs before EACH test** (user, 2026-07-27): create the required schemas and topics per
test, not once per stack boot. `scripts/warmup.sh` cannot be reused — it is `docker exec`-based
(execs `kafka-topics` into the kafka container and `psql` into postgres). Port its three pieces:

- **schema subjects** — 4 POSTs of `schemas/*.avsc` to `/subjects/{subject}/versions`
  (`aggregated-order-book-event`, `raw-order-book-event`, `order-book-snapshot`,
  `rejected-order-book-event`)
- **topics** — `kadm`, every one `--partitions 1`; the stage/raw/output topic families are derived
  from a `exchange_markets ⋈ markets ⋈ exchanges` query, so **the DB seed must land before topic
  creation**
- **DB seed** — free: mount `postgres/02_seed.sql` into the image's `/docker-entrypoint-initdb.d`

**RESOLVED 2026-07-27 — one stack PER SCENARIO** (user, over my recommendation of per-run). The
consequence is the good one: **there is no job-reset step and no `JobResetter` port**. Operator
state cannot leak between scenarios because the cluster does not survive them, which retires the
oldest open question in the plan (whether `reset.sh`'s Flink REST flow works) by deleting the need
for it. The cost is a full boot + warmup + 6 submissions per scenario; the jars are built once per
`go test` run to keep that survivable.

## Nothing may depend on the host machine (user, 2026-07-27)

**The suite must run on a CI/CD platform with NO Java installed.** Therefore **the job jars are
built inside a container** — a Maven/Flink image — never on the host. This **overrides my earlier
recommendation** ("pre-build via a Make target, fail fast if the jars are missing"), which silently
assumed a host JDK + Maven. Docker is the only host dependency the suite may have.

Build facts (verified 2026-07-27 from `flink/normalizer/pom.xml`): **Java 21, Flink 2.2.0**, 7
modules — `common` + `job-{pair-extractor,type-validator,rebaser,precision,book-builder,aggregator}`.
`run-job.sh` builds one at a time with `mvn -pl <module> -am package -q -DskipTests`; for the suite,
one reactor build of all six is enough.

**The cluster image is NOT the stock `flink:` image** — correcting what I wrote above.
`flink/normalizer/Dockerfile` (`FROM flink:2.2.0-scala_2.12-java21`) layers into `/opt/flink/lib`:
the Kafka connector 5.0.0-2.2, `flink-avro` + `flink-avro-confluent-registry` 2.2.0, avro 1.11.4,
Jackson 2.14.3, and the Confluent registry client's full dependency closure (resolved by a throwaway
apt-installed Maven). Jobs submitted to a stock image would fail at runtime. The e2e stack must
build/use **that** Dockerfile — note it already establishes the precedent of running Maven at image
build time.

**Shape, CONFIRMED and built 2026-07-27:** a multi-stage `flink/normalizer/Dockerfile.jobs` —
`FROM maven:3.9-eclipse-temurin-21 AS build`, `mvn package -DskipTests`, final stage carrying just
the six shaded jars. The Go side builds it, copies the jars out of a throwaway container, and
REST-uploads them to the JobManager. Keeps **session mode**. Do NOT bake jars into
`/opt/flink/usrlib` — that is application-mode shaped, and REST upload needs the jar bytes
client-side anyway.

**Cost, and the thing that will actually hurt:** a cold Maven build pulls the whole Flink dependency
closure, and the cluster image itself downloads ~10 jars + apt-installs Maven. Mitigations, in
order: order the Dockerfile `COPY pom.xml` → `dependency:go-offline` → `COPY src` so the dep layer
is cached; give CI a persistent buildx/registry layer cache; and best for CI, build both images once
in a build stage, push them, and let the suite consume them by tag via env override
(`E2E_JOBS_IMAGE` / `E2E_FLINK_IMAGE`). Small port, large time win — without it every CI run pays
the full download.

**Build-context gotcha:** `flink/normalizer/` is outside `e2e/`, so the builder Dockerfile's context
must be `flink/normalizer` (suggest `flink/normalizer/Dockerfile.jobs`). Resolve that path from the
module root, never from the test's working directory.

**Recommended, unconfirmed (rest):**

- **A reuse escape hatch** (`E2E_REUSE_STACK=1`): CI boots cold, local red-green iteration does not
  pay a full boot. Triage is where the time will actually go — see the closing risk section.
  (Harder now that the stack is per-scenario, but the need is the same.)
- Container **wait strategies** replace only *part* of the `SETTLE` sleep. They cover "registry
  answers, taskmanager has free slots"; they do **not** cover "every Flink source has been assigned
  its partitions". Expect to keep a smaller settle.

## Provisioning as built (2026-07-27)

**Hand-wired containers in Go, no compose file** (user, chosen over my compose-module
recommendation). There is no `docker-compose.e2e.yml` and nothing in `e2e/` mentions one.

**Facts that constrain the wiring — verified from the Java, not assumed:**

- Every job reads its own config from env with **in-network defaults**: `kafka:29092`,
  `http://schema-registry:8082`, `jdbc:postgresql://postgres:5432/markets`. So the containers'
  network aliases must be exactly `kafka` / `schema-registry` / `postgres`, and then **no job
  configuration has to be passed at all**. Change an alias and every job breaks silently.
- **The database is `markets`**, created by `postgres/01_schema.sql`. `POSTGRES_DB` stays
  `postgres`; the init scripts make the real one. Waiting for the *second* "database system is ready
  to accept connections" is what proves the seed landed — the first is the temporary server that
  runs the init scripts.
- **All six sources subscribe by topic PATTERN, start at `latest()`, and set no
  `partition.discovery.interval.ms`.** Hence topics MUST exist before submission; a topic created
  afterwards is missed entirely or found a discovery interval later, having lost everything produced
  in between. This is why `provision.Run`'s step order is a contract, not a style choice.
- **Kafka is the one service whose host port cannot be dynamic.** A broker advertises the address
  clients reach it on, and that must be in its environment before it starts. The harness picks a
  free port itself; everything else uses Docker-assigned ports read back at runtime.

**Warmup is scope-scoped, not DB-derived (user, 2026-07-27).** `scripts/warmup.sh` derives topics
from `exchange_markets`; the seed has 355 rows, so a faithful port would create ~2,250 topics **per
scenario**. Instead a scenario declares its `Scope{ExchangeID, PairID}` and gets its 9 topics. This
**removed the Postgres query and the pgx dependency from the harness entirely** — postgres still
runs, because jobs 3 and 4 look up rebases and precisions in it.

**The jars are built once per process and cached in memory**, even though the stack is per scenario.
A cold Maven build pulls the whole Flink closure; paying it per scenario would dominate the run.

**Added `flink/normalizer/.dockerignore`** (`*/target/`) so a host-side Maven build cannot leak
stale jars into either image's build context.

**Known flake, observed 2026-07-27 on the first live run:** the *cluster* image's
`apt-get install maven` step failed with exit 100 — a transient Debian mirror error — and took the
whole test down with it, five minutes in. The identical build succeeded on retry. Every run rebuilds
that image, so CI inherits this: it is the strongest argument for the
**`E2E_FLINK_IMAGE` / `E2E_JOBS_IMAGE` prebuilt-tag override** (build once in a CI build stage, let
the suite consume by tag). Not implemented — no authorization.

**The 8s settle survived.** Wait strategies cover "registry answers, taskmanager registered"; they
do not cover "every source has been assigned its partitions", and nothing in the REST API exposes
that. Inherited from the shell scripts' `SETTLE=8`.

## Failure diagnostics — and who owns teardown (2026-07-28)

Provisioning failure used to report `not RUNNING within 2m (last state "")` and nothing else,
because `harness.Start` terminated the stack itself on the way out — the actual stack trace died
with the taskmanager. Triage is the dominant cost of this harness, so this was the worst place to
save a few lines.

**`harness.Start` no longer tears anything down. THE CALLER ALWAYS OWNS TEARDOWN.** It returns a
**non-nil `*Env` alongside its error** whenever containers actually started, so diagnostics can be
read off a half-provisioned stack before it is closed. The one exception is a `stack.Start`
failure, which self-terminates its own partial stack and returns nil — there is nothing left to
read. `ports.Provisioner.Start` does the typed-nil dance deliberately (a `(*Env)(nil)` in an
interface is not `nil`); do not "simplify" it away.

`internal/diagnostics.Collect` gathers job states, `/jobs/{id}/exceptions` for anything not
RUNNING, and the last 120 lines of the jobmanager + taskmanager logs. It **never returns an
error** — diagnostics run when something already went wrong and must not replace the real
failure; each section reports its own problem inline. The runner collects on a **fresh context**
(`context.WithoutCancel`), because the most valuable case is precisely when the run's context has
already timed out or been interrupted.

`stack.Stack` now stores containers as `named{name, c}` and exposes `Logs(ctx, names...)`. The
five aliases are exported consts (`stack.Kafka`, `stack.JobManager`, …) — they were always a
contract with the jobs' in-network defaults, and now they are also the log keys.

**`internal/config` is DELETED** (2026-07-28) along with `godotenv`. Nothing imported it, its
`localhost:9092/8082/7070` defaults were actively wrong once ports went dynamic, and `settle` was
already hardcoded in `harness.go` duplicating its `defaultSettleSeconds`.

## Design decisions worth keeping

**New top-level `e2e/` module, NOT inside `web/`.** `web/Dockerfile` runs `go test -mod=vendor ./...`
during the image build — an e2e package there would break `docker compose build web`.

**No vendoring.** `web/vendor/` exists only because that Dockerfile must build offline
(proxy.golang.org 403s on this dev machine). The harness never builds in Docker. Verified
2026-07-25: `franz-go@v1.21.4` and `hamba/avro/v2@v2.31.0` are already in the local module cache;
`pkg/kadm` is not, but **fetching it over the network worked**, so the 403 risk did not materialize.
Pin to web/'s versions so the cache is shared.

**No `docker exec` anywhere.** Everything talks TCP/HTTP to mapped container ports. This kills the
`grep -m1 '^{'` the bash needs to strip log4j noise out of `kafka-avro-console-consumer` stdout.
(Originally this meant the compose stack's fixed host ports 9092/8082/7070/5432; since the
testcontainers decision below the ports are dynamic per run and come from the stack provider, so
nothing may hardcode them — `internal/config`'s `localhost:*` defaults are now vestigial.)

**Keep the bash's proven read pattern**: capture the topic's END offset before producing, then read
from exactly that offset. No consumer group, no `auto.offset.reset` race. `kadm.ListEndOffsets` is
the direct analogue of the `kafka-get-offsets` the scripts already shell out to; read via
`kgo.ConsumePartitions` at an explicit offset. Every topic is single-partition (`warmup.sh` creates
them `--partitions 1`), so partition 0 is always right.

**Do NOT reuse `web/internal/kafka/consumer.go`** — consumer-group + regex-subscribe, and it drops
key/offset/timestamp. Wrong shape. There is no producer anywhere in the repo to reuse.

**DO copy the approach in `web/internal/schema/decoder.go`** (magic byte `0x00`, 4-byte big-endian
schema id, `GET {registry}/schemas/ids/{id}`, `avro.Parse`, cache forever under a mutex). Its wire
structs and `magicByte` are package-private and it only decodes the aggregated shape, so the harness
needs its own generalized copy.

**Quiescence, not sleep.** Read until the expected count, OR a 2s quiet window after the first
record, OR a 25s hard cap (= the scripts' `CONSUME_TIMEOUT_MS`). Matters most on the aggregated
topic: the aggregator emits one event per side per input message, so the final book is the **last**
record per side — never a count.

## Sharp edges (the things that will silently break the port)

- **`encoding/json` must use `Decoder.UseNumber()`.** Without it every number becomes `float64` and
  `1800000000000` re-marshals as `1.8e+12` — job 1 sees a different wire shape. Easiest way to break
  this invisibly.
- **The timestamp field is a different Go type per exchange** (verified against the payloads
  2026-07-25): ex8 `.data[].ts` is a **string**; ex1 `lastUpdate` is a **number**; ex2 `event_time`
  is a **number on REST and an ISO-8601 string on WS** under the same name; ex3 has none and is a
  **top-level JSON array**, so the shifter must type-switch before touching it.
- Port all of `produce.sh`'s shift rules, including: ex8 aligns to the 300 ms grid (its `ts` is
  simultaneously event time AND sequence id); ex1 leaves `push.pub.offset` **untouched** (the
  sequence is independent of the timestamp); ex2 shifts both shapes by the same delta in different
  units (bases are whole seconds precisely so this stays exact); ex3 is never rewritten.
- **Scenario 16 file `05-rest-snapshot-string-event-time.json` must NOT be shifted** — its whole
  point is that job 1 drops it. Same `else . end` guard the jq has.
- The jq quirks in the scripts (`.pipeline_timings["io.tibobit.orderbook.PipelineTimings"]`,
  `.asks.array[0]` on raw vs bare `.asks[0]` on the snapshot schema) are
  `kafka-avro-console-consumer` JSON-encoding artifacts. Decoding into Go structs makes them
  disappear — **do not port them.**
- hamba/avro mapping: `timestamp-millis` → `time.Time`, `["null", T]` → **pointer**, enums →
  `string`. Prices/quantities stay `string` per [[bigdecimal-rules]] — never parsed to float.

## Isolation hazards

- **Scenarios cannot run in parallel** — not for state reasons any more (each gets its own stack)
  but for resource ones: five containers including a Flink cluster, on a Docker Desktop that
  reported under 4 GB. `t.Run` without `t.Parallel()`.
- **No reset step.** Superseded by the per-scenario stack. Jobs are still SUBMITTED
  downstream-first, the same order as [[manual-test-data]]'s `reset.sh`, because sources read from
  `latest()`.
- **Assertions scoped by `exchange_id`**, as `smoke-aggregator.sh` already does. Never assert "p1 has
  exactly N levels" — a scenario on another exchange may legitimately contribute to the same pair.
- **The reference data is now guaranteed rather than checked** — the seed is mounted into the
  postgres container, so market 1 is `price_precision 2` / `quantity_precision 8`, rebase `0/0`
  (identity) by construction. Every expected digit depends on that; if the seed ever diverges,
  every scenario's expectations are wrong at once.

## Honest scoping

The **dead-letter oracle is already tabulated** in the README's final table (01–07: 1/0/2/2/0/0/0;
08–12 and 13–17: 0/1/2/0/1 each) — encoding it is mechanical and cheap.
The **final-book expectations are NOT tabulated** and must be derived per scenario from prose plus
the payload files. That is the bulk of the work — expect it to dominate whatever the schedule is.

## Residual coverage the harness does NOT absorb

Two contracts die with the smokes unless separately ported — decide before deleting them, don't
discover it after:

- **`smoke-rebaser.sh`'s DB mutation** (UPDATE `exchange_markets`, wait out the 60s
  `RefreshingLookup`, restore on an EXIT trap). The seed is `0/0` = identity, so **nothing else in
  the repo proves job 3 works** ([[rebaser]]).
- **`smoke-pair-extractor.sh`'s 12-fixture sweep** across ex1–ex6+ex8. The scenarios only cover 4
  exchanges, so deleting it loses parser coverage for **ex4, ex5, ex6** ([[pair-extractor]]).

## Risk that dominates everything else

The expectations themselves have never been validated live. Early red tests are as likely to be the
README's fault or the harness's as the pipeline's — **budget for triage, not just implementation**.

The oldest unknown is whether `reset.sh`'s Flink REST flow works at all (open since 2026-07-20, M8).
Getting reset + produce-one-scenario running live retires it, and asserts nothing — worth doing
before any expectation work rests on it.
