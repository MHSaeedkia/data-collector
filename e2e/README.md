# End-to-end Test Harness

A Go **application** that provisions its own stack, deploys the normalizer
pipeline into it, and (eventually) drives raw exchange payloads through it.

**What exists today: provisioning.** A scenario can ask for a stack and get back
one with schemas registered, topics created and all six jobs RUNNING. Producing
payloads and asserting the aggregated book is not written yet.

It is an application, not a `go test` package (user decision 2026-07-28,
reversing the original design). The lifecycle is explicit and interruptible:
Ctrl-C runs teardown, where a killed test binary would leak five containers and
a Docker network. In exchange, the suite owns what a test framework used to
provide — scenarios are a Go slice rather than discovered functions, `-scenario`
replaces `-run`, and assertions are `domain.Check` funcs returning
`domain.Failure` values rather than testify calls.

Design and rationale: `memory/project_e2e_harness.md`; task list: `todo.md`
Milestone 13.

## The stack is the suite's own

`harness.Start` boots five containers via testcontainers on a private Docker
network — `postgres`, `kafka`, `schema-registry`, `jobmanager`, `taskmanager` —
and tears them down afterwards. The dev `docker-compose.yml` stack is not used
and is not required to be running.

**One stack per scenario.** The pipeline keeps operator state and never
checkpoints, so a shared cluster would carry one scenario's order book into the
next. It also makes the suite the only writer of the raw topics, so nothing
external can corrupt a scenario.

Ports are whatever Docker assigns, and are read off the containers at runtime.
The one exception is Kafka: a broker has to advertise the address clients reach
it on before it starts, so the harness picks a free host port up front.

## Nothing depends on the host but Docker

CI has no JDK and no Maven. Both images are built from the repo:

| | |
| --- | --- |
| `flink/normalizer/Dockerfile` | the **cluster** image — stock Flink plus the Kafka connector, flink-avro, the Confluent registry client and Jackson in `/opt/flink/lib`. Jobs fail at runtime without them. |
| `flink/normalizer/Dockerfile.jobs` | the **build** image — a multi-stage Maven build whose final stage carries only the six shaded job jars. The harness copies them out of a throwaway container and uploads them over Flink's REST API. |

The jars are built once per `go test` run and reused by every scenario; the
Docker layer cache makes repeat runs cheap.

## Provisioning order

It is a hard requirement, not a preference:

1. **Schemas** — four subjects POSTed to the registry.
2. **Topics** — every source subscribes by *pattern* and starts at `latest()`,
   so a topic created after its job started is missed entirely, or discovered a
   partition-discovery interval later having lost everything in between.
3. **Jars** — the slow step, done before the cluster is touched.
4. **Jobs** — submitted downstream-first, each RUNNING before the next.
5. **Settle** — a job reports RUNNING before its sources have been assigned
   their partitions, and nothing in the REST API exposes source readiness.

Only the scope's own topics are created (`domain.TopicsFor`), not the ~2,250 the
seeded `exchange_markets` table implies.

## Layout

| | |
| --- | --- |
| `main.go` | composition root: flags, signal handling, the scenario list |
| `internal/domain` | plain values and pure logic: scopes, topic naming, retentions, job order, scenarios and results |
| `internal/ports` | the interfaces the harness owns |
| `internal/provision` | the use case — ordering and error handling, no I/O |
| `internal/runner` | the use case — scenario lifecycle: provision, check, diagnose, tear down |
| `internal/checks` | the expectations, as adapters that report `domain.Failure` |
| `internal/report` | renders results to a terminal |
| `internal/stack` | testcontainers adapter: the five containers |
| `internal/jobs` | builds the job jars in a container |
| `internal/flink` | JobManager REST: upload, run, wait for RUNNING, read exceptions |
| `internal/diagnostics` | what a failing stack can still tell you, collected before teardown |
| `internal/registry` | Schema Registry: register a subject |
| `internal/kafka` | topic creation |
| `internal/schemas`, `internal/repo` | reading `schemas/*.avsc` from outside this module |

Dependencies point inward: `provision` and `runner` depend only on `ports` and
`domain`, and are tested with fakes and nothing running.

**Raw test data will be Go source, not files on disk.**

## Run

```bash
go test ./...                     # unit tests — no Docker, nothing to tag
go run .                          # every scenario, against a real stack
go run . -scenario provisioning   # just one
go run . -keep                    # leave the stack up to poke at
go run . -timeout 60m             # default is 40m
```

Exit status is 0 only if every scenario passed. No build tag is needed any
more: the live path is a `main` package, so `go test ./...` cannot reach Docker
by construction. Allow a long timeout on a cold run — two images get built, one
of which runs a full Maven build.

When a scenario fails, the report includes the job states, the Flink exception
behind anything not RUNNING, and the tail of the jobmanager/taskmanager logs,
collected before the containers are destroyed.

## Notes

- **Not vendored.** `web/vendor/` exists only because `web/Dockerfile` builds
  offline; this module never enters an image. It is deliberately not a package
  under `web/`, whose Dockerfile runs `go test -mod=vendor ./...` at image build.
- Shared dependency versions are pinned to `web/go.mod`'s so the module cache is
  shared.

## Stack

- [`testcontainers-go`](https://github.com/testcontainers/testcontainers-go) — the containers
- [`franz-go`](https://github.com/twmb/franz-go) — Kafka client and admin
- [`testify`](https://github.com/stretchr/testify) — assertions, in the unit tests only
