# End-to-end Test Harness

**Scaffold only — no harness code yet.** Laid out like `web/`: a Go module whose packages live
under `internal/`, with the build-tagged live test at the module root (where web keeps `main.go`).

When built out, this drives the whole raw pipeline against the running compose stack: reset the
stateful Flink jobs, produce a scenario's **verbatim** raw exchange payloads onto `ex{id}-raw`,
then assert what the chain produced — dead-letter records, the per-stage topics, and finally the
aggregated book on `p{pair_id}-{side}`. It replaces the six `flink/normalizer/smoke-*.sh` scripts
and `manual-test-data/{produce,reset}.sh`.

Design and rationale: `memory/project_e2e_harness.md`; task list: `todo.md` Milestone 13.

## Layout

| | |
| --- | --- |
| `internal/config` | env/`.env` loading — the only package with code so far |
| `internal/flink` | Flink REST client + the reset flow (cancel/resubmit the stateful jobs downstream-first) |
| `internal/kafka` | producer for the raw topics, reader for the output topics |
| `internal/scenario` | the raw payload set **in Go** and the per-exchange timestamp shifter |

**Raw test data is Go source, not files on disk.** Nothing is read from the filesystem at
runtime.

## Run

```bash
go test ./...                 # unit tests, no stack needed
go test -tags e2e -v ./...    # the live tests (not written yet)
```

The `e2e` build tag is what keeps the live tests out of a plain `go test ./...`.

## Config (env vars)

Defaults are the compose stack's **host-published** ports; the harness speaks TCP/HTTP to them
and never shells into a container. Copy `.env.example` to `.env` to override.

- `KAFKA_BROKER` — broker address (default `localhost:9092`)
- `SCHEMA_REGISTRY_URL` — Confluent Schema Registry (default `http://localhost:8082`)
- `FLINK_API` — Flink JobManager REST endpoint (default `http://localhost:7070`)
- `SETTLE` — seconds to wait after resubmitting jobs so every source is assigned its partitions
  before data arrives (default `8`)

## Before a live run

- All six jobs must have been submitted at least once (`make refresh-normalizer`) — the reset
  resubmits from jars already uploaded to the cluster, it does not build anything.
- The `"reset"` symbol must be registered in the `raw-order-book-event` `Type` enum **and the
  jobs resubmitted after**, or job 2 NPEs serializing a gap marker (`todo.md` M11).
- Reference data must be the seeded values — market 1 at `price_precision 2` /
  `quantity_precision 8`, rebase `0/0` — or every expected digit is wrong.

## Notes

- **Not vendored, and not built in Docker.** `web/vendor/` exists only because `web/Dockerfile`
  builds offline; this module never enters an image. It is also deliberately *not* a package
  under `web/`, whose Dockerfile runs `go test -mod=vendor ./...` at image build.
- Dependency versions are pinned to `web/go.mod`'s so the local module cache is shared.

## Stack

- [`github.com/twmb/franz-go`](https://github.com/twmb/franz-go) — Kafka producer/reader
- [`github.com/joho/godotenv`](https://github.com/joho/godotenv) — `.env` loading
- [`github.com/stretchr/testify`](https://github.com/stretchr/testify) — assertions
