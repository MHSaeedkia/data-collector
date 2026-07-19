---
name: orderbook-web
description: Standalone Go web app that consumes consolidated Kafka topics (Confluent Avro) and renders a live order book in the browser
metadata:
  type: project
---

## Live order book web UI (`web/`)

Standalone **Go** app that consumes the consolidator's output topics (`p{pair_id}-{side}`, e.g.
`p2-asks`/`p2-bids`; see [[orderbook-aggregation]]) and shows a live order book in the browser.

## Stack & shape (decisions)

- **Go**: `net/http` (static + HTTP), `github.com/gorilla/websocket` (browser push),
  `github.com/twmb/franz-go` (Kafka consumer — chosen for **regex topic subscription** +
  consumer groups, pure Go), `github.com/jackc/pgx/v5` (postgres lookups),
  `github.com/hamba/avro/v2` (Confluent-wire-format decode).
- Browsers can't read Kafka directly, so: franz-go consumer → keep latest book per topic in
  memory → push to browser over **WebSocket**. `net/http` serves `public/index.html`.

## Wire format: Confluent Avro (not JSON)

The output topics carry Confluent-wire-format Avro ([[orderbook-consolidator-decision]]).
`web/internal/schema` (`Decoder`, hamba/avro) parses the wire header (byte 0 = magic `0x0`, next
4 bytes = big-endian schema-registry id), resolves the writer schema via
`GET {SCHEMA_REGISTRY_URL}/schemas/ids/{id}`, and **caches it forever per id** (registry ids are
immutable — no TTL/invalidation needed). Decodes into a package-private `wireEvent`/`wireLevel`
pair mirroring `consolidated_order_book_event.avsc` exactly, then maps into `domain.RawBook`.

Non-obvious hamba/avro facts (confirmed by reading the library's codec source):
- `timestamp-millis` decodes to `time.Time`, not `int64` — converted via `.UnixMilli()` to keep
  `domain.RawBook.EventTime` as `int64`.
- Avro enums (`side`) decode straight into any Go `string`-kind field, no special type needed.

Malformed/undecodable records log-and-skip (`"Skipping bad message on %s: %v"`), never fatal.

`event_time` stays **epoch millis (int64) end to end** — decoder → `domain.RawBook` → JSON → browser.
It is never formatted server-side. This means there is **no UTC→local conversion to do anywhere**:
epoch millis are timezone-agnostic, and `new Date(ms).toLocaleTimeString()` in `public/index.html`
already renders in the viewer's browser timezone by default. Don't "fix" this by adding an offset —
that would double-shift. The format call passes `{ timeZoneName: "short" }` so the rendered time
carries an explicit zone label (e.g. `13:56:00 GMT+3:30`) and can't be misread as UTC.
If displayed times ever look shifted, the bug is upstream in how the producer stamps `event_time`,
not in the frontend formatting.

## ID → display resolution (key consequence of the ID-only pipeline)

The Flink output carries only `pair_id` and per-level `exchange_id` — no `base`, `quote`, or
`exchange_name` ([[avro-schema-orderbook]]). So the server resolves them for display: loads
markets and exchanges (id → name/label) from postgres, refreshed every 10s, and **enriches** each
raw book before pushing. The markets lookup joins `currencies` for the symbols ([[db-schema]]):
`SELECT m.id, b.name, q.name FROM markets m JOIN currencies b ON m.base_id=b.id JOIN currencies q ON m.quote_id=q.id`.
The enriched book sent to the browser is
`{ pair_id, base, quote, side, event_time, levels:[{price, quantity, exchange:{id,name,label}}] }`.
Unknown ids fall back to placeholders (`p{id}`/`?` for market, `unknown`/`نامشخص` for exchange).

## Architecture (hexagonal `internal/` packages) — the WHY per package

`main.go` deliberately stays in `web/` root (not `cmd/`) as a pure composition root, so
`go run .` / `go build .` / the Dockerfile build path stay unchanged. Testable logic lives in
`internal/` packages; testify for assertions (user's choice over stdlib-only).

- `internal/domain/` — plain structs only, no logic, no deps (raw wire shape, enriched shape,
  WS envelopes).
- `internal/ports/` — `MarketRepository` interface so `registry` doesn't depend on
  `*pgxpool.Pool`. Two separate load methods (not one combined query) on purpose: a failed
  exchanges load must not discard a successful markets load.
- `internal/registry/` — `Refresh(ctx)` only replaces a map if the load returned something
  (transient-error tolerance); `Enrich(RawBook) Book` is the pure id→display resolution with the
  placeholder fallback. Unit-tested via a `fakeRepo`.
- `internal/hub/` — WebSocket add/remove/publish/`ServeWS`; depends on a small `conn` interface
  (`WriteJSON`/`ReadMessage`/`Close`) instead of `*websocket.Conn`, satisfied implicitly by
  gorilla's conn. Unit-tested via a `fakeConn`.
- `internal/ingest/` — `HandleRecord(decoder, enricher, publisher, topic, value)` is the
  per-record glue (decode → `Enrich` → `Publish`). Takes three 1-method interfaces narrowed from
  `*schema.Decoder`/`*registry.Registry`/`*hub.Hub` so its bad-message-is-skipped-not-fatal
  behavior is testable with fakes, independent of those packages.
- `internal/schema/` — the Avro decoder (above). Tested by round-tripping a real Avro message
  through an `httptest.Server` standing in for the registry (also asserts the schema-id cache
  prevents a second HTTP fetch).
- `internal/kafka/`, `internal/postgres/` — thin adapters with no branching logic of their own →
  deliberately not unit-tested (would only assert "the library was called").
- `public/index.html` — single-page vanilla-JS UI, **embedded into the binary** via
  `//go:embed public`. Header is `position: fixed` with `main { margin-top: 53px }` to match.
  Price/qty columns are zero-padded client-side to the max decimal count across the current
  snapshot (asks+bids combined) — **string-based** (`decimalPlaces()`/`padDecimals()`, no
  `parseFloat`) so exactness is preserved ([[bigdecimal-rules]]); recomputed on every `render()`.

## Key implementation notes

- Subscribes via regex `^p[0-9]+-(asks|bids)$` (franz-go `ConsumeRegex`) → matches only the
  consolidated OUTPUT topics; input topics `ex{exchange_id}-p{pair_id}-{side}` carry a leading
  `ex…-` so they don't match ([[kafka-topic-strategy]]). Topic strings are otherwise opaque to
  hub/ingest — never parsed.
- WebSocket served at `/ws`; static file server at `/`.
- Fresh consumer group each start (`orderbook-web-<unixnano>`) + reset to earliest offset, so the
  current book renders on load. Replays history each restart — fine for dev only.
- New pairs added after server start are only picked up on restart (regex matched against topics
  existing at subscribe time).
- Hub broadcast holds a mutex while writing to all clients (gorilla forbids concurrent writes to
  one conn); simple and fine for a dev tool.
- Config via env: `PORT` (default 3000), `KAFKA_BROKER` (default `localhost:9092`),
  `DATABASE_URL` (default `postgres://postgres:postgres@localhost:5432/markets`),
  `SCHEMA_REGISTRY_URL` (default `http://localhost:8082`).

## Docker

- `web/Dockerfile` — multi-stage: `golang:1.26-alpine` build → `alpine:3.22` runtime (non-root
  `app` user). Static `CGO_ENABLED=0` binary; embedded UI means nothing else is copied (~32 MB).
- `web` service in `docker-compose-orderbook-consolidator.yml`: `build: ./web`, port `3000:3000`,
  `depends_on` kafka+postgres (service_healthy), env `KAFKA_BROKER=kafka:29092`,
  `DATABASE_URL=postgres://postgres:postgres@postgres:5432/markets`,
  `SCHEMA_REGISTRY_URL=http://schema-registry:8082`, on `data-collector-net`.
- **Dependencies are vendored** (`web/vendor/`, committed) and the image builds with
  `-mod=vendor` so it works fully offline — necessary because this machine's BuildKit egress
  returns 403 from `proxy.golang.org` for some module zips (the host's own network is fine).
  **Re-run `go mod vendor` after changing deps.** `web/.dockerignore` excludes the
  binary/README/Dockerfile but NOT `vendor/`.

## Run

`cd web && go run .` → http://localhost:3000  (or `go build -o orderbook-web . && ./orderbook-web`)
Docker: `docker compose up -d --build web`.
