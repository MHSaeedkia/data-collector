---
name: orderbook-web
description: Standalone Go web app that consumes consolidated Kafka topics and renders a live order book in the browser
metadata:
  type: project
---

## Live order book web UI (`web/`)

Standalone **Go** app that consumes the Flink job's consolidated output topics
(`p{pair_id}-{side}`, e.g. `p2-asks`/`p2-bids`; see [[orderbook-aggregation]]) and shows a
live order book in the browser. Rewritten from the original Node.js app on 2026-06-28.

## Wire format: JSON → Avro (2026-07-13)

The consolidator's output switched to true Confluent-wire-format Avro on 2026-07-11 (see
[[orderbook-consolidator-decision]] / [[avro-schema]]) but `web/` was left on `json.Unmarshal`
at the time — a known, flagged deploy-blocking gap. Fixed 2026-07-13:

- New package `web/internal/schema` (`Decoder`, `github.com/hamba/avro/v2`): `Decode(value
  []byte) (domain.RawBook, error)` parses the Confluent wire header (byte 0 = magic `0x0`, next
  4 bytes = big-endian schema-registry id), resolves the writer schema via
  `GET {SCHEMA_REGISTRY_URL}/schemas/ids/{id}`, and caches it forever per id (registry ids are
  immutable — no TTL/invalidation needed). Decodes into a package-private `wireEvent`/`wireLevel`
  pair mirroring `consolidated_order_book_event.avsc` exactly, then maps into `domain.RawBook`.
- `event_time` is decoded as `time.Time` (hamba/avro maps the `timestamp-millis` logical type to
  `time.Time`, not `int64` — confirmed by reading the library's codec source before relying on
  it), then converted via `.UnixMilli()` to keep `domain.RawBook.EventTime` as `int64` unchanged.
  `side` (Avro enum) decodes straight into a Go `string` field — hamba/avro's enum codec accepts
  any `string`-kind target, no special type needed.
- `internal/ingest.HandleRecord` gained a `decoder` 1-method interface as its **new first
  parameter** (`HandleRecord(dec decoder, reg enricher, h publisher, topic string, value
  []byte)`), narrowed from `*schema.Decoder` the same way `enricher`/`publisher` are narrowed
  from `*registry.Registry`/`*hub.Hub` — keeps `ingest`'s tests independent of the schema
  package, via a `fakeDecoder`. `json.Unmarshal` and the `encoding/json` import were removed
  from `ingest.go` entirely (decoding is no longer this package's concern).
- New config `SCHEMA_REGISTRY_URL` (default `http://localhost:8082`, the host-exposed listener —
  same pattern as `KAFKA_BROKER`; compose sets it to `http://schema-registry:8082`).
  `main.go` builds one `schema.NewDecoder(cfg.SchemaRegistryURL)` and passes it into every
  `ingest.HandleRecord` call.
- Malformed/undecodable records still just log-and-skip (`"Skipping bad message on %s: %v"`) —
  behavior unchanged, only the decode step underneath changed.
- `go.mod` gained `github.com/hamba/avro/v2` (pulls in `json-iterator/go`, `modern-go/*`,
  `go-viper/mapstructure/v2` as transitive deps); `web/vendor/` re-vendored (`go mod vendor`).
- New tests: `web/internal/schema/decoder_test.go` — round-trips a real Avro-encoded message
  through `httptest.Server` standing in for the registry (asserts field values + that the
  schema-id cache prevents a second HTTP fetch), plus missing-magic-byte / too-short /
  registry-unreachable error cases. `ingest_test.go` updated to use a `fakeDecoder` instead of
  raw JSON bytes. `go build ./... && go vet ./... && go test ./...` all green.

## Stack & shape (decisions)

- **Go**, single `main.go`: `net/http` (static + HTTP), `github.com/gorilla/websocket`
  (browser push), `github.com/twmb/franz-go` (Kafka consumer — chosen for **regex topic
  subscription** + consumer groups, pure Go), `github.com/jackc/pgx/v5` (postgres lookups).
- Browsers can't read Kafka directly, so: franz-go consumer → keep latest book per topic in
  memory → push to browser over **WebSocket**. `net/http` serves `public/index.html`.

## ID → display resolution (key consequence of the ID-only pipeline)

The Flink output carries only `pair_id` and per-level `exchange_id` — no `base`, `quote`, or
`exchange_name` (see [[event-identity-by-ids]]). So the server resolves them for display:
loads markets and `exchanges` (id → name/label) from postgres, refreshed every 10s, and
**enriches** each raw book before pushing. Since the 2026-06-29 normalization ([[db-schema]]),
the markets lookup joins `currencies` for the symbols:
`SELECT m.id, b.name, q.name FROM markets m JOIN currencies b ON m.base_id=b.id JOIN currencies q ON m.quote_id=q.id`
(was `SELECT id, base, quote FROM markets`). The enriched book sent to the
browser is `{ pair_id, base, quote, side, event_time, levels:[{price, quantity, exchange:{id,name,label}}] }`.
Unknown ids fall back to placeholders (`p{id}`/`?` for market, `unknown`/`نامشخص` for exchange).

## Files (refactored 2026-07-06 into hexagonal/clean architecture)

Originally everything lived in one `web/main.go`. Rewritten (behavior unchanged, verified via
unit tests + a manual boot smoke test) into `internal/` packages so the previously-untestable
logic (DB-merge-on-partial-failure, enrichment fallback, WS broadcast/prune, bad-message
handling) has real unit tests, without needing a live postgres/Kafka/websocket in tests.
`main.go` deliberately stays in `web/` root (not `cmd/`) so `go run .` / `go build .` / the
Dockerfile's build path need zero changes.

- `web/main.go` — composition root ONLY: reads env, builds the pgx pool, wires
  `postgres.Repository` → `registry.Registry` → `ingest.HandleRecord` → `hub.Hub`, starts the
  Kafka consumer goroutine + refresh ticker, serves HTTP. Not unit-tested (pure wiring).
- `web/internal/domain/` — plain structs only, no logic, no deps: `Market`, `Exchange` (display
  metadata), `RawBook`/`RawLevel` (Flink wire shape), `Book`/`Level` (enriched wire shape sent to
  browser), `WSSnapshot`/`WSUpdate` (the two WS message envelopes).
- `web/internal/ports/` — `MarketRepository` interface (`LoadMarkets`/`LoadExchanges` →
  `map[int]domain.Market` / `map[int]domain.Exchange`). Exists so `registry` doesn't depend on
  `*pgxpool.Pool` directly. Two separate methods (not one combined query) preserves the original
  "a failed exchanges load doesn't discard a successful markets load" behavior.
- `web/internal/registry/` — `Registry` (was the old `registry` struct): `Refresh(ctx)` merges via
  the `ports.MarketRepository` interface (only replaces a map if the load returned something —
  same transient-error tolerance as before), `Enrich(RawBook) Book` is the pure id→display
  resolution with the unknown-id placeholder fallback (`p{id}`/`?`, `unknown`/`نامشخص`).
  `registry_test.go`: partial-failure-preserves-data, unknown-id fallback, level order/fields —
  all via a `fakeRepo`, no DB.
- `web/internal/hub/` — `Hub` (was the old `hub` struct): add/remove/publish/`ServeWS`. Depends on
  a small `conn` interface (`WriteJSON`/`ReadMessage`/`Close`) instead of `*websocket.Conn`
  directly, satisfied implicitly by gorilla's real conn. `hub_test.go`: snapshot-on-add,
  broadcast-to-all, drop-client-on-write-failure, remove-closes-and-unregisters — all via a
  `fakeConn`, no real socket.
- `web/internal/ingest/` — new package: `HandleRecord(decoder, enricher, publisher, topic,
  value)` is the per-Kafka-record glue (decode → `Enrich` → `Publish`) that used to be inline in
  `consume`'s `EachRecord` callback. Takes three 1-method interfaces (`decoder`/`enricher`/
  `publisher`) narrowed from `*schema.Decoder`/`*registry.Registry`/`*hub.Hub` so its
  bad-message-is-skipped-not-fatal behavior has its own test independent of those packages'
  constructors. Decoding used to be inline `json.Unmarshal`; moved behind the `decoder`
  interface 2026-07-13 when the wire format became Avro — see "Wire format: JSON → Avro" above.
- `web/internal/schema/` — new package (2026-07-13): `Decoder` decodes Confluent-wire-format
  Avro records by resolving each record's writer schema from the registry by id (cached forever
  per id) and mapping into `domain.RawBook`. See "Wire format: JSON → Avro" above.
- `web/internal/kafka/` — `Consumer.Run(ctx, onRecord)`: thin franz-go adapter (fresh consumer
  group + earliest-offset reset, same as before), owns the poll loop, calls back per record. No
  branching logic of its own → not unit-tested (would only assert "franz-go was called").
- `web/internal/postgres/` — `Repository` implementing `ports.MarketRepository` via pgx; same two
  SQL queries as before, no branching logic → not unit-tested (the merge logic it feeds is tested
  in `registry`).
- `web/public/index.html` — single-page UI (vanilla JS), pair dropdown, asks/bids
  tables, spread, live WS updates with auto-reconnect. Connects to `ws://host/ws`.
  **Embedded into the binary** via `//go:embed public` (served with `fs.Sub` + `http.FS`).
  2026-07-08: `header` is `position: fixed` (stays pinned on scroll); `main` got
  `margin-top: 53px` to match the header's rendered height so content isn't hidden under it.
  2026-07-08: price/quantity columns now display uniform decimal places. `price`/`quantity`
  are wire strings (`domain.Level`, [[bigdecimal-rules]]-exact, not floats), so decimal count
  varies per level (e.g. "123", "45.6", "0.001"). `render()` computes `priceDecimals`/
  `qtyDecimals` as the max `decimalPlaces()` across ALL rendered levels (asks+bids combined,
  since visually it's one column) and `rows()` zero-pads every value to that width via
  `padDecimals()` (string-based, no `parseFloat`, so exactness is preserved). Recomputed on
  every `render()` call so it tracks the live snapshot.
- `web/go.mod`/`go.sum` — added `github.com/stretchr/testify` (test-only dep, user chose testify
  over stdlib `testing` for assertions); `web/vendor/` re-vendored (`go mod vendor`) so the
  Dockerfile's offline `-mod=vendor` build still works unmodified.
- `web/.gitignore` (ignores the compiled `/orderbook-web`), `web/README.md` (unchanged — run/build
  commands are still `go run .` / `go build -o orderbook-web .` from `web/`).

## Key implementation notes

- Subscribes via regex `^p[0-9]+-(asks|bids)$` (franz-go `ConsumeRegex`) → matches only the
  consolidated OUTPUT topics; input topics `ex{exchange_id}-p{pair_id}-{side}` carry a
  leading `ex…-` so they don't match. (Segment order flipped 2026-07-12 — see
  [[kafka-topic-strategy]].)
- WebSocket is served at `/ws`; the static file server is at `/`. (Node version upgraded at
  `/`; Go splits them, hence the one front-end change.)
- Fresh consumer group each start (`orderbook-web-<unixnano>`) + reset to earliest offset, so
  the current book renders on load. Replays history each restart — fine for dev only.
- New pairs added after server start are only picked up on restart (regex matched against
  topics existing at subscribe time).
- Hub broadcast holds a mutex while writing to all clients (gorilla forbids concurrent writes
  to one conn); simple and fine for a dev tool.
- Config via env: `PORT` (default 3000), `KAFKA_BROKER` (default `localhost:9092`),
  `DATABASE_URL` (default `postgres://postgres:postgres@localhost:5432/markets`),
  `SCHEMA_REGISTRY_URL` (default `http://localhost:8082`, added 2026-07-13).

## Docker

- `web/Dockerfile` — multi-stage: `golang:1.26-alpine` build → `alpine:3.22` runtime (runs as a
  non-root `app` user). Static `CGO_ENABLED=0` binary; embedded UI means nothing else is copied
  into the image (~32 MB).
- Added as the `web` service in `docker-compose-orderbook-consolidator.yml`: `build: ./web`,
  port `3000:3000`, `depends_on` kafka+postgres (service_healthy), env `KAFKA_BROKER=kafka:29092`,
  `DATABASE_URL=postgres://postgres:postgres@postgres:5432/markets`, and (2026-07-13)
  `SCHEMA_REGISTRY_URL=http://schema-registry:8082`, on `data-collector-net`.
- **Dependencies are vendored** (`web/vendor/`, committed) and the image builds with `-mod=vendor`
  so it works fully offline. This was necessary because this machine's BuildKit egress returns
  403 from `proxy.golang.org` for some module zips (the host's own network is fine). Re-run
  `go mod vendor` after changing deps. `web/.dockerignore` excludes the binary/README/Dockerfile
  but NOT `vendor/`.

## Run

`cd web && go run .` → http://localhost:3000  (or `go build -o orderbook-web . && ./orderbook-web`)
Docker: `docker compose up -d --build web`.
