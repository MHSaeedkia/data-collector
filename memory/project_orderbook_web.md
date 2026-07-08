---
name: orderbook-web
description: Standalone Go web app that consumes consolidated Kafka topics and renders a live order book in the browser
metadata:
  type: project
---

## Live order book web UI (`web/`)

Standalone **Go** app that consumes the Flink job's consolidated output topics
(`{side}-p{pair_id}`, e.g. `asks-p2`/`bids-p2`; see [[orderbook-aggregation]]) and shows a
live order book in the browser. NOT dockerized — runs on the host against the exposed broker
`localhost:9092`. Rewritten from the original Node.js app on 2026-06-28.

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
- `web/internal/ingest/` — new package: `HandleRecord(enricher, publisher, topic, value)` is the
  per-Kafka-record glue (unmarshal → `Enrich` → `Publish`) that used to be inline in `consume`'s
  `EachRecord` callback. Takes two 1-method interfaces (`enricher`/`publisher`) narrowed from
  `*registry.Registry`/`*hub.Hub` so its bad-JSON-is-skipped-not-fatal behavior has its own test
  independent of those packages' constructors.
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

- Subscribes via regex `^(asks|bids)-p\d+$` (franz-go `ConsumeRegex`) → matches only the
  consolidated OUTPUT topics; input topics `{side}-p{pair_id}-ex{exchange_id}` carry a
  trailing `-ex…` so they don't match.
- WebSocket is served at `/ws`; the static file server is at `/`. (Node version upgraded at
  `/`; Go splits them, hence the one front-end change.)
- Fresh consumer group each start (`orderbook-web-<unixnano>`) + reset to earliest offset, so
  the current book renders on load. Replays history each restart — fine for dev only.
- New pairs added after server start are only picked up on restart (regex matched against
  topics existing at subscribe time).
- Hub broadcast holds a mutex while writing to all clients (gorilla forbids concurrent writes
  to one conn); simple and fine for a dev tool.
- Config via env: `PORT` (default 3000), `KAFKA_BROKER` (default `localhost:9092`),
  `DATABASE_URL` (default `postgres://postgres:postgres@localhost:5432/markets`).

## Docker

- `web/Dockerfile` — multi-stage: `golang:1.26-alpine` build → `alpine:3.22` runtime (runs as a
  non-root `app` user). Static `CGO_ENABLED=0` binary; embedded UI means nothing else is copied
  into the image (~32 MB).
- Added as the `web` service in `docker-compose.yml`: `build: ./web`, port `3000:3000`,
  `depends_on` kafka+postgres (service_healthy), env `KAFKA_BROKER=kafka:29092` and
  `DATABASE_URL=postgres://postgres:postgres@postgres:5432/markets`, on `data-collector-net`.
- **Dependencies are vendored** (`web/vendor/`, committed) and the image builds with `-mod=vendor`
  so it works fully offline. This was necessary because this machine's BuildKit egress returns
  403 from `proxy.golang.org` for some module zips (the host's own network is fine). Re-run
  `go mod vendor` after changing deps. `web/.dockerignore` excludes the binary/README/Dockerfile
  but NOT `vendor/`.

## Run

`cd web && go run .` → http://localhost:3000  (or `go build -o orderbook-web . && ./orderbook-web`)
Docker: `docker compose up -d --build web`.
