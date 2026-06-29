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

## Files

- `web/main.go` — `registry` (id→display maps, periodic refresh), `hub` (WS clients + latest
  book per topic), `consume` (franz-go poll loop), `main` (wiring). HTTP server starts FIRST
  and stays up even if Kafka/postgres are down (errors are logged, not fatal).
- `web/public/index.html` — single-page UI (vanilla JS), pair dropdown, asks/bids tables,
  spread, live WS updates with auto-reconnect. Connects to `ws://host/ws`. Essentially
  unchanged from the Node version because the server enriches books into the same shape
  (`base`/`quote`/`level.exchange.label`) the page already expected — only the WS URL changed.
  **Embedded into the binary** via `//go:embed public` (served with `fs.Sub` + `http.FS`), so
  the build is a single self-contained executable that runs from anywhere — no `public/` dir
  needed at runtime.
- `web/go.mod`/`go.sum`, `web/.gitignore` (ignores the compiled `/orderbook-web`), `web/README.md`.

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
