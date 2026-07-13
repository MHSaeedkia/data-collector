# Order Book Web UI

Live viewer for the consolidated order book topics produced by the Flink job
(`p{pair_id}-{side}`, e.g. `p2-asks`, `p2-bids`).

A small Go server consumes those topics, resolves the IDs to human-readable labels from
postgres, keeps the latest book per topic, and pushes updates to the browser over WebSocket.
The page has a pair dropdown and renders asks/bids live.

## Run

```bash
cd web
go run .
```

Then open http://localhost:3000.

To build a **single self-contained binary** instead — the UI (`public/`) is embedded via
`go:embed`, so the binary runs from anywhere with no `public/` directory at runtime:

```bash
go build -o orderbook-web .
./orderbook-web
```

## Docker

Built and run as the `web` service in the repo's `docker-compose.yml`:

```bash
docker compose up -d --build web
```

It reaches the other services over the compose network (`KAFKA_BROKER=kafka:29092`,
`DATABASE_URL=postgres://postgres:postgres@postgres:5432/markets`) and is exposed on
http://localhost:3000.

The image is a multi-stage build (golang-alpine → distroless static). Dependencies are
**vendored** (`web/vendor/`, committed) so the image builds fully offline without the Go
module proxy; run `go mod vendor` after changing dependencies.

## Config (env vars)

- `PORT` — HTTP port (default `3000`)
- `KAFKA_BROKER` — broker address (default `localhost:9092`, the host-exposed listener)
- `DATABASE_URL` — postgres DSN (default `postgres://postgres:postgres@localhost:5432/markets`)

## Notes

- The Flink output carries only IDs (`pair_id`, and `exchange_id` per level) — no `base`,
  `quote`, or `exchange_name`. The server resolves them for display by loading the `markets`
  and `exchanges` tables from postgres (refreshed every 10s), then enriches each book before
  pushing it to the browser. Unknown ids fall back to placeholders.
- Subscribes via regex `^p\d+-(asks|bids)$`, so it reads only the consolidated **output**
  topics — input topics (`ex{exchange_id}-p{pair_id}-{side}`) carry a leading `ex…-` and
  don't match.
- Uses a fresh consumer group each start and resets to the earliest offset, so the current
  book shows on load. Fine for dev; for high-volume topics this replays history on each restart.
- New pairs created after the server starts are picked up on restart (the regex is matched
  against topics that exist at subscribe time).
- The UI starts and serves immediately even if Kafka or postgres is unreachable; it logs and
  keeps running.

## Stack

- `net/http` + `embed` — HTTP server serving the UI baked into the binary (`go:embed public`)
- [`github.com/gorilla/websocket`](https://github.com/gorilla/websocket) — browser push
- [`github.com/twmb/franz-go`](https://github.com/twmb/franz-go) — Kafka consumer (regex topics)
- [`github.com/jackc/pgx/v5`](https://github.com/jackc/pgx) — postgres lookups
