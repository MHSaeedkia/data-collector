# Order Book Web UI

Live viewer for two families of order book topic produced by the Flink pipeline:

- **aggregated** (job 6) — `p{pair_id}-{side}`, e.g. `p2-asks`, `p2-bids`: the union across
  every exchange, one record per side.
- **per-exchange** (job 5) — `ex{exchange_id}-p{pair_id}-orderbook-snapshot-flink`: one
  exchange's full book, **both sides in one record**.

A small Go server consumes both, resolves the IDs to human-readable labels from postgres,
keeps the latest book per (pair, exchange, side), and pushes updates to the browser over
WebSocket. The page has a pair dropdown and an exchange dropdown: with **All exchanges
(aggregated)** selected it renders the job-6 union, and picking a specific exchange renders
that exchange's own book for the same pair.

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
`DATABASE_URL=postgres://postgres:postgres@postgres:5432/markets`,
`SCHEMA_REGISTRY_URL=http://schema-registry:8082`) and is exposed on http://localhost:3000.

The image is a multi-stage build (golang-alpine → distroless static). Dependencies are
**vendored** (`web/vendor/`, committed) so the image builds fully offline without the Go
module proxy; run `go mod vendor` after changing dependencies.

## Config (env vars)

- `PORT` — HTTP port (default `3000`)
- `KAFKA_BROKER` — broker address (default `localhost:9092`, the host-exposed listener)
- `DATABASE_URL` — postgres DSN (default `postgres://postgres:postgres@localhost:5432/markets`)
- `SCHEMA_REGISTRY_URL` — Confluent Schema Registry URL (default `http://localhost:8082`, the
  host-exposed listener), used to resolve each record's Avro writer schema by id

## Notes

- The Flink output carries only IDs (`pair_id`, and `exchange_id` per level) — no `base`,
  `quote`, or `exchange_name`. The server resolves them for display by loading the `markets`
  and `exchanges` tables from postgres (refreshed every 10s), then enriches each book before
  pushing it to the browser. Unknown ids fall back to placeholders.
- Record values are Confluent-wire-format Avro (magic byte + schema-registry id + Avro binary),
  matching the Flink sinks — **not JSON**. `internal/schema.Decoder` resolves each record's
  writer schema from the registry by id (schema-registry ids are immutable, so schemas are
  cached forever once fetched) and decodes into the internal `domain.RawBook` shape. It picks
  the payload shape from the schema's **full name**, not the topic: an
  `AggregatedOrderBookEvent` yields one book, an `OrderBookSnapshot` yields two (one per side)
  with its record-level `exchange_id`/`simulation` copied onto every level, so everything past
  the decoder sees a single shape. Malformed/undecodable records are logged and skipped.
- **Two Kafka consumers, both from the LATEST offset — live records only, never history.**
  The aggregated consumer subscribes `^p\d+-(asks|bids)(-merged)?$`, the per-exchange one
  `^ex\d+-p\d+-orderbook-snapshot-flink$`. These topics carry a full book on every event, so
  replaying their retention window at startup costs far more than it is worth: the aggregated
  consumer used to start at the earliest offset (so the book painted on load) and with the 6h
  retention `warmup.sh` sets, every restart replayed six hours of order books straight at the
  browser — which is what used to freeze the server (see the websocket note below). The
  trade-off is that a quiet pair or exchange shows nothing until its next event; the `snapshot`
  reply covers everything that has arrived since the process started. Both use a fresh consumer
  group each start, which is what makes "latest" mean latest — a stable group would resume from
  committed offsets and replay the backlog again.
- **The server pushes each browser only the pair+exchange it selected.** The browser sends
  `{"type":"select","pair_id":N,"exchange_id":N}` (exchange `0` = aggregated) on connect and on
  every dropdown change; the server answers with a `snapshot` of what it holds and then
  `update`s as records arrive. Both dropdowns are filled from a `catalog` message (every market
  and exchange in postgres, re-sent only when it changes) — with server-side filtering the
  client can no longer infer the lists from the data it receives.
- **Websocket writes never happen under the hub's lock.** Each client has its own writer
  goroutine and a small outbound queue that *coalesces* — every message carries a complete book
  or a complete catalog, so a newer one replaces the older one for the same slot instead of
  queueing behind it. A browser that falls behind therefore skips frames and stays correct,
  rather than backing the socket up. Writes carry a deadline and idle clients are pinged, so a
  peer that has silently gone away is dropped instead of being written to forever. Before this,
  one stalled socket blocked `Publish` under the shared mutex and froze the whole server: no
  updates for anyone, and new browsers got the websocket handshake followed by silence.
- New pairs created after the server starts are picked up on restart (the regex is matched
  against topics that exist at subscribe time).
- The UI starts and serves immediately even if Kafka or postgres is unreachable; it logs and
  keeps running.

## Stack

- `net/http` + `embed` — HTTP server serving the UI baked into the binary (`go:embed public`)
- [`github.com/gorilla/websocket`](https://github.com/gorilla/websocket) — browser push
- [`github.com/twmb/franz-go`](https://github.com/twmb/franz-go) — Kafka consumer (regex topics)
- [`github.com/hamba/avro/v2`](https://github.com/hamba/avro) — Avro decoding (schema fetched
  from the registry per record, Confluent wire format)
- [`github.com/jackc/pgx/v5`](https://github.com/jackc/pgx) — postgres lookups
