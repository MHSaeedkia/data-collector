---
name: orderbook-web
description: Standalone Go web app that consumes the aggregated, per-exchange and price-merged Kafka topics (Confluent Avro) and renders a live order book in the browser
metadata:
  type: project
---

## Live order book web UI (`web/`)

Standalone **Go** app that consumes the aggregator's output topics (`p{pair_id}-{side}`, e.g.
`p2-asks`/`p2-bids`; see [[orderbook-aggregation]]) — and, since 2026-08-10, job 5's
per-exchange books as well, and since 2026-08-24 the [[price-merger]]'s `-merged` topics —
and shows a live order book in the browser.

## Stack & shape (decisions)

- **Go**: `net/http` (static + HTTP), `github.com/gorilla/websocket` (browser push),
  `github.com/twmb/franz-go` (Kafka consumer — chosen for **regex topic subscription** +
  consumer groups, pure Go), `github.com/jackc/pgx/v5` (postgres lookups),
  `github.com/hamba/avro/v2` (Confluent-wire-format decode).
- Browsers can't read Kafka directly, so: franz-go consumer → keep latest book per topic in
  memory → push to browser over **WebSocket**. `net/http` serves `public/index.html`.

## Wire format: Confluent Avro (not JSON)

The output topics carry Confluent-wire-format Avro ([[aggregator]]).
`web/internal/schema` (`Decoder`, hamba/avro) parses the wire header (byte 0 = magic `0x0`, next
4 bytes = big-endian schema-registry id), resolves the writer schema via
`GET {SCHEMA_REGISTRY_URL}/schemas/ids/{id}`, and **caches it forever per id** (registry ids are
immutable — no TTL/invalidation needed). Decodes into a package-private `wireEvent`/`wireLevel`
pair mirroring `aggregated_order_book_event.avsc` exactly, then maps into `domain.RawBook`.

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

- Subscribes via regex `^p[0-9]+-(asks|bids)(-merged)?$` (franz-go `ConsumeRegex`) → matches the
  aggregated OUTPUT topics and the merger's (see the 2026-08-24 section); upstream per-exchange topics carry a leading `ex…-` so they don't
  match ([[kafka-topic-strategy]]). Topic strings are otherwise opaque to
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
- `web` service in `docker-compose.yml`: `build: ./web`, port `3000:3000`,
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

**2026-08-03 — `simulation` plumbed through** `wireLevel` → `domain.RawLevel` → `domain.Level` → the WebSocket JSON the browser receives. **Not rendered in the UI** — the data reaches the client, displaying it was not in scope. See [[simulation-flag]].

---

## 2026-08-10 — per-exchange view (job 5 alongside job 6)

The page now has an **exchange dropdown** next to the pair one: "All exchanges (aggregated)"
renders job 6's union exactly as before, any specific exchange renders that exchange's own book
for the same pair, straight from job 5's `ex{id}-p{id}-orderbook-snapshot-flink`
([[book-builder]]). One HTML page, both views — user requirement.

### The shape mismatch, and where it is absorbed

Job 6 emits **one record per side** with `exchange_id`/`simulation` per LEVEL. Job 5 emits
**one record holding BOTH sides** with `exchange_id`/`simulation` per RECORD. The whole
difference is absorbed in `internal/schema`, which now returns `[]domain.RawBook` — one book
for an aggregated record, two for a snapshot — and copies the snapshot's record-level
exchange/simulation down onto every level. Everything downstream (`Enrich`, hub, the WS
envelope, the render loop) therefore stayed on one shape. Getting this wrong the other way
(a second parallel book type all the way to the browser) is the expensive mistake here.

`Decode` dispatches on the writer schema's **Avro full name**
(`io.tibobit.orderbook.OrderBookSnapshot` vs `…AggregatedOrderBookEvent`), not on the topic
string — the name is what actually determines the payload shape, and it keeps topics opaque
past the consumer (the rule this app already followed). An unrecognised name is an error, not a
best-effort decode. The decoder's `wireSnapshot` deliberately omits `trigger_id`,
`last_sequence_id` and `pipeline_timings`: hamba skips schema fields with no struct
counterpart, and the tests marshal those fields with real (non-default) values specifically to
prove the skip decoders handle a populated union and a nested record.

**An empty side is still emitted.** A reset ([[type-validator]]) empties both sides of a job-5
book; dropping empty sides would leave the browser rendering a book that no longer exists.

`domain.Book.Exchange` is `*Exchange` — nil = aggregated. Exchange ids start at 1, so **0 means
"all exchanges"** on the wire in both directions (`RawBook.ExchangeID`, the browser's
`exchange_id`). The per-LEVEL exchange is still filled on a per-exchange book even though it is
constant there, so the table renders identically; the UI just hides the Exchange column when a
specific exchange is selected.

### Three user decisions (2026-08-10)

1. **Two Kafka consumers, different offsets.** Aggregated stays at **earliest** (the book paints
   on load — the original decision); the per-exchange family reads from **latest**, because
   those topics carry a full book on every event for every exchange × pair and replaying the 1h
   retention window at startup costs far more than it is worth. franz-go's
   `ConsumeResetOffset` is client-wide, which is the *only* reason there are two clients —
   `NewAggregatedConsumer` / `NewSnapshotConsumer`. Accepted trade: an idle exchange shows
   nothing until its next event.
2. **Server-side filtering.** The hub pushes each client only the pair+exchange it selected,
   instead of broadcasting everything: with a book per exchange on top of the aggregated one,
   broadcast-all would send each browser far more than it can display. The browser sends
   `{"type":"select","pair_id","exchange_id"}` on connect and on every dropdown change; the hub
   answers with a `snapshot` of what it already holds (so switching paints instantly) and then
   `update`s. This is why `ServeWS`'s read loop now carries meaning — it used to exist only to
   detect the close. The hub still holds every book in memory; only the fan-out is filtered.
3. **Dropdowns come from postgres, not from the data.** Server-side filtering *broke* the old
   mechanism — the client used to infer the pair list from the books it received, which it no
   longer gets. So a `catalog` message (every market and every exchange the registry knows,
   sorted by id) is sent on connect and re-sent only when it actually changed — it is recomputed
   on the existing 10s refresh tick and is almost always identical. The user chose **all**
   markets/exchanges rather than only subscribed ones, so the pair dropdown will list markets
   that have no topics at all.

### Consequences worth knowing

- The hub's `latest` map is keyed by `domain.Selection{pair, exchange, side}` derived from the
  book itself, **not by topic** — a job-5 record carries two sides on one topic, so the topic is
  no longer a unique key. `topic` survives only as context in the bad-message log line.
- The browser holds exactly **one book per side** now, not a map of everything; a book arriving
  for a selection that was just changed away from is dropped on receipt (in-flight guard).
- `store()`/`render()` key off `pair_id` + exchange id, not the `base/quote` string as before.
- Not yet run against live Kafka — Go tests (all packages green), `go vet`, `gofmt` clean, and a
  local run confirming the page serves and `/ws` upgrades and delivers the catalog frame.

---

## 2026-08-24 — merged view (the price merger's `-merged` topics)

Third view of the same pair, alongside job 6's union and job 5's per-exchange books: the
[[price-merger]]'s `p{id}-{side}-merged`, where equal prices are **summed** and a level names the
list of exchanges behind the sum.

### The `exchange_id` vocabulary — ONE ubiquitous language, no bare numbers

The exchange dropdown got a third entry, `All exchanges (merged)` (user's choice over a separate
Union/Merged toggle). Merged is a *view*, not an exchange, so it has no `exchange_id` — but
`Selection{pair, exchange}` already used 0 for the aggregated union, and real ids start at 1, so
merged takes the free negative slot. `Selection`, `Matches`, `WSSelect`, the hub and the browser's
select message are therefore **all unchanged**. The alternative (a `View` field on
Selection/Book/WSSelect + hub matching) is cleaner in the abstract and cost several times the code;
if a fourth view ever appears, that is the moment to switch.

An `exchange_id` is now **exactly one of three things**, in both directions on the wire, and each is
named — the user's requirement after the first cut left `-1` named and `0` a bare literal:

| value | constant | meaning |
| --- | --- | --- |
| `1`+ | (postgres ids) | a real exchange — job 5's own book |
| `0` | `domain.AggregatedExchangeID` | job 6's union: levels side by side |
| `-1` | `domain.MergedExchangeID` | the merger's sum: one level per price |

Declared in one const block at the top of `internal/domain/book.go` with the whole vocabulary in one
doc comment, and **mirrored in `public/index.html` as `AGGREGATED`/`MERGED`** (the JS cannot import
Go — if one side changes, the other has to change with it; that coupling is the price of the
sentinel design and is called out in both files). Nothing writes the bare numbers any more:
`registry.Enrich` tests `!= domain.AggregatedExchangeID`, the hub/registry/decoder tests assert on
the constants, and the browser's `showExchange` is an explicit
`=== AGGREGATED || === MERGED` rather than the `<= 0` trick the first cut used — the numeric
ordering is an accident of the encoding, not a fact about the views.

The flag that drives it is `Book.Merged` / `RawBook.Merged`, not the exchange field: `Book.Exchange`
is nil for the aggregated *and* the merged book, so nil alone can't tell them apart —
`Book.ExchangeID()` checks `Merged` first, and it is **the single place** a book is mapped onto the
vocabulary above (a producer never stamps `MergedExchangeID` into `RawBook.ExchangeID`). `hub.latest` is keyed by `ExchangeID()`, so getting this
wrong would silently overwrite the aggregated book with the merged one for the same pair+side.
Two hub tests pin exactly that.

### One consumer, not a third

`aggregatedPattern` widened to `^p[0-9]+-(asks|bids)(-merged)?$` rather than adding a client. The
merged topics carry one record per aggregated record — same rate, and **earliest** is the right
offset for both (the book must paint on load). This regex is the deliberate **mirror image** of
`MergerJob`'s: that one must *exclude* `-merged` or it consumes its own output forever, this one
wants both. Only `NewSnapshotConsumer` still needs its own client, and only because
`ConsumeResetOffset` is client-wide.

### The shape difference, and where it is absorbed

Same place as last time — `internal/schema`. `Decode` dispatches on the third Avro full name
`io.tibobit.orderbook.MergedOrderBookEvent`. A merged **level** has `exchange_ids[]`/`source_ids[]`
instead of the scalars, so `RawLevel` gained `ExchangeIDs`/`SourceIDs` (both `omitempty`, empty on
every other producer) and `domain.Level` gained `Exchanges []Exchange`. The lists are **not**
flattened to a first exchange: "who is behind this quantity?" is the only reason `exchange_ids`
exists on the schema at all. `wireMerged` omits the record-level `source_id` the way `wireSnapshot`
omits `trigger_id` — hamba skips schema fields with no struct counterpart, and the test marshals it
with a real value to prove the skip works.

`registry.Enrich` resolves the list for a merged book and **leaves the scalar `Level.Exchange` at
its zero value** — resolving the absent id 0 would stamp every merged row with the
`unknown`/`نامشخص` placeholder. The per-level placeholder fallback still applies inside the list, so
an id postgres doesn't know shows as `unknown` rather than dropping a contributor.

### Browser

`exchangeNames(l)` joins `l.exchanges` with `", "` (user's choice over a bare count), falling back
to `l.exchange.name`. The Exchange column now shows for `selectedExchange <= 0` (both cross-exchange
views, still hidden for a single exchange where it is one constant), with the header reading
`Exchanges` in the merged view. Levels render in the merger's order — asks ascending, bids
descending, live before simulated at one price — the same convention `render()` already assumes.

### Status

All Go tests green (5 new: 2 decoder, 2 registry, 2 hub), `go vet` + `gofmt` clean, `node --check`
on the extracted inline script, and a local run confirming the page serves with the third option. **NOT verified against live Kafka** — docker was
down, so no merged record has ever actually been decoded by this app, and neither had the merger
been run live when it was written. The first real test is a stack with the merger job submitted.
