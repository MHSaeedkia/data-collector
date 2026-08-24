# market-subscriptions

An operator console for turning market data feeds on and off.

It reads which markets exist from postgres (`exchange_markets` joined to `exchanges`) and
asks NiFi, over its two control-plane endpoints, to subscribe or unsubscribe them. One Go
binary with the UI embedded — there is no separate frontend to build or deploy.

Replaces the CSV-driven CLI, which is kept in [`csv-bulk-sync/`](csv-bulk-sync/).

---

## The one rule worth knowing

**This service never writes a settled status.** When you subscribe a market it writes
`pending-subscribe` and asks NiFi to do the work. **NiFi** writes `subscribe` back to the
row once the feed is really on. Same for unsubscribe.

So a row sitting in a pending state means *"NiFi has been asked and has not confirmed
yet"* — a real, observable state, not a UI spinner. If rows stay pending, look at NiFi,
not at this service.

The one exception is failure: if the POST to NiFi fails, the pending status is rolled back
to whatever the row held before, because a pending row NiFi was never told about would sit
there forever looking like work in progress. **A request that timed out may still have
reached NiFi**, so a rolled-back row can in principle be settled by NiFi a moment later —
that shows up on the next refresh.

---

## Running

```bash
cp .env.example .env      # then edit
go run .                  # or: docker compose up -d market-subscriptions
```

Open <http://localhost:8090>.

## Configuration

Everything is read from `.env`, or from real environment variables which always win
(that is how docker-compose configures it). Nothing is hardcoded — see
[`.env.example`](.env.example) for the full list with defaults:

| Variable | Default | Purpose |
|---|---|---|
| `PORT` | `8090` | HTTP port |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/markets` | where `exchange_markets` lives |
| `NIFI_BASE_URL` | `http://localhost:8081/control-plane` | NiFi's control-plane root |
| `NIFI_SUBSCRIBE_PATH` | `/subscribe` | appended to the base URL |
| `NIFI_UNSUBSCRIBE_PATH` | `/unsubscribe` | appended to the base URL |
| `NIFI_TIMEOUT` | `10s` | per-request timeout |
| `NIFI_MAX_RETRIES` | `3` | attempts before a market is reported failed |
| `NIFI_RETRY_DELAY` | `2s` | wait between attempts |
| `UI_REFRESH_SECONDS` | `10` | table auto-refresh; `0` disables |

Both resolved NiFi URLs are logged at startup, so a typo is visible immediately rather
than showing up later as "NiFi is down".

## The UI

- Every market with its current status, sorted by exchange then market.
- Filter by exchange, by status, or search; click any column header to re-sort.
- Tick individual markets, or **Select all shown** to act on everything the current
  filters leave visible — that is how you subscribe a whole exchange at once.
- **Subscribe** / **Unsubscribe** apply to the selection. Each market gets its own result,
  so one failure never blocks the rest, and failures are listed by name.

## API

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/subscriptions` | every market with its status |
| `GET` | `/api/exchanges` | exchange list for the filter |
| `POST` | `/api/actions` | `{"action":"subscribe","ids":[1,2]}` |
| `GET` | `/api/config` | UI settings derived from `.env` |
| `GET` | `/healthz` | pings postgres |

`POST /api/actions` returns one result per requested id:

```json
{"results":[{"id":1,"market":"bybit/BTCUSDT","status":"pending-subscribe","ok":true}]}
```

## What it touches

Only `exchange_markets.status`. It never inserts markets, never edits precision or rebase
columns, and never writes to Kafka. Adding a market is still a database concern.
