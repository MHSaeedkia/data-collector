---
name: market-subscriptions
description: market-subscriptions/ is the Go operator console for turning feeds on/off — it writes ONLY pending-* statuses and NiFi settles them; supersedes the CSV CLI now kept in csv-bulk-sync/.
metadata:
    type: project
---

# market-subscriptions — the subscribe/unsubscribe console (2026-08-22)

`markets/` was renamed `market-subscriptions/` and grew a Go web service. The old CSV CLI
was **kept**, moved to `market-subscriptions/csv-bulk-sync/` (user decision — not deleted).

## The invariant the whole design rests on

**This service writes ONLY the pending statuses. NiFi settles them.** Subscribing a market
writes `pending-subscribe`, POSTs to NiFi, and stops. NiFi writes `subscribe` back to the
row when the feed is actually on. Confirmed by the user 2026-08-22 — nothing in the repo
records this, and it is not inferable from the schema, which merely allows all four
values.

Consequences worth knowing before changing anything here:

- **A pending row is a real state, not a UI artifact.** Rows stuck pending mean NiFi did
  not follow through; the bug is not in this service.
- **`domain.Action.Pending()` can never return a settled status**, and a test asserts
  exactly that. It is the guard against someone "helpfully" writing `subscribe` directly.
- **On a failed POST the pending write is rolled back** to the row's previous status,
  because a pending row NiFi was never told about would sit there forever. ⚠ The honest
  limit: a request that TIMED OUT may still have reached NiFi, so a rolled-back row can be
  settled by NiFi moments later. Accepted deliberately — stranding the row is worse.
- The status column is a postgres ENUM, so every write needs an explicit
  `$1::subscription_status` cast; pgx sends a Go string as text and postgres will not
  coerce it.

## Shape

Mirrors `web/` exactly ([[orderbook-web]]): Go module, hexagonal `internal/` packages,
UI in `public/` baked in with `go:embed`, vendored deps, multi-stage Dockerfile whose
build stage chains off the test stage so tests cannot be skipped. **One binary, no separate
frontend** — the user asked for this explicitly.

`internal/{config,domain,postgres,nifi,httpapi}`. `httpapi.Store` and `httpapi.Notifier`
are interfaces purely so the write path is testable without a database or a live NiFi.

## Config is entirely `.env`, by user requirement

Nine variables, all with defaults in `internal/config`, real env vars winning over the file
(that is how docker-compose configures it). **A bad value falls back to the default rather
than failing startup** — refusing to boot over a typo would remove the very console you use
to see what is going on. Both resolved NiFi URLs are logged at startup, because a wrong
`NIFI_BASE_URL` otherwise only shows up later as "NiFi is down".

## The two NiFi endpoints

`POST {NIFI_BASE_URL}{/subscribe|/unsubscribe}` with `{"exchange": name, "market": symbol}`,
judged purely on a 2xx — the exact contract `csv-bulk-sync/market-sync.sh` has always used,
including retrying a non-2xx. `exchange` is `exchanges.name`, `market` is
`exchange_markets.market` (the exchange's own symbol string, see [[db-schema]]).

⚠ **Name collision, still unresolved**: NiFi's `/control-plane` HTTP API here is unrelated
to the Kafka topic `control-plane` used by [[control-plane]]. Same words, different thing.

## Not run live

Built, vetted, 18 tests green, but **never run against a real postgres or NiFi** — docker
was down when it was written. The DB layer (`internal/postgres`) has no tests for the same
reason; everything above it is covered with fakes.
