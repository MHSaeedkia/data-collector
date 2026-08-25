---
name: db-schema
description: Postgres `markets` DB schema — currencies/markets/exchanges/exchange_markets tables, normalized base/quote, seed data
metadata:
  type: project
---

## Postgres schema (`postgres/01_schema.sql` + `postgres/02_seed.sql`, db = `markets`)

(File split from the old single `postgres/init.sql` — noticed stale 2026-07-13.)
Normalized on 2026-06-29 (commit e240fe3): a `currencies` lookup table was added and
`markets.base`/`quote` (VARCHAR) were replaced by FKs `base_id`/`quote_id`.

### Tables
- `currencies(id BIGSERIAL pk, name VARCHAR(20) UNIQUE)` — asset symbols (BTC, USDT, IRT, …). **New.**
- `markets(id, base_id BIGINT → currencies, quote_id BIGINT → currencies, price_precision, quantity_precision, display_price_precision, display_quantity_precision, UNIQUE(base_id, quote_id))`. All four precision columns nullable INTEGER with `>= 0` CHECKs. Was `base`/`quote` VARCHAR before; **a pair's display symbols now require joining `currencies` twice** (base + quote).
- `exchanges(id, name VARCHAR(20) UNIQUE, label VARCHAR(20))`.
- `exchange_markets(id, exchange_id → exchanges, market VARCHAR(100), market_id → markets, status subscription_status DEFAULT 'unsubscribe', price_amount_rebase INT NOT NULL DEFAULT 0, volume_amount_rebase INT NOT NULL DEFAULT 0, depth_aggregation_key INT NULL, staleness_threshold_seconds INT NOT NULL DEFAULT 60, our_profit_percent NUMERIC(6,3) NOT NULL DEFAULT 0.1, slippage_percent NUMERIC(6,3) NOT NULL DEFAULT 1, UNIQUE(exchange_id, market))`. `market` is the exchange-specific symbol string (e.g. `BTCUSDT`, `BTC_USDT`, `BTCTMN`). The two `*_rebase` columns drive job 3 of [[raw-pipeline-decision]] — exact rebase formula not confirmed yet (likely 10^n exponent, verify before use). **`our_profit_percent`/`slippage_percent` added 2026-08-25 (user request) for [[project_adjustment]]'s `OurProfitFunction`/`SlippageFunction`** — plain percent values (`1` = 1%, matching the existing `BuySellCommissionFunction` constant style), `CHECK (... >= 0)`. Deliberately per-`(exchange_id, market_id)` not per-market: the user confirmed slippage genuinely varies by exchange for the same market (e.g. ex1/market1 = 1%, ex2/market1 = 2%), which rules out putting it on `markets` (would need a join + trigger to keep NOT NULL guaranteed, see the "why not a separate table" reasoning below). ⚠ `01_schema.sql` only applies to a fresh volume — an already-provisioned DB (local dev, server) needs the `ALTER TABLE` run by hand; same class of caveat as [[kafka-topic-strategy]]'s retention `--if-not-exists`. `02_seed.sql`'s ~351 `exchange_markets` rows were all seeded at the schema defaults (0.1/1) via sed, not hand-picked per row — any real per-exchange override is still a manual edit.

## Why not a separate `exchange_market_adjustments` table (considered, rejected 2026-08-25)

A join was considered so profit/slippage could live outside `exchange_markets`. Rejected because the
NOT NULL + default guarantee the user wanted is automatic with inline columns (Postgres enforces it
on every row that already exists) and NOT automatic with a separate table (a market could exist with
no matching adjustment row, needing a trigger or `RefreshingLookup` fallback logic to fake the same
guarantee). Revisit only if slippage needs to become 1-to-many (e.g. depth-tiered), which the user
confirmed is not the current target — flat percent per `(exchange_id, market_id)` is.
- enum `subscription_status` = `subscribe | unsubscribe | pending-subscribe | pending-unsubscribe`.

### Seed data (as of 2026-06-29) — ⚠ LOCAL SEED ONLY, server DB has diverged
- 30 currencies (ids 1–27 base assets BTC…BTT; 28=USDT, 29=IRT, 30=TMN).
- 3 exchanges: `1 nobitex` (نوبیتکس), `2 bitpin` (بیت پین), **`3 wallex` (والکس) — added 2026-06-29**.
- **Server DB (192.168.150.104, checked 2026-07-13) has 8 exchanges**: 1=nobitex, 2=bitpin,
  3=wallex, 4=ramzinex, 5=bitget, 6=bybit, 7=ompfinex, 8=okx — and subscribed exchange_markets
  rows per exchange (ex4/ex7 store NUMERIC market ids in `market`, e.g. `218`, `14`). The local
  `02_seed.sql` no longer reflects production. See `sample-raw-data.md` + [[raw-pipeline-decision]].
- 81 markets: 27 base assets × {USDT, IRT, TMN}. All `price_precision=2, quantity_precision=8`.
- ~162 `exchange_markets` rows, **all seeded `status='unsubscribe'`** → [[kafka-topic-strategy]] warmup creates **zero** topics until rows are flipped to `subscribe`.
- Each INSERT block fixes explicit ids then `setval(pg_get_serial_sequence(...))` to realign the sequence.

### To resolve pair_id/exchange_id → display symbols
`SELECT m.id, b.name AS base, q.name AS quote FROM markets m JOIN currencies b ON m.base_id=b.id JOIN currencies q ON m.quote_id=q.id`.
This is the join [[orderbook-web]] now uses; before normalization it was `SELECT id, base, quote FROM markets`.

**Why:** Single source of truth for pairs/exchanges/subscriptions; drives [[kafka-topic-strategy]] topic provisioning and [[orderbook-web]] id→label enrichment.
**How to apply:** Any code reading pair symbols must join `currencies` via `markets.base_id`/`quote_id` — there is no `base`/`quote` column on `markets` anymore.

## Suspected seed bug — the `1K_SHIB*` rebase rows (noticed 2026-08-01, NOT fixed)

`exchange_markets` expresses an exchange's scaled-unit quoting through the rebase exponents, and
the `1M_PEPE*` rows do it correctly: nobitex `1M_PEPEUSDT` is `(-6, +6)` and `1M_PEPEIRT` is
`(-7, +6)` — a million-unit price/volume shift, plus one more on the price for IRR→toman. By that
rule the `1K_` rows should carry a thousand-unit shift, but nobitex `1K_SHIBUSDT` is `(0, 0)` and
`1K_SHIBIRT` is `(-1, 0)` — the toman shift is there, the 1000× is missing. Same for `1M_BTTUSDT`
`(0, 0)` / `1M_BTTIRT` `(-1, 0)`. If those feeds are live, their prices are off by 10^3/10^6 and
their quantities by the same. Raised with the user; left alone pending their call, since it is
production reference data and the local seed is already known to be stale against the server.
