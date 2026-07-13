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
- `exchange_markets(id, exchange_id → exchanges, market VARCHAR(100), market_id → markets, status subscription_status DEFAULT 'unsubscribe', price_amount_rebase INT NOT NULL DEFAULT 0, volume_amount_rebase INT NOT NULL DEFAULT 0, UNIQUE(exchange_id, market))`. `market` is the exchange-specific symbol string (e.g. `BTCUSDT`, `BTC_USDT`, `BTCTMN`). The two `*_rebase` columns drive job 3 of [[raw-pipeline-decision]] — exact rebase formula not confirmed yet (likely 10^n exponent, verify before use).
- enum `subscription_status` = `subscribe | unsubscribe | pending-subscribe | pending-unsubscribe`.

### Seed data (as of 2026-06-29)
- 30 currencies (ids 1–27 base assets BTC…BTT; 28=USDT, 29=IRT, 30=TMN).
- 3 exchanges: `1 nobitex` (نوبیتکس), `2 bitpin` (بیت پین), **`3 wallex` (والکس) — added 2026-06-29**.
- 81 markets: 27 base assets × {USDT, IRT, TMN}. All `price_precision=2, quantity_precision=8`.
- ~162 `exchange_markets` rows, **all seeded `status='unsubscribe'`** → [[kafka-topic-strategy]] warmup creates **zero** topics until rows are flipped to `subscribe`.
- Each INSERT block fixes explicit ids then `setval(pg_get_serial_sequence(...))` to realign the sequence.

### To resolve pair_id/exchange_id → display symbols
`SELECT m.id, b.name AS base, q.name AS quote FROM markets m JOIN currencies b ON m.base_id=b.id JOIN currencies q ON m.quote_id=q.id`.
This is the join [[orderbook-web]] now uses; before normalization it was `SELECT id, base, quote FROM markets`.

**Why:** Single source of truth for pairs/exchanges/subscriptions; drives [[kafka-topic-strategy]] topic provisioning and [[orderbook-web]] id→label enrichment.
**How to apply:** Any code reading pair symbols must join `currencies` via `markets.base_id`/`quote_id` — there is no `base`/`quote` column on `markets` anymore.
