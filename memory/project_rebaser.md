---
name: rebaser
description: M4 DONE 2026-07-18 — job-rebaser module (raw pipeline job 3): rebase exponents, the missing-row dead-letter decision, and the DB-mutating smoke
metadata:
    type: project
---

# Job 3 — rebaser (Milestone 4, done 2026-07-18)

`flink/normalizer/job-rebaser/` (package `io.tibobit.normalizer.rebase`,
[[normalizer-scaffold]] conventions). Consumes `ex[0-9]+-p[0-9]+-type-validated-raw-flink`
(job 2's valid output), shifts every level by the per-(exchange, pair) powers of ten held in
`exchange_markets`, emits to `ex{id}-p{id}-rebased-flink` (SAME `raw-order-book-event` subject).
9 module tests + live smoke 3/3 green.

## Decisions

- **Missing `exchange_markets` row → DEAD-LETTER `no_rebase_row`** (user decision 2026-07-18,
  the flag todo.md M4 left open). NOT drop, NOT passthrough. Passthrough was rejected because an
  un-rebased price is *silently corrupt* — orders of magnitude off, and nothing downstream can
  tell rebased from un-rebased; a visible dead-letter beats wrong numbers reaching the UI. Reuses
  job 2's `RejectedOrderBookEvent` + `ex{id}-p{id}-rejected-flink` topic, so the two jobs share
  one audit stream. In practice near-unreachable: job 1 resolved this event's `pair_id` from the
  same table, so only a refresh race (row deleted mid-flight) gets here.
- **This forced `ProcessFunction`, not the `RichMapFunction` todo.md sketched** — a map has no
  side output. Still stateless and **unkeyed**: one event's rebase never depends on another.
- **Lookup is keyed `{exchange_id}|{market_id}`**, not `{exchange_id}|{market}` like job 1's
  `ExchangeMarketsLoader` — job 3 only ever sees the resolved `pair_id`, never the exchange's
  market string. Hence a separate `RebaseFactorsLoader` (+ `RebaseFactors` holder) rather than
  reusing job 1's loader. Query filters `WHERE market_id IS NOT NULL` (nullable FK; `rs.getInt`
  would silently turn a null into key `ex|0`).
- **Null side stays null, empty stays empty.** ex3 wallex sends one side per message with the
  other null; collapsing null→empty would later read as "this side is empty, clear it". Job 5 is
  where the halves merge — job 3 must not disturb the distinction.
- Values are re-canonicalized on write (`Decimals.canonicalize` after `Decimals.rebase`), so
  `2.2 × 10⁻³` emits `0.0022`, never `2.2E-3`. Exact throughout — `scaleByPowerOfTen`, no double
  ([[bigdecimal-rules]]).

## Smoke (`smoke-rebaser.sh`) — the DB-mutation wrinkle

Follows the raw-in whole-chain **smoke rule** ([[normalizer-scaffold]]): raw OKX → `ex8-raw`,
all THREE jobs must be RUNNING, capture `ex8-p1-rebased-flink`, `ts = now` so the timing chain
is live. Verified chain now extends through job 3:
`event_time ≤ pair_extract_in ≤ pair_extract_out ≤ type_validate_in ≤ type_validate_out ≤
rebase_in ≤ rebase_out`.

**The wrinkle unique to this smoke:** the seeded `(8, p1)` row is `0 / 0` — identity — so it
proves nothing. The script therefore **temporarily UPDATEs `exchange_markets`** (price `+2`,
volume `−3`; both directions in one case), **sleeps past the `RefreshingLookup` interval**
(`REFRESH_INTERVAL_MS`, default 60s → `REFRESH_WAIT_S=65`) so the *already-running* job reloads,
then **restores the original values in an EXIT trap**. That wait is why this smoke is slower than
the others, and the running job keeps the test exponents for up to another refresh interval after
the script exits. Asserted arithmetic: `62770 → 6277000`, `2.2 → 0.0022`.

**The `no_rebase_row` path is NOT smoke-testable and that is not an oversight**: deleting the row
to trigger it also removes what job 1 uses to resolve `pair_id`, so job 1 drops the event and
nothing reaches job 3. Covered by `RebaseFunctionTest` instead. Third smoke case asserts the
complement — a job-2 reject arrives dead-lettered with `rebase_in == null`, proving job 3 never
saw it (asserting a presence beats asserting an absence).

**Why:** job 4 (precision) and job 5 (book build) assume amounts are already on the common scale;
a wrong or skipped rebase is invisible downstream because the numbers still look plausible.
**How to apply:** rebase exponents are DATA, not code — an exchange changing its scale is a DB
edit picked up by the refresh, never a code change. If a new job needs reference data keyed by
`pair_id`, copy `RebaseFactorsLoader`, don't extend job 1's market-string loader.
