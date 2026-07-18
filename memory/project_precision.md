---
name: precision
description: M5 DONE 2026-07-18 — job-precision module (raw pipeline job 4): DOWN-truncation, the truncate-to-zero DROP decision, and why it has no dead-letter
metadata:
    type: project
---

# Job 4 — precision (Milestone 5, done 2026-07-18)

`flink/normalizer/job-precision/` (package `io.tibobit.normalizer.precision`,
[[normalizer-scaffold]] conventions). Consumes `ex[0-9]+-p[0-9]+-rebased-flink` (job 3's output),
truncates every level to the per-pair decimal places in `markets`, emits to
`ex{id}-p{id}-applied-precision-flink` (SAME `raw-order-book-event` subject). 15 module tests +
live smoke 3/3 green.

## Decisions

- **Nonzero quantity truncating to exactly 0 → EMIT `"0"`, keep the level** (user decision
  2026-07-18; this REVISES an earlier same-day decision to drop the level — the drop is gone from
  the code, don't reintroduce it). Job 5 reads `quantity == 0` as *delete this level*, and that
  is the intended consequence here: a size below the market's `quantity_precision` is not
  representable liquidity, so the book should not carry a level for it. The rejected alternatives
  were **dropping** (which makes a dust level and a never-sent level indistinguishable, and leaves
  a stale resting level in the book forever) and **flooring to 1 ulp** (the only option that
  reports *more* size than reality, contradicting DOWN-truncation everywhere else).
  Consequence: `applyLevels` is a pure per-level transform — **level count in == level count
  out** — which is what keeps the whole job a plain `RichMapFunction`.
- **Truncation is always DOWN** (`Decimals.truncate` = `setScale(p, DOWN)`). An order book must
  never claim a better price or more size than the exchange reported; rounding up invents
  liquidity. Exact throughout, no double ([[bigdecimal-rules]]).
- **No dead-letter at all — a missing/NULL precision is a PASSTHROUGH.** This is deliberately
  the opposite of job 3 ([[rebaser]]), and the asymmetry is the point: an un-rebased amount is
  *silently corrupt* (orders of magnitude off, indistinguishable downstream), whereas an
  un-truncated amount is merely more precise than asked for — still correct, nothing to
  quarantine. Both precision columns are nullable, and a null means "not configured", so
  `MarketPrecisionLoader` uses `rs.getObject`, never `rs.getInt` (which turns null into 0 and
  would silently truncate everything to whole numbers).
- **Stateless `RichMapFunction`, unkeyed** — matches the todo.md sketch, unlike job 3 which the
  dead-letter decision forced into a `ProcessFunction`. Dropping a level is just a shorter
  output list, so no side output is needed.
- **Lookup is keyed by `pair_id` alone** (`RefreshingLookup<Integer, MarketPrecision>` over
  `markets`), not by `(exchange, pair)` like job 3 — precision is a property of the market
  itself, not of the exchange reporting it.
- **Null side stays null, empty stays empty** — same ex3 half-book rule as job 3. Since no level
  is ever dropped, a side's length never changes: an all-dust side arrives with N levels and
  leaves with N levels all carrying `"0"`, which job 5 turns into N deletes.

## Smoke (`smoke-precision.sh`)

Follows the raw-in whole-chain **smoke rule** ([[normalizer-scaffold]]): raw OKX → `ex8-raw`,
all FOUR jobs must be RUNNING, capture `ex8-p1-applied-precision-flink`, `ts = now` so the
timing chain is live. Verified chain now extends through job 4:
`… ≤ rebase_in ≤ rebase_out ≤ precision_in ≤ precision_out`.

**Unlike `smoke-rebaser.sh` it does NOT mutate the DB.** The reference-data rule (make the data
non-trivial) is already satisfied by the seed: `markets` p1 is `price_precision=2,
quantity_precision=8`, so feeding values with more decimals exercises real truncation. It
*asserts* that seed as a precondition, and also asserts the rebase row is still `0/0` so job 3
stays a no-op and every digit change is job 4's doing — that check doubles as a guard against
running while a `smoke-rebaser.sh` run is still in flight.

The dust path IS smoke-tested here (a dust ask rides alongside a real one; both come out, the
dust one with quantity `"0"`) — unlike job 3's `no_rebase_row`, which is unreachable from raw.

**Why:** the truncate-to-zero rule is the one place job 4 changes the *meaning* of an event
rather than just its digits — a dust update leaves this job as a delete instruction. Anyone
touching job 5's delete handling needs to know deletes arrive from here, not only from exchanges.
**How to apply:** precisions are DATA, not code — a market changing its tick/lot size is a DB
edit picked up by the refresh, and widening `quantity_precision` immediately stops those deletes.
