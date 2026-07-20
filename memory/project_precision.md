---
name: precision
description: M5 DONE 2026-07-18, collision-merge fix 2026-07-20 — job-precision module (raw pipeline job 4): DOWN-truncation, truncate-to-zero, merging prices that collide after truncation, and why it has no dead-letter
metadata:
    type: project
---

# Job 4 — precision (Milestone 5, done 2026-07-18)

`flink/normalizer/job-precision/` (package `io.tibobit.normalizer.precision`,
[[normalizer-scaffold]] conventions). Consumes `ex[0-9]+-p[0-9]+-rebased-flink` (job 3's output),
truncates every level to the per-pair decimal places in `markets`, emits to
`ex{id}-p{id}-applied-precision-flink` (SAME `raw-order-book-event` subject). 21 module tests
green (15 at M5 + 6 for the 2026-07-20 collision-merge fix); live smoke was 3/3 at M5 and has
NOT been re-run since the merge fix.

## Decisions

- **Nonzero quantity truncating to exactly 0 → EMIT `"0"`, keep the level** (user decision
  2026-07-18; this REVISES an earlier same-day decision to drop the level — the drop is gone from
  the code, don't reintroduce it). Job 5 reads `quantity == 0` as *delete this level*, and that
  is the intended consequence here: a size below the market's `quantity_precision` is not
  representable liquidity, so the book should not carry a level for it. The rejected alternatives
  were **dropping** (which makes a dust level and a never-sent level indistinguishable, and leaves
  a stale resting level in the book forever) and **flooring to 1 ulp** (the only option that
  reports *more* size than reality, contradicting DOWN-truncation everywhere else).
- **Colliding prices are MERGED, quantities SUMMED** (user decision 2026-07-20 — this FIXES a
  bug; the earlier "pure per-level transform, level count in == level count out" invariant is
  GONE, don't restore it). Truncating prices makes distinct wire prices collide: at
  `price_precision 2`, `1.234` and `1.235` are both `1.23`. The old code emitted both, so a side
  carried the same price twice, and job 5 — whose `MapState` is keyed by canonicalized price —
  kept only the last, **silently erasing the other level's liquidity**. `applyLevels` now groups
  by truncated price in a `LinkedHashMap` (first-appearance order preserved) and sums.
  - **Sum the RAW quantities, truncate the sum ONCE** — not truncate-then-sum. Loses the least
    and stays DOWN-biased, so it still never claims more size than the exchange sent. Real
    consequence: several dust quantities that would each become `"0"` can now add up to a
    representable level and survive, which is more correct, not less.
  - **Same rule on `snapshot` and `update`** — the job never inspects `type`. On an update a
    quantity is an absolute *replacement*, not an increment, so summing two collided
    replacements is an approximation. It is unavoidable: this job is stateless and does not hold
    the untruncated book, so nothing here can know what the other collided price still rests at.
    Summing conserves the size the frame carried; last-wins discards it. Rejected: branching on
    `type` — two code paths, and the snapshot path would still be the only correct one.
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
- **Null side stays null, empty stays empty** — same ex3 half-book rule as job 3. A side's length
  can only ever SHRINK, and only via a collision merge: no level is dropped, but colliding ones
  are combined. An all-dust side with no collisions arrives with N levels and leaves with N
  levels all carrying `"0"`, which job 5 turns into N deletes.

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

Since 2026-07-20 each case also sends a THIRD ask colliding with the first, and asserts **3 asks
in → 2 out**. That length shrink is the merge assertion (before the fix this job could not
change a side's length at all). The expected quantity `0.2345679` is picked to discriminate the
two orderings: sum-raw-then-truncate gives `0.2345679`, truncate-each-then-sum gives
`0.23456789`, so a regression to the wrong order fails loudly instead of looking plausible.

**Why:** truncate-to-zero and collision-merge are the two places job 4 changes the *meaning* of an
event rather than just its digits — a dust update leaves this job as a delete instruction, and a
collided pair leaves it as a single summed level. Anyone touching job 5's delete handling needs
to know deletes arrive from here, not only from exchanges; anyone comparing level counts across
the chain needs to know job 4 can now emit fewer levels than it received.
**How to apply:** precisions are DATA, not code — a market changing its tick/lot size is a DB
edit picked up by the refresh, and widening `quantity_precision` immediately stops those deletes.
