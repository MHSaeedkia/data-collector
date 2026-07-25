---
name: manual-test-data
description: 2026-07-20 — BTC-USDT-only manual e2e test scenarios for the normalizer chain (17 independent scenarios, ex8+ex3+ex1+ex2); why independence needs a 3-job state reset and why the timestamp is shifted per scenario
metadata:
    type: project
---

# Manual test data (`flink/normalizer/manual-test-data/`, 2026-07-20)

Scenario payloads for **manual** e2e testing: produce to `ex{id}-raw`, run the whole 6-job
chain, verify in the order book web UI. Distinct from the parser fixtures in
`job-pair-extractor/src/test/resources/fixtures/` — those are canonical wire samples, these
exercise *behaviour* (acceptance, rejection, deletes, truncation, resync). 17 scenarios (01–06
ex8/okx, 07 ex3/wallex, **08–12 ex1/nobitex** added 2026-07-21, **13–17 ex2/bitpin** added
2026-07-25), `produce.sh` + `reset.sh` + a README with the expected outcome per message.

## ex1/nobitex scenarios (08–12, added 2026-07-21, see [[pair-extractor]]/[[type-validator]])

Exercise the REST-snapshot + WS-delta split. Wire symbol `BTCUSDT` → `market_id` 1. Because the
REST snapshot is null-seq (arms `baselinePending`) and the first WS update **adopts its offset**,
these reach a job-2 path ex8 structurally cannot: **08** rest→ws resync happy-path + a mid-stream
re-anchor (update offset jumps `1001`→`9000`, adopted without a gap check; dead-letter 0); **09**
WS update before any REST snapshot → `no_baseline` (1); **10** offset gap (`1005` after `1001`,
Centrifugo jump is exactly 1) → `sequence_gap` then a contiguous `1002` still → `awaiting_snapshot`,
REST resync recovers (2); **11** Centrifugo noise (connect reply, foreign channel, malformed book)
all discarded, interleaved to prove a drop ≠ a reject and doesn't disturb baseline adoption (0);
**12** (added 2026-07-21) an OLD REST snapshot (byte-identical replay) sent AFTER newer WS deltas →
`out_of_order` (1) — a null-seq snapshot has no offset so job 2 orders it by event time; a loud
`60000` bid wall in update `1001` makes the book leak visible if the guard regresses (see
[[type-validator]] out-of-order fix).
**produce.sh extended**: ex1 shifts `lastUpdate` (event time only — top-level on REST,
`push.pub.data.lastUpdate` on WS) onto now like ex8's `ts`, but **leaves offsets untouched** (the
sequence is the independent `push.pub.offset`; no 300 ms cadence). Same "not yet run live" caveat —
needs NiFi's REST feed on `ex1-raw` + the coupled job-2 resync build.

## ex2/bitpin scenarios (13–17, added 2026-07-25)

A deliberate one-for-one mirror of 08–12 (13↔08 resync+re-anchor, 14↔09 `no_baseline`, 15↔10 gap,
16↔11 noise, 17↔12 stale replay), because ex2 turned out to have ex1's exact regime — see
[[pair-extractor]]. Wire symbol **`BTC_USDT`** (underscore; `BTCUSDT` would drop silently as an
unknown market), channel prefix `orderbook:` (colon), *not* ex1's `public:orderbook-`. Bands are
62700/62900/63100/62800 with 2-decimal prices, so ex2 levels stay attributable if scenarios are
ever chained with ex1's integer-priced ones. Dead-letter oracle: 0/1/2/0/1, same as 08–12.

**Two things here are genuinely ex2-only and cost the thought:**

- **`event_time` has two wire types under one name** — epoch millis (number) on REST, ISO-8601
  string on WS. So `produce.sh`'s ex2 branch must shift *both* shapes by the same amount, in
  different units. Solved by putting every ex2 base on a **whole second** (`ET = 1800000000000` =
  `2027-01-15T08:00:00Z`, +1s steps): DELTA is then a whole-second multiple and the shift is exact
  as millis and as `fromdateiso8601`-seconds alike. Sub-second realism is lost (the real feed sends
  microseconds); acceptable — that precision is covered by the parser fixture, and jq cannot
  round-trip fractional ISO anyway. **Scenario 17 is the only test that proves the two units land
  on a common scale** — if they ever diverged, the stale replay would be accepted.
- **Scenario 16 file 05** is a REST snapshot whose `event_time` is a *string*: correct in every
  other respect, dropped at job 1. This is the live NiFi contract risk made runnable — if the real
  feed quotes the timestamp, every ex2 snapshot disappears with **no dead-letter record**, only a
  drop counter. It carries a `60000.00` bid wall so acceptance would be unmissable in the UI. It is
  the one ex2 file `produce.sh` deliberately does **not** shift (it's dropped anyway).

Every one of the 29 files was run through `BitpinParser` before the README's per-file expectations
were written (temporary test, since deleted), so the market/type/seq/jump/event-time column of that
README is parser-verified. The *pipeline* expectations carry the same "not run live" caveat as
08–12, plus the coupled-deploy trap (todo.md M12).

**Two user constraints, imposed in this order, and the second one reshaped the design:**

1. everything on **BTC-USDT / `pair_id` 1** (ex8 wire symbol `BTC-USDT`, ex3 `BTCUSDT`, both →
   `market_id` 1)
2. **every scenario must run standalone**, on fresh state, in any order

Constraint 1 alone had forced a single ordered timeline (shared pair ⇒ shared job state). An
intermediate revision shipped exactly that; constraint 2 then required undoing it. The tension
is real and is resolved by **resetting state in the tooling**, not by separating the data.

## How independence is actually achieved

Two halves, both required:

- **Data**: each scenario opens with its own baseline snapshot and its own self-contained `ts`
  timeline starting at its own `B+300`. Nothing references a book or sequence established
  elsewhere. (Scenarios 03 and 04 needed a leading snapshot added when they were split out.)
- **`reset.sh`**: cancels + resubmits the three **stateful** jobs before each run —
  `normalizer-aggregator`, `normalizer-book-builder`, `normalizer-type-validator`.
  pair-extractor/rebaser/precision are stateless, left alone.

The reset is the load-bearing half: scenario 01 tests the `no_baseline` branch, which is
unreproducible without a fresh [[type-validator]]. Scenario 07 step 1 ("no asks yet") is why
the reset must include [[book-builder]] and not just the validator.

Reset details that cost thought:
- **No checkpointing anywhere on this platform ⇒ cancel+resubmit IS the state reset.**
- **Downstream-first order** (aggregator → 5 → 2), same rule as `make refresh-normalizer`:
  sources read `latest`, so an upstream job restarted first emits into a topic nobody reads yet.
- Resubmits via the **already-uploaded jar** (`GET /jars` → newest by artifactId prefix →
  `POST /jars/{id}/run`), so no Maven rebuild; jobs must have been submitted once before.
  ~20–30s per reset. Job names come from `env.execute(...)`, jar prefixes from the artifactIds.
- A `SETTLE` sleep after RUNNING before producing, so sources are assigned first.

`produce.sh` takes exactly one scenario (no "all" mode — that would defeat the point) and calls
`reset.sh` unless `--no-reset`.

## `produce.sh` shifts ex8 `ts` per scenario

For okx `ts` is simultaneously sequence id, event time, and the only timestamp
([[pair-extractor]] `OkxParser` stamps event_time = ts). Files carry synthetic base
`1800000000000`, which is in the **FUTURE** vs real time (2026 ≈ 1.784e12). Sending verbatim is
actively harmful, not just odd: the aggregator drops events older than stored, so a
future-dated book poisons the stored timestamp and every subsequent *real* event is dropped
until wall-clock catches up.

The shift is now computed **per scenario** (min ts in that dir → now, aligned to the 300 ms
grid). It was briefly global-across-the-tree during the single-timeline revision; per-scenario
is correct again now that scenarios are independent. Verified that the deliberate violations
survive the shift. ex3 is never rewritten (no ordering field; job 1 stamps processing time).

jq detail: the shift filter is guarded by `if (.data? | type) == "array"` so noise frames
without `.data` pass through; that guard is also why the filter is ex8-only — ex3 payloads are
top-level JSON **arrays**, where `.data` errors outright.

## Coverage map + per-scenario acceptance oracle

Each scenario documents its expected dead-letter count, which is the check that does not depend
on reading the UI:

01 update-before-snapshot → **1** (`no_baseline`); 02 happy path incl. an **asks-only** update
(null side must not wipe bids) → **0**; 03 gap → `sequence_gap` → `awaiting_snapshot` (a
*correctly*-stepped update still rejected while diverged) → resync into a new price band → **2**;
04 byte-identical replay AND a backwards snapshot, loud `69999`/`10000` levels so a leak is
visible, neither rejection advancing `lastSeq` → **2**; 05 [[precision]] truncation incl. **prices
colliding into one book price and MERGING by sum** (extended 2026-07-20 with a bid-side collision,
an update-frame collision, and a pair of dusts that survive because their raw sum is
representable) plus dust→`"0"`→delete with a control add → **0** (job 4 has no dead-letter by
design); 06 non-book frames discarded, interleaved between snapshot
and update to prove drops don't disturb sequence tracking → **0** (**drop ≠ dead-letter**); 07
wallex null-seq passthrough + half-book merge → **0**.

Market 1 is `price_precision 2 / quantity_precision 8`, rebase `0/0` for both exchanges, so
**wire prices equal UI prices** — expectations are eyeball-checkable without rebase math.

Scenario 07's wallex prices all end in `.5` and interleave with (never collide with) ex8's
integer bands, so levels stay attributable if scenarios are ever chained with `--no-reset`
(the aggregator unions ex8 + ex3 on p1). Synthetic on purpose.

## Status / known gap

Files, JSON validity, per-scenario ts shifts and both scripts' syntax are **verified locally**;
the pipeline expectations and `reset.sh` itself have **NOT** been run against a live stack. In
particular the Flink REST reset flow (jar lookup by prefix, resubmit, RUNNING wait) is written
from the API shape used by `run-job.sh` and is unproven. First run is the real verification.

A non-JSON frame (bare `pong`) is **not** covered — whether job 1 drops or throws on unparseable
bytes is unverified, and a manual-test file that might wedge the job was a bad trade. Tracked in
todo.md.
