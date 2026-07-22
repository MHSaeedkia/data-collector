---
name: book-builder
description: M6 DONE 2026-07-18 — job-book-builder module (raw pipeline job 5): keyed book state, the ex3 null-side merge, and why quantity 0 is a delete regardless of event type
metadata:
    type: project
---

# Job 5 — book builder (Milestone 6, done 2026-07-18)

`flink/normalizer/job-book-builder/` (package `io.tibobit.normalizer.bookbuild`,
[[normalizer-scaffold]] conventions). Consumes `ex[0-9]+-p[0-9]+-applied-precision-flink`
(job 4's output), maintains the live book per `(exchange_id, pair_id)` in keyed `MapState`, emits
the WHOLE book as an `OrderBookSnapshot` on every event → `ex{id}-p{id}-orderbook-snapshot-flink`
(subject `order-book-snapshot`). 15 module tests + live smoke 3/3 green.

## Decisions

- **Snapshot vs update differ ONLY by "clear the side first".** Both then run the same per-level
  rule, so `quantity == 0` means "no level here" in a snapshot exactly as in an update. This is
  the simplification that made the whole function ~40 lines: there is no separate snapshot path.
  It also closes the hazard flagged when [[precision]] was decided — a *snapshot* containing a
  dust level cannot leave a zero-quantity level resting in the book.
- **A quantity-0 delete does NOT imply the exchange sent a delete.** Job 4 emits `"0"` for any
  nonzero size that truncates away at `markets.quantity_precision`, so deletes also originate
  inside our own pipeline. Recorded as a comment on the delete branch itself, because the
  failure mode is a future debugger chasing a delete that appears nowhere in the raw feed.
- **`null` side ≠ empty side — this is THE wallex (ex3) merge point.** ex3 sends a full snapshot
  one side per message with the other null. "Replace wholesale" therefore means *replace only the
  sides actually present*: null leaves that side's state untouched, `[]` is a real report of "no
  liquidity this side" and clears it. Decided against doing this in job 1 ([[pair-extractor]]).
- **Prices are canonicalized before being used as MapState keys** (`stripTrailingZeros`), because
  MapState is hash-based and would otherwise hold "10.50" and "10.5" as two levels of the same
  price — the consolidator lesson ([[bigdecimal-rules]]).
- **Sides are sorted on the way out** (asks ascending, bids descending — the platform convention
  from [[orderbook-aggregation]]). MapState iteration order is undefined, so without this the
  emitted book would be nondeterministic; that costs nothing to fix here and makes both the smoke
  and any human reading the topic sane. Job 6 diffs by price and does not depend on the order.
- **No sequence re-validation and no dead-letter.** Job 2 already enforced the rules and the
  topics are single-partition, so ordering holds; anything arriving here is by definition valid.
  `last_sequence_id` is just the event's `sequence_id` passed through — no extra state — and stays
  null for feeds with no ordering field (ex3).
- **Cold start is unsolved, deliberately.** No checkpointing is configured anywhere on this
  platform, so after a restart a book is empty until the next snapshot re-seeds it. Same known
  gap as the old merger; recorded, not fixed (it is a platform-wide conversation — todo.md).

## Reset branch (added 2026-07-21, plans/aggregator-gap-drop.md Part B)

`type == "reset"` (job 2's gap marker, see [[type-validator]]) → `asks.clear()` + `bids.clear()`,
then falls through to the SAME emit path, so the emitted `OrderBookSnapshot` comes out empty. Reuses
the existing sorted-emit rather than a bespoke empty-book construction (an emptied `MapState` sorts to
empty lists). Without this branch a reset would carry null sides → `applySide` leaves state untouched
→ the stale book would be re-emitted, which is exactly the bug being fixed. The emptied book makes the
exchange drop out of the downstream aggregator. Matched by `resetEmptiesBook`. **Not run live.**

## Gotchas

- **The registered `order-book-snapshot` subject was stale (v1, no `pipeline_timings`)** and had
  to be re-registered from `schemas/order_book_snapshot.avsc` before the sink would work — the
  exact same trap that bit jobs 1 and 2 ([[pair-extractor]], [[type-validator]]). Any *new* output
  subject in this pipeline should be assumed stale until checked.

## Smoke (`smoke-book-builder.sh`)

Follows the raw-in whole-chain **smoke rule** ([[normalizer-scaffold]]): raw OKX → `ex8-raw`, all
FIVE jobs must be RUNNING, capture `ex8-p1-orderbook-snapshot-flink`, `ts = now`. The three cases
run in order against ONE accumulating book — that ordering is the test, since job 5 is the first
job whose *state* is the deliverable: snapshot seeds → update merges → quantity 0 deletes. Case 1
carries a dust ask and asserts the book comes out with ONE ask, which is the truncate-to-zero
decision verified end to end across two jobs. Case 2's update reports `bids: []` and asserts the
snapshot's bid survives — the merge-not-replace guarantee.

The **ex3 null-side merge is NOT reachable from this smoke** (OKX always sends both sides); it is
covered by the module test `nullSideKeepsOtherSideState`.

**Why:** job 5 is where the pipeline stops being a per-event transform chain and starts holding
truth — every earlier job could be reasoned about one message at a time, this one cannot.
**How to apply:** anything that looks wrong in the emitted book is either a wrong *event* (look
upstream, the timings say which stage) or wrong *accumulated state* (look at ordering and at
whether a null side was mistaken for an empty one — those are the only two ways this job loses
information).
