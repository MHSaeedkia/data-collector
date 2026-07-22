---
name: level-emitter
description: M7 DONE 2026-07-18 — job-level-emitter module (raw pipeline job 6, the last one): the full-book → changed-levels diff, and why it holds a second copy of the book
metadata:
    type: project
---

# Job 6 — level emitter (Milestone 7, done 2026-07-18)

> **DEPRECATED 2026-07-22 (Part D):** job 6 is retired. Its role (feeding the consolidator
> per-level deltas) is gone — the terminal [[aggregator]] consumes job 5's full books directly.
> The module was moved to `flink/normalizer/DEPRECATED-job-level-emitter/` and dropped from the
> normalizer reactor (`flink/normalizer/pom.xml`), so it no longer builds into the Flink image and
> `run-job.sh`/`refresh-normalizer` no longer submit it. Code kept for reference; the rest of this
> note describes it as it was. Paths below still say `flink/normalizer/job-level-emitter/`.

`flink/normalizer/job-level-emitter/` (package `io.tibobit.normalizer.levelemit`,
[[normalizer-scaffold]] conventions). Consumes `ex[0-9]+-p[0-9]+-orderbook-snapshot-flink`
([[book-builder]]'s output), diffs each full book against the last book it emitted, and publishes
one `price-level-event` per CHANGED level to the **existing** `ex{id}-p{id}-{side}` topics — the
ones the consolidator already consumes. 13 module tests + live smoke 3/3 green.

This is the end of the raw pipeline. Everything downstream ([[orderbook-consolidator-decision]],
[[orderbook-web]]) is untouched by it, which is the point.

## Decisions

- **The diff is the job.** Job 5 re-emits the whole book on every tick; forwarding that would
  rewrite the consolidator's state on every message. A price whose canonicalized quantity differs
  from what we last sent (or that we never sent) is an upsert; a price we last sent that is absent
  from this book is emitted with `quantity = "0"`; an unchanged book emits **nothing**.
- **`quantity = "0"` is the delete signal because the consumer says so.** Confirmed by reading
  `PerExchangeBookBuilder.processElement` before implementing, not assumed: `signum() == 0` →
  `levels.remove(price)`, and a delete for an unknown price is skipped there, so a redundant
  delete would be harmless anyway.
- **This job keeps its OWN copy of the book, duplicating job 5's state — deliberately.** Job 5's
  state is what the book *is*; job 6's is what we have already *told* the consolidator. They
  diverge on restart, and it is the second one that decides what to send. Merging the two jobs
  would have conflated them.
- **Prices AND quantities are canonicalized before comparison.** The comparison is string equality
  on decimals, so without it "1.0" vs "1" would emit an upsert for a level that never moved. Job 5
  already canonicalizes both, so this is belt-and-braces ([[bigdecimal-rules]]).
- **Timings are stamped around the diff, not around the collect.** The emitted events are buffered
  in a list so `level_emit_out` is written once the diff has actually been computed — otherwise
  the last stage of the latency chain would measure nothing.
- **`event_time` is the book's event_time, copied to every level emitted from it.** This matters:
  the consolidator's R1 rule DROPS an event older than what it holds for that price
  ([[orderbook-aggregation]]). The check is strict `<`, so levels sharing one book's timestamp are
  all accepted.
- **Emission order within a book is not controlled** (upserts then deletes, per side). Safe because
  the consolidator keys by `(pair, exchange, side)` and levels of different prices are independent;
  a single price is never emitted twice from one book.
- **Cold start** is the same unsolved platform gap as job 5: after a restart the state is empty, so
  the first book re-emits every level (harmless — upserts converge). What is NOT recovered is
  deletes for levels that vanished *during* the downtime: a book we never saw cannot be diffed.

## Gotchas

- **`price-level-event` was registered at v1 WITHOUT `pipeline_timings`** and had to be re-registered
  (now v2, id 10) — the same stale-subject trap as jobs 1, 2 and 5. Unlike those, this subject is
  *frozen* and live, so compatibility was checked via `/compatibility/subjects/.../versions/latest`
  BEFORE registering (`is_compatible: true` — the field is nullable with a default, so old readers
  and old data both still work). Do that check first for any frozen subject.

## Smoke (`smoke-level-emitter.sh`)

Follows the raw-in whole-chain **smoke rule** ([[normalizer-scaffold]]): raw OKX → `ex8-raw`, all
SIX jobs must be RUNNING, capture `ex8-p1-asks` / `ex8-p1-bids`.

**Its assertions are shaped differently from every other smoke here, and this is the reason:** job
6 emits a diff, so its output depends on what it emitted in *previous runs of the script*, whose
state is still in the job. So it cannot assert "exactly N records arrived". Instead each run picks
a **price band derived from the clock** (levels the job has never seen), drains the batch its own
event produced, and asserts a specific `(price, quantity)` is present or absent. Deletes of an
earlier run's levels riding along in the same batch are correct behaviour, not noise.

Case 2 is the one that matters: after an update touching one ask, the *untouched* ask from case 1
must NOT appear on the topic. That is the amplification-suppression guarantee.

**Why:** job 6 is the seam where the new pipeline replaces NiFi's normalized output — its records
must be indistinguishable from what NiFi writes today, which is what makes the M8 cutover a switch
rather than a migration.
**How to apply:** if the consolidated book looks wrong, the question is *which* copy disagrees —
compare job 5's `order-book-snapshot` topic against the book the consolidator rebuilt from job 6's
levels (an explicit open task in todo.md). Equal books mean the bug is upstream; different books
mean the diff lost or invented a level.
