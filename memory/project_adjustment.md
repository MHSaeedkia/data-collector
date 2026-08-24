---
name: adjustment
description: flink/adjustment/ — standalone Flink project reading job 6's p{id}-{side} and publishing p{id}-{side}-adjusted on its own subject; three COMPOUNDING price stages (commission 0.35%, profit 0.1%, slippage 1%) whose rates ride on the event; the asks-up/bids-down sign convention is assumed, not confirmed.
metadata:
    type: project
---

# Order book adjustment — `flink/adjustment/`

A **third parallel view** of the cross-exchange book, alongside job 6's `p{id}-{side}` and the
merger's `-merged`. Requested 2026-08-24, to be built up in steps the user dictates.

**Step 1 (done): read `p{id}-{side}` → `p{id}-{side}-adjusted`, through three named stages.
Step 2 (done): the three stages, empty. Step 3 (done): each stage moves the price by a constant
percent and records the rate it used on the published event.** Later steps will add the actual adjustment logic — do not anticipate
them ([[scope-discipline]]).

## Shape, and why it mirrors the merger

Standalone single-module project **outside `flink/normalizer/`**, package `io.tibobit.adjustment`,
self-contained (its own `AvroSchemaLoader` copy rather than a dependency on `normalizer-common`,
which is never installed to a repository). Same trade, same reasoning as [[price-merger]] — the
user named the merger as the model. It is not a stage of the raw pipeline; it reads that pipeline's
finished output.

`Decimals.canonicalize` was copied in at step 3, when there was finally arithmetic to need it; step
1 deliberately went without.

## Decisions

**Its own subject, `adjusted-order-book-event`** (`schemas/adjusted_order_book_event.avsc`, added
step 3). Steps 1–2 reused `aggregated-order-book-event`, which was right while the record was
byte-identical to job 6's — the moment the user asked to *see the rates in the event*, it stopped
being. A **new** subject, deliberately not an evolution of the aggregated one: job 6's contract with
`web/` is frozen and must not grow fields because a downstream job wanted them.

⚠ `scripts/warmup.sh` had to gain a `register_schema` line. The e2e harness registers every
`schemas/*.avsc` on its own, so a missing line there is **invisible to a green e2e run** and only
surfaces as this job dying at its first emit — exactly how `control-command` was broken for two
days ([[control-plane]]).

The levels are field-for-field identical to `AggregatedLevel`; only the record gained fields. The
original price is **not** carried alongside the adjusted one — not asked for, and easy to add if
auditing ever wants it.

**Three named placeholder stages, chained in the user's order** (2026-08-24, user's explicit
call — it REVERSES the first version of this job, which had no operator at all on the grounds that
an empty `map` is a placeholder for logic that does not exist):

```
source -> BuySellCommissionFunction -> OurProfitFunction -> SlippageFunction -> sink
```

Each returns its input unchanged for now; how each one calculates is a later step and is
deliberately NOT guessed at. **The order is part of the contract, not tidiness** — the stages
compose, so slippage sees prices the two above it have already moved. If slippage should work off
the exchange's original prices instead, the chain has to change shape, not just its arithmetic.

Every stage is `.name()`d so the chain is readable in the Flink web UI — the cheapest check that a
deployed job is wired the way the source says.

### What each stage does (step 3)

| Stage | Percent | Applied to |
| --- | --- | --- |
| `BuySellCommissionFunction` | 0.35 | the exchange's own price |
| `OurProfitFunction` | 0.1 | the commission-adjusted price |
| `SlippageFunction` | 1 | the profit-adjusted price |

**They COMPOUND, they do not add.** On asks the total is 1.0035 × 1.001 × 1.01 = **1.014548535**,
not the 1.0145 that summing the rates gives. That follows from the chain the user asked for, and
`theThreeStagesCompoundRatherThanAdd` pins it with literal expected prices — worked out
independently rather than recomputed from the rates, since a test that repeats the implementation's
formula agrees with it by construction.

**The rates are constants in each function class** (`static final BigDecimal PERCENT`), which is
where the DB read replaces them — user's sequencing, 2026-08-24: constants first, database later.
Each stage writes the rate it used onto the record itself, so the event and the arithmetic cannot
disagree; there is no second place holding "what we said we charged".

### ⚠ The sign convention — the single most important line in this job

`Prices.multiplier` is the only place it is decided: **`asks` is what a user BUYS at so every charge
moves that price UP; `bids` is what they SELL at so every charge moves it DOWN.** Both take money
from the same side of the trade. Inverting either would publish a book you could buy from and sell
back into at a profit.

This is the standard convention and what the stages were written against, but **it was assumed, not
confirmed by the user**. If it is wrong it is wrong in that one method. Inverting it fails 7 tests.

### Exactness

`BigDecimal` from the wire string throughout ([[bigdecimal-rules]]); `movePointLeft(2)` converts
percent to fraction rather than `divide(100)`, because a scale shift is exact and cannot throw.
Prices are canonicalized on the way out so the scale that exact multiplication grows
(`62650.00` × 1.0035 = `62869.275000`) does not accumulate as trailing zeros down the chain.
`arithmeticIsExactNotFloatingPoint` fails if anyone reaches for a double.

**Nothing is rounded to the market's tick size.** Job 4 applied `markets.price_precision` upstream
and multiplying re-introduces decimals past it, but re-truncating needs the per-market precision
this job does not read, and picking a rounding DIRECTION is a decision with money in it (down
favours the buyer on asks and us on bids). Left exact and flagged rather than guessed.

16 tests. Three mutations confirmed to bite: inverting the sign (7 fail), swapping BigDecimal for
double (4 fail), and the serializer dropping a rate (2 fail).

**The model mirrors the schema in FULL, and here that is load-bearing.** The merger's
`AggregatedOrderBook` is a reader — it models only what it consumes. This job re-encodes the same
record type, so **a field missing from the model is not merely unread, it is dropped and replaced
by the schema default on the way out** — `simulation` would silently become 0 and `source_id` `""`,
both of which are values a real record could legitimately hold. That is the entire risk surface of
a pass-through, and `PassThroughSerdeTest` exists for it: every fixture value is deliberately
non-default, and three mutations were confirmed to fail it (drop `source_id`, drop `simulation`,
write `side` as a plain String — the last one blows up the encoder outright, the same shape as the
live `reset`-enum bug in [[type-validator]]).

**Prices are NOT canonicalized.** The merger canonicalizes (`10.00` → `10`) because it has to
compare numerically; this job must not, and a test pins it. The adjusted topic carries job 6's
strings character for character.

## ⚠ The regex is anchored, and it matters more here than in the merger

`^p[0-9]+-(asks|bids)$`. Unanchored it would match **both** this job's own `p1-asks-adjusted` output
(a self-feeding loop) **and** the merger's `p1-asks-merged`, which is a different record type and
would fail to decode. Kafka's pattern subscription is full-match so the anchors are belt-and-braces,
but widening this regex is how you break it.

## ⚠ Slot budget: the cluster is now FULL

`taskmanager.numberOfTaskSlots: 8` and no job sets parallelism, so each of the 8 jobs takes one
slot: 6 normalizer + merger + adjustment = **8/8**. The next job, or any parallelism increase,
needs the taskmanager reconfigured. [[price-merger]] recorded 7/8 as "the 8th is the last one" —
this is that one.

## OPEN — moving the rates into the database

The user's stated next step. **No table has a column for any of these today** (`exchange_markets`
carries rebase/precision/staleness/depth-aggregation; `markets` the four precision columns), so it
is a schema change plus a `RefreshingLookup`, the way jobs 3 and 4 read rebase factors and
precisions.

The unresolved part is **granularity, and it differs per stage**:

- A commission is charged by the **exchange**, and levels carry `exchange_id` — so it is plausibly
  per-exchange, which would make it a per-LEVEL rate and the current per-record field a lie the
  moment two exchanges with different fees appear in one book.
- **Our profit** is ours, so per-market or global is plausible and a per-record field is fine.
- **Slippage** is usually a function of DEPTH, so a flat percent may not survive contact with the
  real requirement at all.

Settle granularity before writing the migration: it decides whether the rate fields stay on the
record or move onto the level, which is a schema change either way but a much worse one to do
twice.

## OPEN — the lineage question, deliberately not decided

[[record-lineage]]'s rule is that any step writing a record to a topic mints a fresh `id` and names
its parent. Step 1 was specified as "without any changes", so **the record's `id` is carried
through verbatim** — the adjusted record claims job 6's id rather than one of its own. That is the
literal instruction and it is what a pass-through means, but it does conflict with the platform
convention, and the aggregated schema has no record-level `source_ids` field to name a parent in
even if we wanted to. Raise it before step 2 writes anything genuinely different.

## Deploy

`./flink/run-job.sh adjustment` works with **no script edit** — discovery scans poms for a shade
`<mainClass>` ([[flink-deploy-tooling]]). Added to the root Makefile's `ALL_JOBS` (first, with the
merger, since both are downstream of job 6), so `make run-all-jobs` submits it; `refresh-normalizer`
and `run-normalizer-jobs` stay normalizer-only and leave it down, exactly like the merger.
`scripts/warmup.sh` now pre-creates `p{id}-{side}-adjusted` at the 6h output retention.

## Status

16 tests green, `mvn package` clean, shaded jar carries the right `Main-Class` and **zero**
`org/apache/flink` or `org/apache/avro` entries (everything is `provided`, as intended).
**NOT run live** — no stack was up, no smoke test, no e2e scenario. Also not wired into
[[staleness-exporter]] (which does not watch `-merged` either), `e2e/`, or `web/`.
