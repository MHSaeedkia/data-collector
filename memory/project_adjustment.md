---
name: adjustment
description: flink/adjustment/ — standalone Flink project reading job 6's p{id}-{side} and publishing p{id}-{side}-adjusted; step 1 is a verbatim pass-through, later steps add real adjustment logic.
metadata:
    type: project
---

# Order book adjustment — `flink/adjustment/`

A **third parallel view** of the cross-exchange book, alongside job 6's `p{id}-{side}` and the
merger's `-merged`. Requested 2026-08-24, to be built up in steps the user dictates.

**Step 1 (done): read `p{id}-{side}`, write the same record verbatim to `p{id}-{side}-adjusted`,
through three named but empty adjustment stages.** Nothing is transformed yet. Later steps will add the actual adjustment logic — do not anticipate
them ([[scope-discipline]]).

## Shape, and why it mirrors the merger

Standalone single-module project **outside `flink/normalizer/`**, package `io.tibobit.adjustment`,
self-contained (its own `AvroSchemaLoader` copy rather than a dependency on `normalizer-common`,
which is never installed to a repository). Same trade, same reasoning as [[price-merger]] — the
user named the merger as the model. It is not a stage of the raw pipeline; it reads that pipeline's
finished output.

`Decimals` was deliberately NOT copied over: step 1 does no arithmetic, and a helper with no caller
is not a helper.

## Decisions

**Output uses the SAME subject, `aggregated-order-book-event`.** The adjusted record IS an
aggregated record — nothing was changed — so it needs no schema of its own and **no registry work
to deploy**, which is the standing trap on this platform ([[avro-schema]]). Consumers resolve by
the wire-header id, so anything that reads `p{id}-{side}` reads the adjusted topic unchanged.
⚠ The moment a step adds a field that only exists after adjustment, that is when it needs its own
subject — and then the usual "re-register AND resubmit, in that order" rule applies.

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

`AdjustmentFunctionsTest` writes the no-op contract down and is **meant to fail when real logic
lands**; that failure is the prompt to restate the contract rather than let a stage move prices
with nothing describing it. Mutation-checked: making one stage alter a price fails both its own
test and the chain test.

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

## OPEN — what the three stages actually do, and where their parameters live

Nothing in the repo mentions commission, fee, profit, slippage or markup, and **no table has a
column for any of them** (`exchange_markets` carries rebase/precision/staleness/depth-aggregation
only; `markets` carries the four precision columns). So before step 2 these have to be settled:

- **The formulas**, per stage — percentage, basis points, or fixed; applied to price, to quantity,
  or both.
- **The sign convention.** `asks` is what a user buys at and `bids` what they sell at, so a
  commission normally moves price in OPPOSITE directions on the two sides. Assumed, never
  confirmed — do not implement off this sentence.
- **Where the parameters live and at what granularity.** Commission is plausibly per-EXCHANGE (each
  exchange charges its own, and levels carry `exchange_id`), while "our profit" is ours and is
  plausibly per-market or global — different granularities for different stages. The platform's
  existing pattern is a postgres column read through `RefreshingLookup` (jobs 3 and 4), which would
  mean a schema change.
- **The output shape.** If an adjusted level must carry the ORIGINAL price alongside the adjusted
  one, the "reuse `aggregated-order-book-event`" decision above dies and this job needs its own
  subject plus the usual re-register-then-resubmit dance. Worth deciding once rather than three
  times.

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

11 tests green, `mvn package` clean, shaded jar carries the right `Main-Class` and **zero**
`org/apache/flink` or `org/apache/avro` entries (everything is `provided`, as intended).
**NOT run live** — no stack was up, no smoke test, no e2e scenario. Also not wired into
[[staleness-exporter]] (which does not watch `-merged` either), `e2e/`, or `web/`.
