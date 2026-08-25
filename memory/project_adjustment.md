---
name: adjustment
description: flink/adjustment/ — standalone Flink project reading job 6's p{id}-{side} and publishing p{id}-{side}-adjusted on its own subject; three price stages (commission, profit, slippage) each sized off the ORIGINAL price so they ADD rather than compound; commission is still a flat 0.35% constant, profit/slippage are read from exchange_markets PER LEVEL (2026-08-25, since rates vary per exchange); the asks-up/bids-down sign convention is assumed, not confirmed.
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
| `BuySellCommissionFunction` | 0.35 | the price the level ARRIVED with |
| `OurProfitFunction` | 0.1 | the price the level ARRIVED with |
| `SlippageFunction` | 1 | the price the level ARRIVED with |

**They ADD, they do not compound** (user correction, 2026-08-24, same day the compounding version
shipped). Every stage sizes its amount off the price the level **arrived with**, never off the
running price, so an ask ends at base × **1.0145** — not the 1.0035 × 1.001 × 1.01 = 1.014548535
the first implementation produced. `AdjustedLevel.basePrice` is what makes that possible: seeded
from the arrival price in the constructor, never written by a stage, and deliberately not on the
wire (the original is recoverable from the published price and rates).

⚠ **The consequence that is easy to miss: the chain order no longer affects the result.** Addition
commutes, so reordering the stages produces identical prices. The order is presentation — it is what
the Flink UI shows — but it is no longer arithmetic, which it *was* under the compounding version.
`reorderingTheStagesCannotChangeTheResult` pins that, and it is the test that fails if anyone
reintroduces a dependency between stages.

Expected prices in the tests are literals worked out independently, not recomputed from the rates:
a test that repeats the implementation's formula agrees with it by construction.

**Commission's rate is still a constant** (`static final BigDecimal PERCENT` in
`BuySellCommissionFunction`), unchanged and out of scope of the DB move below. **Profit and
slippage are no longer constants** — see "Step 4" below. Each stage writes the rate it used onto
the record (commission) or the level (profit, slippage) so the event and the arithmetic cannot
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
Prices are canonicalized on the way out so exact arithmetic's growing scale does not accumulate as
trailing zeros down the chain.
`arithmeticIsExactNotFloatingPoint` fails if anyone reaches for a double.

**Nothing is rounded to the market's tick size.** Job 4 applied `markets.price_precision` upstream
and multiplying re-introduces decimals past it, but re-truncating needs the per-market precision
this job does not read, and picking a rounding DIRECTION is a decision with money in it (down
favours the buyer on asks and us on bids). Left exact and flagged rather than guessed.

19 tests. Four mutations confirmed to bite: inverting the sign (7 fail), swapping BigDecimal for
double (4 fail), the serializer dropping a rate (2 fail), and **reverting to compounding by sizing
the amount off the running price instead of the base (4 fail)**.

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

## Step 4 — all three rates moved onto the level, read from the DB (2026-08-25, DONE)

**Granularity settled by the user 2026-08-25, in two parts**: `our_profit_percent`/`slippage_percent`
first (flat percent per `(exchange_id, market_id)` — NOT per-market-only, the user's first framing,
because slippage genuinely differs by exchange for the same market, their own example: ex1/market1 =
1%, ex2/market1 = 2%; not depth-tiered either). **Then, same day, the user asked for "exactly the same
job for buy/sell commission"** — confirmed per exchange+market too, not per-exchange-only, so it
followed the identical path. See [[project_db_schema]] for the columns, defaults, and the "why not a
separate table" reasoning.

**This forced a schema change that wasn't originally scoped**: all three rate fields used to be
RECORD-level on `adjusted_order_book_event.avsc` (commission always was; profit/slippage until the
first half of this step), applied uniformly to every level. That is provably wrong once rates vary per
exchange, because one book is job 6's UNION across exchanges — `AdjustedLevel` already carries its own
`exchange_id` per level for exactly this reason. **All three fields now live on the nested
`AdjustedLevel` record**; `AdjustedOrderBook` carries none of them any more.

**Read path**: `AdjustmentFactors` (POJO: `profitPercent`, `slippagePercent`, `commissionPercent`, all
`BigDecimal` — mirrors `RebaseFactors`) + `AdjustmentFactorsLoader` (mirrors `RebaseFactorsLoader`
exactly: one query, `SELECT exchange_id, market_id, our_profit_percent, slippage_percent,
buy_sell_commission_percent FROM exchange_markets WHERE market_id IS NOT NULL`, keyed
`{exchange_id}|{market_id}`). `BuySellCommissionFunction`/`OurProfitFunction`/`SlippageFunction` are
all `RichMapFunction`s, each holding its own `RefreshingLookup<String, AdjustmentFactors>` — **three
independent lookups, one per stage**, each polling exchange_markets on its own schedule
(`REFRESH_INTERVAL_MS`, default 60s, same env var name as job 3). Deliberately not shared: mirrors the
one-lookup-per-operator ownership job 3 already has, and Flink ships each operator's constructor args
as its own serialized closure anyway, so sharing one instance wouldn't actually reduce anything at
runtime — it would just look shared in the source.

**`Prices.applyPercent` (the record-wide version) is GONE** — once commission joined profit/slippage
per-level, nothing called it any more, so it was deleted rather than left as dead code.
`applyPerLevelPercent` is now the ONLY arithmetic method: takes a
`BiFunction<Integer,Integer,BigDecimal> (pairId, exchangeId) -> percent` and a
`BiConsumer<AdjustedLevel,String>` rate-writer, so the sign convention and arithmetic still live in
exactly one place for all three stages.

**Fallback when exchange_markets has no row for a level's `(exchange, pair)`**: falls back to
`DEFAULT_PERCENT` — the value that used to be hardcoded (0.35 / 0.1 / 1), a `static final BigDecimal`
on each function class. This job has no dead-letter side output the way job 3 does for
`no_rebase_row` (it's a `MapFunction` chain, not a `ProcessFunction`), and silently charging 0%
would under-charge rather than merely go stale — so falling back to the old constant was judged
safer than either introducing a new side-output topic (bigger change than asked) or charging
nothing. Not discussed with the user explicitly; worth confirming if it ever matters in practice
(should be rare — same "near-guaranteed to exist" argument `RebaseFactorsLoader` makes, since job 1
already resolved this exchange+pair from the same table upstream of the aggregated book this job
reads).

**Files touched**: `schemas/adjusted_order_book_event.avsc` + its `_example.json` (breaking change,
twice the same day: first profit/slippage record→level, then commission record→level too — the
record now carries ZERO rate fields), `AdjustedLevel`/`AdjustedOrderBook`/`Prices`/
`BuySellCommissionFunction`/`AdjustedOrderBookSerializer`/`AdjustmentJob` (wiring: `POSTGRES_URL`/
`POSTGRES_USER`/`POSTGRES_PASSWORD`/`REFRESH_INTERVAL_MS` env vars, same names as job 3), plus three
NEW files from the first half — `AdjustmentFactors`, `AdjustmentFactorsLoader`, and a **copied**
`RefreshingLookup` (this module is self-contained, doesn't depend on normalizer-common — same trade as
`AvroSchemaLoader`/`Decimals`; keep the two copies in sync by hand if either changes). `pom.xml`
carries the postgres driver at **default (compile) scope, NOT `provided`** — see the deploy-failure
note below; `provided` was tried first and is wrong for this specific dependency.

**Tests**: `AdjustmentFunctionsTest` (14→16→18: `differentExchangesInTheSameBookGetDifferentRates` +
`aMissingRowFallsBackToTheDefaultPercent` from the profit/slippage half, then
`commissionAlsoVariesPerExchangeInTheSameBook` + `aMissingRowFallsBackToTheDefaultCommissionPercent`
mirroring both for commission) + `AdjustedOrderBookSerdeTest` (5, updated for the moved fields +
`theModelCoversEveryFieldOfTheSchema`'s new field lists, twice). Every existing test that predates the
DB read opens its function with an EMPTY lookup map, which deliberately triggers the SAME fallback
constants the old hardcoded values were — so none of the pre-existing numeric literals needed to
change. Verified: `mvn -o clean test` — 23/23 green.

⚠ **LIVE DEPLOY FAILED, then fixed, same day (2026-08-25)**: first submission crashed every task
attempt on startup with `ClassNotFoundException: org.postgresql.Driver` (`NoRestartBackoffTimeStrategy`
→ job goes straight to FAILED, no retry). Cause: the postgres dependency was declared `provided`,
copying the scope of the OTHER dependencies in this pom without checking whether it applies to
postgres too. It doesn't — **`/opt/flink/lib/` on this Flink image does NOT carry the postgres
driver**, per `flink/normalizer/pom.xml`'s own comment on the identical dependency ("NOT in the
Flink image lib, so compile scope: it must ship inside each DB-reading job module's shaded jar"),
and job-rebaser/job-pair-extractor both declare it with no scope (defaults to `compile`) for that
reason. Fixed by dropping `<scope>provided</scope>` so the driver shades into the jar; confirmed
`org/postgresql/Driver.class` present in `target/adjustment-1.0-SNAPSHOT.jar` via `unzip -l`
before redeploying. **Lesson for the next new dependency in this pom: check what
`flink/normalizer/Dockerfile` actually installs into `/opt/flink/lib/` before marking anything
`provided` — don't copy a neighboring dependency's scope on assumption.**

⚠ **Deploy order, same trap as every other schema change on this platform**: re-register
`adjusted-order-book-event` (content changed even though the subject name didn't — `warmup.sh`
re-reads the `.avsc` file directly, no script edit needed) BEFORE resubmitting `job-adjustment` —
the serializer caches the write schema on first use. Also needs the server's `exchange_markets`
`ALTER TABLE` (see [[project_db_schema]]) run before the job starts, or every level falls back to
`DEFAULT_PERCENT` and the DB read is silently a no-op.

**✅ VERIFIED LIVE end-to-end, 2026-08-25** — `make run-all-jobs` was run against the real stack
after both the profit/slippage AND commission changes landed. `orderbook-adjustment` came up
`RUNNING` and stayed healthy with no exceptions in the taskmanager log (confirmed via
`docker logs taskmanager | grep -i adjustment`), which proves the `buy_sell_commission_percent`
column actually exists on this server's `exchange_markets` and the three-lookup DB read works
live, not just in the test suite. A separate, unrelated infra problem surfaced during this same
verification — see [[flink-deploy-tooling]] for the slot-leak fix (taskmanager+jobmanager
restart), which is a platform-wide issue and not specific to this job.

**02_seed.sql header bug, found and fixed same day**: the earlier profit/slippage `sed` edit
had TWO `-e` clauses in one invocation; the second failed to compile (unbalanced parens),
which aborts the WHOLE sed command before touching the file — so the INSERT column-list header
never got `our_profit_percent, slippage_percent` appended, even though a later, separate fix DID
append the two values to every row's tuple. Result: 8 named columns, 10 values per row —
`02_seed.sql` was broken SQL and had already been committed. Caught and fixed while adding the
commission column (same header line, one more pass). Lesson: after a multi-`-e` sed that reports
an error, verify ALL clauses landed — a compile-time failure in one clause silently drops every
clause in that invocation, not just the broken one.

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

19 tests green, `mvn package` clean, shaded jar carries the right `Main-Class` and **zero**
`org/apache/flink` or `org/apache/avro` entries (everything is `provided`, as intended).
**NOT run live** — no stack was up, no smoke test, no e2e scenario. Also not wired into
[[staleness-exporter]] (which does not watch `-merged` either), `e2e/`, or `web/`.
