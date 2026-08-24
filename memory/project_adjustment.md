---
name: adjustment
description: flink/adjustment/ — standalone Flink project reading job 6's p{id}-{side} and publishing p{id}-{side}-adjusted; step 1 is a verbatim pass-through, later steps add real adjustment logic.
metadata:
    type: project
---

# Order book adjustment — `flink/adjustment/`

A **third parallel view** of the cross-exchange book, alongside job 6's `p{id}-{side}` and the
merger's `-merged`. Requested 2026-08-24, to be built up in steps the user dictates.

**Step 1 (done): read `p{id}-{side}`, write the same record verbatim to `p{id}-{side}-adjusted`.**
Nothing is transformed yet. Later steps will add the actual adjustment logic — do not anticipate
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

**No identity operator between source and sink.** Step 1 is `fromSource(...).sinkTo(...)`. An empty
pass-through `map` would be a placeholder for logic that does not exist; the transform gets its own
operator when there is one.

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

6 tests green, `mvn package` clean, shaded jar carries the right `Main-Class` and **zero**
`org/apache/flink` or `org/apache/avro` entries (everything is `provided`, as intended).
**NOT run live** — no stack was up, no smoke test, no e2e scenario. Also not wired into
[[staleness-exporter]] (which does not watch `-merged` either), `e2e/`, or `web/`.
