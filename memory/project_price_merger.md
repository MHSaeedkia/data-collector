---
name: price-merger
description: flink/merger/ — standalone Flink project (NOT a normalizer job) that sums job 6's unioned levels into one level per price on p{id}-{side}-merged
metadata:
    type: project
---

# Price merger — `flink/merger/`

A **second, parallel view** of the cross-exchange book: one level per price, quantities **summed**
across exchanges. Requested 2026-08-11.

```
ex1: price 10, qty 3, source_id A     ->  price: 10, quantity: 7,
ex2: price 10, qty 4, source_id B         exchange_ids: [1,2], source_ids: [A,B]
```

## This does NOT replace union-never-sum

[[orderbook-aggregation]]'s union-never-sum is a pinned business decision and job 6 is untouched.
Both topics are live at once and consumers pick. Anyone reading only that memory file would
conclude summing is forbidden — it is forbidden *on `p{id}-{side}`*, which is why this is a
separate job writing a separate topic rather than a flag on [[aggregator]].

## Why it is outside `flink/normalizer/`

User's explicit instruction, and it is structurally right: this is not a stage of the
raw-normalization pipeline, it reads that pipeline's finished output. Own Maven project,
**single module, no parent/common split** — there is one job here.

**Self-contained on purpose.** It duplicates `AvroSchemaLoader` and `Decimals.canonicalize` rather
than depending on `io.tibobit:normalizer-common`, which is never installed to a repository —
depending on it would mean building `flink/normalizer` before this project could compile at all.
That is ~40 duplicated lines against a hard build-order coupling; if common ever gets published,
revisit. Only `canonicalize` was copied, not `rebase`/`truncate`: this job does neither.

Runs on the **same Flink cluster and the same image** (`flink/normalizer/Dockerfile`) — it needs
nothing in `/opt/flink/lib` that the normalizer has not already installed, so no compose change.
Slot budget went 6 → 7 of `taskmanager.numberOfTaskSlots: 8`; **the 8th is now the last one.**

## Four user decisions (2026-08-11)

1. **Input = job 6's `p{id}-{side}`, not job 5's per-exchange books.** Costs one Kafka hop of
   latency, buys a **stateless** job: job 6 has already fanned in, so one aggregated record is the
   complete book for that pair+side and the merge is a pure function of it. No MapState, no
   splitter, no gap/reset handling — an exchange job 6 dropped is simply already absent from the
   input. The alternative was re-implementing ~300 lines of [[aggregator]].
2. **Grouped by (price, simulation), never price alone.** A live and a simulated level at one price
   stay two MergedLevels, live first. Summing them would report simulated depth as real —
   consistent with why [[simulation-flag]] is per-level at all.
3. **`exchange_ids` array on each level**, positionally aligned with `source_ids`. The merge
   destroys the scalar `exchange_id`, and "who is behind this 7?" is the first question a reader of
   a merged level asks.
4. **Naming `p{id}-{side}-merged`** / subject `merged-order-book-event`.

## Lineage — the one place this bends the convention

[[record-lineage]]'s rule is one hop per level. Here the level's `source_ids` are the contributing
**AggregatedLevel.source_id**s, which name **job-5 snapshots** — one hop further back than the
immediate parent. There is no alternative: an aggregated *level* has no id of its own to point at.
The record-level `source_id` is the strict one-hop parent (the aggregated record's `id`), and it is
**singular**, unlike job 6's per-level scheme — the merger consumes exactly one record per output
record, so the parent is unambiguous. `id` is re-minted here like at every hop.

## Gotchas already paid for

- **Input topic regex is anchored** `^p[0-9]+-(asks|bids)$`. Unanchored it would also match this
  job's own `p1-asks-merged` output — a self-feeding loop. Kafka's pattern subscription uses
  full-match semantics so the anchors are belt-and-braces, but widening that regex is dangerous.
- **Output `price` is canonicalized** (`10.00` → `10`). Merging must compare numerically, so
  equal-value prices become one level and the output has to spell it one way. Level count out is
  therefore ≤ level count in, and the price *string* may differ from job 6's.
- Avro hands back `Utf8`, not `String` — converted at the decode boundary
  ([[record-lineage]] has the same warning).
- Sorting is **explicit**, not inherited from job 6's already-sorted input: job 6's tie-break is
  quantity, which says nothing about where a simulated twin lands.

## Status

16 tests green (12 merge + 4 serde, incl. a real Avro binary round-trip against the canonical
schema, which `GenericData.validate` alone would not catch). Jar packages with the right
Main-Class. **NOT run live — no stack was up, no smoke test, no e2e scenario.**

Deliberately **not** done, needs a decision:
- ~~Not wired into deployment~~ **done 2026-08-11**: `make run-all-jobs` submits it, first in the
  chain (it is downstream of job 6). `refresh-normalizer` / `run-normalizer-jobs` are still
  normalizer-only, so a refresh leaves the merger down — see [[flink-deploy-tooling]].
- [[staleness-exporter]] does not watch the `-merged` topics.
- No e2e coverage; `e2e/` asserts jobs 5 and 6 only ([[e2e-harness]]).
- ~~The web UI does not consume `-merged`~~ **done 2026-08-24**: a third entry in [[orderbook-web]]'s
  exchange dropdown, `exchange_id = -1` on the wire. Still NOT verified against live Kafka on either
  side — no merged record has ever been produced AND consumed for real.
