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
- ~~The web UI does not consume `-merged`~~ **done 2026-08-24**: a third entry in [[orderbook-viewer]]'s
  exchange dropdown, `exchange_id = -1` on the wire. Still NOT verified against live Kafka on either
  side — no merged record has ever been produced AND consumed for real.

## 2026-09-13 — "merged spread ≠ aggregated spread" is LAG, not merge logic (measured live)

User report: with a single-exchange best level on each side, the merged and aggregated spreads in
the UI should match, and they don't. **Diagnosis (read-only on the dev server): the merger is
falling behind job 6, and each side falls behind by a different amount.** Nothing was changed.

- **Throughput deficit, measured from topic end offsets over 65 s:** all `p*-{side}` topics grew
  **908 rec/s**, `-merged` **727 rec/s**, `-adjusted` **474 rec/s**. Both jobs map 1 record to 1
  record, so output below input means a growing backlog. It is uneven per side: over the same
  65 s, `p1-asks-merged` got +462 and `p1-bids-merged` +237, while job 6 wrote ~1970 to each.
- **Event-time gap on pair 1:** `p1-asks-merged` was **~17 min** behind `p1-asks`,
  `p1-bids-merged` **~26 min**. About 20 min later, `-asks-merged` was ~23 min behind, so the
  gap keeps growing and the job never catches up. The UI therefore subtracts a bid from one
  minute from an ask from another.
- **Cause:** CPU starvation. Host run queue was 30–31 on 8 CPUs (~4% idle), and the TaskManager
  held ~2.7 cores for all 8 jobs. Swap was full but not actively paging. Each job runs its whole
  source→merge→sink chain in ONE thread at parallelism 1, and that thread got ~27% of a core.
  Every record is a ~750-level Avro book, fully decoded, BigDecimal-parsed, sorted and
  re-encoded. Job 5 (`ex5`/`ex8` snapshots, ~9 s old, which is about the console-consumer's
  startup time) and job 6 (within ~20 s) were current.
- **The merge function was NOT proven correct on live data.** Pairing a merged record with its
  parent through `source_id` failed: job 6's `event_time` is the MAX across books and isn't
  monotonic, so binary search by event time doesn't work. Its correctness rests on code review
  plus the 16 unit tests.

Traps that cost time, so nobody re-derives them:
- **Kafka `CreateTime` on every Flink output is inherited from the upstream record**, not the time
  it was written. It says nothing about freshness; use end-offset deltas instead.
  **2026-09-14: the broker is switched to `LogAppendTime` (not deployed yet)**, see [[kafka-topic-strategy]] § 2026-09-14.
  Once it is deployed, the record timestamp is each hop's append time, but it still does not show lag, so keep using offsets.
- **Flink REST vertex metrics read 0 for every rate on every job** (`numRecordsInPerSecond`,
  `records-lag-max`, busy/backpressure), even while topics grow. They are useless for
  diagnosis here; cause not investigated.
- Flink REST on the dev server is host port **7070**, not 8081.
- `kafka-get-offsets --topic '<regex>'` works; `--topic-partitions` with a regex returns nothing.

Side findings, not fixed:
- **ex9/lbank `event_time` is +8 h (28 790 s ahead of wall clock).** This is the risk
  [[project_pair_extractor]] flagged: `TS` is read as UTC but is really UTC+8. Job 6 takes the
  max, so every pair with lbank carries an event_time 8 h in the future, merged and adjusted too.
- `p1` aggregated looked **crossed** in one sample: lbank best ask 77171.64 < bybit best bid
  77315.1. Unverified whether that is a real price gap or a stale lbank book.
- **How to raise merger parallelism** (answered 2026-09-13, not applied). `run-job.sh` already
  reads `PARALLELISM` (default 1): `PARALLELISM=N ./flink/run-job.sh merger`. It does NOT cancel
  the running copy first, so cancel the old merger or two of them write `-merged`. It needs N-1
  free slots, and the dev TaskManager is 8/8. Each extra reader brings its own fetch buffer
  (up to 8 MB) into the ~287 MB direct-memory budget. Per-topic order holds, because every input
  topic has one partition, is owned by one reader, and source→map→sink are chained. **More
  threads do not add cores:** on a CPU-saturated host this takes CPU away from job 6.
- **Slots raised 8 → 12 in `docker-compose.yml`** (2026-09-13, user request, NOT deployed). The
  user's target is merger parallelism **5**, and 7 other jobs × 1 + 5 = 12. Process memory
  (2g) was deliberately left alone, which leaves the direct-memory risk open.
  `docker-compose.prod.yml` already has 4 TMs × 3 = 12 slots, so it is unchanged. The comments in
  `MergerJob`/`AggregatorJob`/`AdjustmentJob` that say "8 slots" are now stale, and were left
  untouched.
- **Makefile now sets parallelism per job** (2026-09-13, user request). `PARALLELISM_<job>` falls
  back to `DEFAULT_PARALLELISM := 1`, and `PARALLELISM_merger := 5`. One helper,
  `$(call submit_jobs,<jobs>)`, replaced the four copied `for` loops (refresh-normalizer,
  run-normalizer-jobs, run-all-jobs, prod-deploy). **The helper is used by prod-deploy too, so
  prod also gets merger = 5, which fits its 12 slots exactly.** Override per run with
  `make run-all-jobs PARALLELISM_merger=3`. ⚠ Keep the parallelism sum ≤ the slot count, which
  nothing checks. ⚠ macOS `make` is GNU 3.81 (no `--eval`); test helpers through a wrapper
  makefile that `include`s the real one. Verified with a fake `FLINK_RUN`: correct values
  reach the script, and a failure stops the chain with a non-zero exit.
- **✅ VERIFIED LIVE 2026-09-13 after deploying merger parallelism 5 + 12 slots: the merger has
  CAUGHT UP.** Over 66 s the aggregated topics wrote 1053.4 rec/s and `-merged` 1052.6, equal
  per topic (p1-asks +558/+558, p3 +606/+606), and the latest `p1-{side}` and
  `p1-{side}-merged` event_times were IDENTICAL in two samples 60 s apart. The Flink UI (per the
  user) shows busy dropping from 100% to ~50% with 12% data skew. **More merger parallelism is
  NOT needed**: 50% busy is ~2× headroom. The skew comes from hot pairs on one-partition topics,
  so more readers would not reduce it. **`orderbook-adjustment` is now the lagging job**:
  608.6 rec/s against 1053.4 in, with `p1-*-adjusted` ~46 min behind. It needs at least 1.73×,
  so parallelism 3, but all 12 slots are used. Reached the server DIRECTLY from the laptop
  (`ssh -p2020 m_gholami@192.168.150.31`); the `asus` jump host is not required.
- **Re-sized 2026-09-13 (NOT deployed): 14 slots, `PARALLELISM_job-aggregator := 2`, merger 5 → 4,
  `adjustment := 3`.** The live per-subtask numbers behind it: merger ~40% busy at 5 (≈480/s
  each), so 4 → ~60% at 1200/s; adjustment 100% busy at 1 (≈530/s), so 3 → ~75%. The input
  rises to ~1200/s once job 6 stops lagging, which is why sizing used that figure rather than
  today's ~1000. ⚠ **prod has 4 TM × 3 = 12 slots, so `prod-deploy` no longer fits** and fails
  on its last jobs. Resizing prod's per-TM memory was left for the user. ⚠ Direct memory was
  225 MB of the ~287 MB limit with 12 tasks, and this adds 2 net source readers (≤ 8 MB fetch
  each). No memory change was made; watch for `Direct buffer memory`. See [[aggregator]]
  § 2026-09-13 for the job-6 code fix.
- **More CPU for the TaskManager** (answered 2026-09-13, nothing applied). No container in
  EITHER compose file has a CPU limit (prod sets memory limits only), so the TaskManager may
  already use all 8 cores. The problem is contention, not a cap, and "more CPU" can only mean
  more WEIGHT (`cpu_shares`, the default is 1024), capping neighbours (`cpus:`), pinning
  (`cpuset`), or offloading. The zero-sum catch: Kafka and NiFi feed the pipeline, so starving
  them moves the lag upstream. `alloy` (~1 core), grafana, loki and portainer run on the dev box
  but are NOT defined in this repo. NiFi stays out of scope ([[project_flink_production]]).
- Structural: asks and bids are separate records on separate topics, so ANY independent consumer
  can pair two sides from different moments. Lag makes it minutes instead of milliseconds.
