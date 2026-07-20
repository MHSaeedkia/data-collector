# Manual test data — BTC-USDT raw payloads for the normalizer chain

Hand-crafted `ex{id}-raw` payloads for **manual** end-to-end testing: produce them to the raw
topic, let all 6 normalizer jobs run, and watch the result land in the order book web UI.

**Everything targets one market: BTC-USDT (`pair_id` 1).** ex8/okx uses the wire symbol
`BTC-USDT`, ex3/wallex uses `BTCUSDT`; both resolve to `market_id` 1 in the seed.

**Every scenario is independent.** Run any one of them, on its own, in any order, as many times
as you like. Nothing has to run first.

These are **not** unit-test fixtures. The parser fixtures under
`job-pair-extractor/src/test/resources/fixtures/` stay the canonical wire samples; this set is
built to exercise *behaviour* — acceptance, rejection, deletes, truncation, resync.

Ground truth for the wire formats is `sample-raw-data.md` at the repo root.

---

## Running

```bash
./produce.sh --list                  # what's available
./produce.sh 03-ex8-sequence-gap     # reset state, then run just this scenario
DELAY=1 ./produce.sh 02-ex8-happy-path   # 1s between messages, easier to watch the UI
```

There is deliberately **no "run everything" mode** — scenarios are independent, and each one
needs its own reset to stay that way.

### What makes them independent

Two things, and both are needed:

1. **Each scenario carries its own baseline.** Every one opens with its own snapshot and its
   own self-contained `ts` timeline starting from the same base. Nothing refers to a book or a
   sequence number established elsewhere.
2. **`produce.sh` resets pipeline state first**, by calling `reset.sh`.

The reset is the load-bearing half. All scenarios share `pair_id` 1, so the *only* thing
separating one run from the next is job state — and scenario 01 exists precisely to test the
"no baseline yet" branch, which cannot be reproduced any other way.

`reset.sh` cancels and resubmits the four **stateful** jobs, downstream-first:

| Job | State it holds |
| --- | --- |
| `orderbook-consolidator` | per-exchange book + last `event_time` |
| `normalizer-level-emitter` | its copy of "what we already told the consolidator" |
| `normalizer-book-builder` | the order book itself (`MapState` per side) |
| `normalizer-type-validator` | `{lastSeq, awaitingSnapshot}` |

The other three jobs (pair-extractor, rebaser, precision) are stateless and are left running.
Because nothing in this platform is checkpointed, cancel+resubmit **is** the state reset.
Downstream-first ordering matters: every source reads `latest`, so an upstream job restarted
first would emit into a topic nobody is reading yet.

Resubmission reuses the jar already uploaded to Flink — no Maven rebuild — so each job must
have been submitted at least once (`run-job.sh` / `make refresh-normalizer`). Budget ~20–30s
per reset.

`--no-reset` skips it, for when you deliberately want to chain scenarios and watch state carry
over.

### Why the script rewrites `ts` (ex8 only)

For okx, `ts` is simultaneously the **sequence id**, the **event time**, and the only timestamp
on the message. The files carry a fixed synthetic base (`1800000000000`) so the timelines are
readable and diffable — but that base sits in the **future** relative to real time.

Sending it verbatim would be actively harmful, not merely odd: the consolidator drops events
older than the one it has stored, so a future-dated book would poison the stored timestamp and
every subsequent *real* event would be dropped until wall-clock caught up.

So each scenario's `ts` window is shifted onto now, aligned to the 300 ms cadence, preserving
every delta **within** that scenario — the +300 steps and the deliberate gap, duplicate and
backwards snapshot all survive. `--verbatim` is not offered here; the shift is per-scenario and
always correct.

ex3/wallex has no ordering field at all and is stamped with processing time by job 1, so
nothing is rewritten there.

---

## Before you start

- normalizer stack up (`docker-compose-normalizer.yml`), all 6 jobs + the consolidator submitted
- `scripts/warmup.sh` run (topics + registry subjects + DB seed)
- order book web UI open on BTC/USDT

## Reference data

From `postgres/02_seed.sql`, market 1 is `price_precision 2`, `quantity_precision 8`, rebase
`0/0` (identity) for both exchanges — so **wire prices equal UI prices** and every expectation
below is checkable by eye.

`ts` offsets are shown relative to each scenario's own base `B = 1800000000000`.

| Scenario | Exchange | Focus |
| --- | --- | --- |
| [01](#01--update-before-snapshot) | ex8 okx | update with no baseline |
| [02](#02--happy-path) | ex8 okx | the full happy path |
| [03](#03--sequence-gap--resync) | ex8 okx | gap → awaiting_snapshot → resync |
| [04](#04--stale--duplicate) | ex8 okx | duplicate + out-of-order snapshot |
| [05](#05--precision--dust) | ex8 okx | truncation, price collision, dust delete |
| [06](#06--noise-frames) | ex8 okx | non-book frames are discarded |
| [07](#07--wallex-half-books-ex3) | ex3 wallex | null sequence, half-book merge |

---

## 01 — update before snapshot

**`01-ex8-update-before-snapshot/`** — the case you asked for first. This is the one scenario
that genuinely depends on a fresh validator, which the reset guarantees.

| # | File | `ts` | Expected |
| --- | --- | --- | --- |
| 01 | `update-no-baseline` | B+300 | **REJECTED** `no_baseline` → `ex8-p1-rejected-flink`. Nothing reaches the UI. |
| 02 | `snapshot` | B+600 | **ACCEPTED** — becomes the baseline. UI shows 5 asks / 5 bids around 62770. |
| 03 | `update` | B+900 | **ACCEPTED** (+300). |

After 03: ask `62772` **gone** (qty `"0"` delete), ask `62771` → `0.29045069`, new ask `62790`,
bid `62769` → `0.55175335`, new bid `62758`.

The point: an update carries no book of its own, so applying one without a baseline would
invent liquidity. Job 2 refuses it; job 5 never sees it.

**Dead letter: 1 record** — `no_baseline`.

## 02 — happy path

**`02-ex8-happy-path/`** — snapshot, then three strictly consecutive updates.

| # | File | `ts` | Expected |
| --- | --- | --- | --- |
| 01 | `snapshot` | B+300 | baseline: 5 asks / 5 bids in the 62800 band. |
| 02 | `update-modify-add-delete` | B+600 | ask `62800` → `1.0`, ask `62802` **deleted**, ask `62807` **added**, bid `62799` → `1.45`. |
| 03 | `update-delete-bid` | B+900 | ask `62803` added, bid `62798` **deleted**, bid `62780` added. |
| 04 | `update-asks-only` | B+1200 | **no `bids` key at all** — ask `62800` → `0.9`, ask `62810` deleted, **bids untouched**. |

04 is the deliberately interesting one: a missing side is `null`, not empty. Job 5 must leave
the bid side alone. If the bids disappear from the UI after 04, that null/empty distinction is
broken.

**Dead letter: 0 records.**

## 03 — sequence gap → resync

**`03-ex8-sequence-gap/`** — okx's expected jump is exactly **300**.

| # | File | `ts` | step | Expected |
| --- | --- | --- | --- | --- |
| 01 | `snapshot` | B+300 | — | baseline, 62800 band. |
| 02 | `update-ok` | B+600 | +300 | ACCEPTED. |
| 03 | `update-gap` | B+1500 | **+900** | **REJECTED** `sequence_gap`, sets `awaitingSnapshot`. `lastSeq` stays B+600. |
| 04 | `update-awaiting-snapshot` | B+1800 | +300 | **REJECTED** `awaiting_snapshot` — correct jump, but the book is known-diverged. |
| 05 | `snapshot-resync` | B+2100 | — | ACCEPTED, clears `awaitingSnapshot`, **replaces** the book. |
| 06 | `update-ok` | B+2400 | +300 | ACCEPTED again. |

04 is the subtle one: its jump is perfectly valid. It is still rejected, because once a gap is
missed the local book no longer matches the exchange and only a snapshot can be trusted.

05 deliberately jumps to a **different price band (63000s)**, so the replacement is unmistakable
in the UI — no 62800-band level should survive it.

**Dead letter: 2 records** — `sequence_gap`, then `awaiting_snapshot`.

## 04 — stale / duplicate

**`04-ex8-stale-duplicate/`**

| # | File | `ts` | Expected |
| --- | --- | --- | --- |
| 01 | `snapshot` | B+300 | baseline, 63000 band. |
| 02 | `update-ok` | B+600 | ACCEPTED. |
| 03 | `update-replay-duplicate` | B+600 | **REJECTED** `stale_or_duplicate` — byte-identical replay of 02. |
| 04 | `snapshot-out-of-order` | **B+150** | **REJECTED** `stale_or_duplicate` — a *snapshot*, but older than `lastSeq`. |
| 05 | `snapshot-ok` | B+900 | ACCEPTED. |

04 carries absurd levels (ask `69999`, bid `10000`) purely so the failure is loud: **if those
prices ever appear in the UI, the out-of-order snapshot check is not running.** A newer snapshot
always wins; an older one must never overwrite a fresher book.

Neither rejection advances `lastSeq`, which is why 05 at B+900 is still accepted.

**Dead letter: 2 records** — `stale_or_duplicate` twice (one update replay, one backwards
snapshot).

## 05 — precision & dust

**`05-ex8-precision-dust/`** — `price_precision 2`, `quantity_precision 8`, truncation is
`RoundingMode.DOWN` (never rounds up — that would invent size).

`01-snapshot` (B+300) expectations after job 4:

| Wire | Becomes | Why |
| --- | --- | --- |
| ask `62900.1234` @ `3.1234567891` + ask `62900.1289` @ `2.0000000099` | **one** level `62900.12` @ `5.12345679` | **collision: merged, quantities summed** |
| ask `62902.999` @ `4` | `62902.99` @ `4` | truncates DOWN, not to `62903.00` |
| ask `62905.10` @ `0.0000000090` | qty → `"0"` | **dust: level deleted, never shown** |
| ask `62906.1234` @ `0.000000006` + ask `62906.1299` @ `0.000000006` | **one** level `62906.12` @ `0.00000001` | **two dusts that SURVIVE by summing** |
| bid `62899.0567` @ `5.9876543219` + bid `62899.0512` @ `1` | **one** level `62899.05` @ `6.98765432` | collision on the bid side too |
| bid `62898.9999` @ `7.25` | `62898.99` @ `7.25` | |

**7 asks in → 5 levels out; 5 bids in → 4 levels out.** The collisions are the thing to watch:
truncating prices makes distinct wire prices land on the same book price, and job 4 merges them
into one level carrying the **sum** of their quantities (user decision 2026-07-20). Previously it
emitted both, and job 5 — which keys its `MapState` by canonicalized price — silently kept only
the last, losing the other's liquidity.

`62906.1234`/`62906.1299` is the subtle pair: each quantity alone truncates to `"0"` and would be
a delete, but job 4 sums the RAW quantities and truncates the sum ONCE, so `0.000000012` becomes
`0.00000001` and the level **survives**. Summing after truncating would wrongly delete it.

`02-update-dust-delete` (B+600) then sets live ask `62901.55` and live bid `62897.50` to
sub-precision quantities. Both truncate to `"0"`, which job 5 reads as a **delete** — the levels
vanish from the UI. Intended, per the M5 decision: size below the lot precision is not
representable liquidity. Asks `62910.25` @ `6` and `62910.2599` @ `1.5` are added in the same
message as a control — they must appear as **one** level `62910.25` @ `7.5`, which also proves the
merge applies to `update` frames, not just snapshots. (On an update a quantity is a replacement
rather than an increment, so summing collided rows is a deliberate approximation — job 4 is
stateless and cannot know what the other collided price still rests at.)

**Dead letter: 0 records** — job 4 has no dead-letter path at all, deliberately unlike job 3.

## 06 — noise frames

**`06-ex8-noise-frames/`** — job 1 whitelist-parses: anything that is not a recognized book
frame is **silently discarded** (dropped, *not* dead-lettered, never a crash).

| # | File | `ts` | Expected |
| --- | --- | --- | --- |
| 01 | `subscribe-ack` | — | discarded (no `action`) |
| 02 | `error-frame` | — | discarded |
| 03 | `snapshot` | B+300 | **ACCEPTED** — baseline, 62950 band |
| 04 | `unknown-action` | B+450 | discarded — `action: "partial"` is neither `snapshot` nor `update`. Carries `9999` quantities so a leak is obvious. |
| 05 | `other-channel` | B+500 | discarded — a `trades` frame; has `data[].ts` but no `asks`/`bids`. |
| 06 | `update` | B+600 | **ACCEPTED** (+300 from 03). |

Noise is interleaved with real frames on purpose — the two discards sitting *between* the
snapshot and the update are what prove a dropped frame does not disturb sequence tracking.

Success = the book after 06 reflects exactly 03 + 06 (ask `62955` deleted, ask `62970` added,
bid `62945` → `1.65`), and **nothing appears in `ex8-p1-rejected-flink`**. A discard is not a
rejection; that distinction is the whole scenario.

Note 04 and 05 sit off the 300 ms grid deliberately: they are dropped by job 1 and never reach
the validator, so their `ts` is irrelevant. If either ever *does* reach job 2, it surfaces as a
`sequence_gap` — a useful secondary signal.

**Dead letter: 0 records.** Any record here is a real failure.

> Not covered: a non-JSON frame (a bare `pong` string). Whether job 1 drops or throws on
> unparseable bytes is **unverified**; I did not want a manual-test file that might wedge the
> job. Worth confirming separately (tracked in `todo.md`).

## 07 — wallex half-books (ex3)

**`07-ex3-wallex-half-book/`** — same pair, different exchange. Included because ex8
structurally cannot exercise these two behaviours.

| # | File | Expected |
| --- | --- | --- |
| 01 | `buy-depth` | bids only; `asks` is **null**. 3 wallex bids appear, **no wallex asks**. |
| 02 | `sell-depth` | asks only. Both wallex sides now present — the half-books merged. |
| 03 | `buy-depth-refresh` | bid side **replaced** (`62942.5`, `62927.5`; the other two gone), **asks survive**. |

Two things under test:

1. **`sequence_id` is null** → job 2 passes every message through unchecked. Wallex has no
   ordering field, so there is no staleness protection here at all — by design, documented.
2. **`null` side ≠ empty side.** 02 must not wipe the bids and 03 must not wipe the asks. This
   is the merge point ex8 cannot reach, since okx always sends `asks`/`bids` together (except
   02/04, the synthetic version of the same question).

The step-01 assertion ("no asks yet") is exactly why this scenario needs the book-builder reset
and not just the validator reset — a previous run's asks would otherwise still be sitting in
job 5's state.

**Wallex prices all end in `.5`** (`62942.5`, `62952.5`, …). That is so they stay attributable
if you ever run this alongside an ex8 scenario with `--no-reset`: the consolidator unions the
two exchanges on pair 1, and the `.5` levels interleave with — never collide with — ex8's
integer bands. Synthetic on purpose; two real exchanges would quote overlapping prices. To
verify ex3 in isolation, read `ex3-p1-*` directly.

Wallex also sends prices/quantities as **JSON numbers**, not strings — the one feed in scope
with that hazard.

**Dead letter: 0 records.**

---

## Where to look when something disagrees

Per stage, for pair 1:

```
ex8-raw                           # what you sent, verbatim
ex8-p1-raw-flink                  # job 1 parsed  (routing, type, sequence_id)
ex8-p1-type-validated-raw-flink   # job 2 accepted
ex8-p1-rejected-flink             # job 2 rejected — carries the reason string
ex8-p1-rebased-flink              # job 3
ex8-p1-applied-precision-flink    # job 4 (scenario 05 shows up here)
ex8-p1-orderbook-snapshot-flink   # job 5 full book
ex8-p1-{side}                     # job 6 → consolidator → UI
```

(Scenario 07 is the same chain under `ex3-p1-*`.)

The dead-letter topic carries the rejection reason, so 01/03/04 are checked there directly
rather than inferred from the UI. Each scenario states its expected dead-letter count above;
those counts are per-run, so read only the records produced since your reset.

| Scenario | Expected dead-letter records |
| --- | --- |
| 01 | 1 — `no_baseline` |
| 02 | 0 |
| 03 | 2 — `sequence_gap`, `awaiting_snapshot` |
| 04 | 2 — `stale_or_duplicate` ×2 |
| 05 | 0 |
| 06 | 0 |
| 07 | 0 |

More than expected means something valid was rejected; fewer means a check is not firing.

If results still look wrong, confirm the reset actually completed — `reset.sh` prints each job
reaching `RUNNING`. A job that failed to resubmit leaves the chain broken and every scenario
downstream of it silent.
