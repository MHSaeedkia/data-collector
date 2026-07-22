# Manual test data — BTC-USDT raw payloads for the normalizer chain

Hand-crafted `ex{id}-raw` payloads for **manual** end-to-end testing: produce them to the raw
topic, let all 6 normalizer jobs run, and watch the result land in the order book web UI.

**Everything targets one market: BTC-USDT (`pair_id` 1).** ex8/okx uses the wire symbol
`BTC-USDT`, ex1/nobitex and ex3/wallex use `BTCUSDT`; all resolve to `market_id` 1 in the seed.

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

`reset.sh` cancels and resubmits the three **stateful** jobs, downstream-first:

| Job | State it holds |
| --- | --- |
| `normalizer-aggregator` | per-exchange book + last `event_time`, keyed per `(pair, side)` |
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

### Why the script rewrites the timestamp (ex8 and ex1)

For okx, `ts` is simultaneously the **sequence id**, the **event time**, and the only timestamp
on the message. The files carry a fixed synthetic base (`1800000000000`) so the timelines are
readable and diffable — but that base sits in the **future** relative to real time.

Sending it verbatim would be actively harmful, not merely odd: the aggregator drops events
older than the one it has stored, so a future-dated book would poison the stored timestamp and
every subsequent *real* event would be dropped until wall-clock caught up.

So each scenario's timestamp window is shifted onto now, preserving every delta **within** that
scenario — for ex8 the +300 steps and the deliberate gap, duplicate and backwards snapshot all
survive. `--verbatim` is not offered here; the shift is per-scenario and always correct.

**ex1/nobitex** carries `lastUpdate` (top-level on the REST snapshot, `push.pub.data.lastUpdate`
on the WS delta), which is **only** the event time — its ordering field is the independent
`push.pub.offset`. So the script shifts `lastUpdate` onto now for the same aggregator-poisoning
reason, but leaves the **offsets untouched** (they're small readable integers: `1000`, `1001`,
…). The +1 contiguity, the deliberate gap, and the resync re-anchor all live in the offsets, so
they are preserved exactly. Unlike ex8 there is no cadence alignment — `lastUpdate` and the
sequence are decoupled.

ex3/wallex has no ordering field at all and is stamped with processing time by job 1, so
nothing is rewritten there.

---

## Before you start

- normalizer stack up (`docker-compose.yml`), all 6 jobs submitted (aggregator downstream-first)
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
| [08](#08--ex1-rest-snapshot--ws-resync) | ex1 nobitex | REST snapshot arms baseline, first WS update adopts its offset; re-anchor |
| [09](#09--ex1-update-before-snapshot) | ex1 nobitex | WS update before any REST snapshot → `no_baseline` |
| [10](#10--ex1-sequence-gap--rest-resync) | ex1 nobitex | offset gap → `awaiting_snapshot` → REST resync |
| [11](#11--ex1-noise-frames) | ex1 nobitex | Centrifugo non-book frames are discarded |
| [12](#12--ex1-stale-rest-replay) | ex1 nobitex | old REST snapshot replayed after WS deltas → `out_of_order` |

ex1/nobitex `ts` offsets below are shown relative to each scenario's own base `LU = 1800000000000`
(the `lastUpdate` field); its **sequence offsets are literal** (not shifted) — see the timestamp
note above.

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
if you ever run this alongside an ex8 scenario with `--no-reset`: the aggregator unions the
two exchanges on pair 1, and the `.5` levels interleave with — never collide with — ex8's
integer bands. Synthetic on purpose; two real exchanges would quote overlapping prices. To
verify ex3 in isolation, read `ex3-p1-*` directly.

Wallex also sends prices/quantities as **JSON numbers**, not strings — the one feed in scope
with that hazard.

**Dead letter: 0 records.**

---

## ex1/nobitex — a REST snapshot + WS delta feed

Scenarios 08–12 exercise nobitex, which is **not** structurally like any of ex8/ex3. nobitex
serves the initial book once over **REST** and then only **deltas** over WebSocket, so NiFi
publishes **two payload shapes** to `ex1-raw`:

- **REST snapshot** — top-level `"action":"snapshot"`, NiFi injects the market as `"pair"`, and it
  carries **no offset**: job 1 stamps `type=snapshot`, `sequence_id=null`, event_time=`lastUpdate`.
- **WS delta** — a Centrifugo push on channel `public:orderbook-BTCUSDT`, **no `action`** field:
  job 1 stamps `type=update`, `sequence_id=push.pub.offset`, **`sequence_jump=1`** (Centrifugo
  offsets increment by exactly one), event_time=`push.pub.data.lastUpdate`.

The interesting bit is in job 2. The REST snapshot's `sequence_id` is null, so it **cannot seed
`lastSeq`** the way an okx snapshot does. Instead a null-seq snapshot **arms a resync**: the
**first WS update after it adopts that update's offset as the baseline unconditionally** (no gap
check), and normal `+1` contiguity resumes from there. Scenarios 08 and 10 are built around that
mechanism; ex8 cannot reach it because okx snapshots always carry a real sequence.

> These ex1 scenarios have only been unit-verified so far. They need NiFi's REST feed live on
> `ex1-raw` **and** the coupled job-2 resync build deployed; a partial rollout makes every ex1
> update reject `no_baseline` (see `todo.md` M10).

## 08 — ex1 REST snapshot → WS resync

**`08-ex1-rest-then-ws-resync/`** — the signature nobitex path, plus a mid-stream re-anchor.

| # | File | offset | Expected |
| --- | --- | --- | --- |
| 01 | `rest-snapshot` | — (null) | **ACCEPTED**, passes through unchecked, **arms the resync**. Baseline: 5 asks / 5 bids in the 62650 band. |
| 02 | `ws-update` | 1000 | **ACCEPTED** — first update after the null-seq snapshot **adopts 1000 as the baseline**. ask `62652` deleted, ask `62670` added, bid `62649`→`0.55175335`, bid `62638` added. |
| 03 | `ws-update` | 1001 | **ACCEPTED** (+1). ask `62651` deleted, ask `62680` added, bid `62638` deleted, bid `62635` added. |
| 04 | `rest-snapshot` | — (null) | **ACCEPTED** — a fresh REST snapshot **re-arms** the resync and **replaces** the book (jumps to the 62850 band). |
| 05 | `ws-update` | **9000** | **ACCEPTED** — first update after the re-anchor adopts `9000` **unconditionally**, even though it is nowhere near `1002`. This is the whole point: a re-anchor does **not** gap-check. |
| 06 | `ws-update` | 9001 | **ACCEPTED** (+1 from the new baseline). |

05 is the one to watch: `9000` after `1001` would be a screaming `sequence_gap` on any other feed.
Here the preceding snapshot re-armed the baseline, so it is adopted, not rejected. If 05 lands in
`ex1-p1-rejected-flink`, the re-anchor is broken.

**Dead letter: 0 records.**

## 09 — ex1 update before snapshot

**`09-ex1-update-before-snapshot/`** — nobitex's version of the "no baseline yet" case. Realistic:
the WS subscription can deliver a delta before the REST fetch has returned the initial book.

| # | File | offset | Expected |
| --- | --- | --- | --- |
| 01 | `ws-update-no-baseline` | 1000 | **REJECTED** `no_baseline` → `ex1-p1-rejected-flink`. Nothing reaches the UI. |
| 02 | `rest-snapshot` | — (null) | **ACCEPTED** — baseline, 62650 band, arms the resync. |
| 03 | `ws-update` | 2000 | **ACCEPTED** — first update after the snapshot adopts `2000` as the baseline. |

An update carries no book of its own; applying one with no baseline would invent liquidity. Unlike
okx, ex1's snapshot does not seed the sequence directly — so this proves the *update* is what
adopts the baseline, and only after a snapshot has armed it.

**Dead letter: 1 record** — `no_baseline`.

## 10 — ex1 sequence gap → REST resync

**`10-ex1-sequence-gap/`** — Centrifugo offsets increment by exactly **1**, so any skip is a gap.

| # | File | offset | step | Expected |
| --- | --- | --- | --- | --- |
| 01 | `rest-snapshot` | — (null) | — | baseline, 62650 band, arms the resync. |
| 02 | `ws-update` | 1000 | adopt | **ACCEPTED** — adopts `1000` as the baseline. |
| 03 | `ws-update-ok` | 1001 | +1 | **ACCEPTED**. |
| 04 | `ws-update-gap` | **1005** | +4 | **REJECTED** `sequence_gap`, sets `awaitingSnapshot`. `lastSeq` stays `1001`. |
| 05 | `ws-update-awaiting` | 1002 | +1 | **REJECTED** `awaiting_snapshot` — a *perfectly contiguous* `1002`, but the book is known-diverged, so only a snapshot can be trusted. |
| 06 | `rest-snapshot-resync` | — (null) | — | **ACCEPTED**, clears `awaitingSnapshot`, **re-arms** the resync, **replaces** the book (63000 band). |
| 07 | `ws-update` | 2000 | adopt | **ACCEPTED** — adopts `2000` as the new baseline. |
| 08 | `ws-update-ok` | 2001 | +1 | **ACCEPTED**. |

05 is the subtle one: `1002` *is* the correct successor to `1001`, yet it is still rejected. Once a
gap is missed the local book no longer matches the exchange, and for ex1 only a **REST snapshot**
(06) can re-arm the resync — a contiguous update cannot self-heal. 06 jumps to the **63000 band** so
the replacement is unmistakable in the UI; no 62600-band level should survive it.

**Dead letter: 2 records** — `sequence_gap`, then `awaiting_snapshot`.

## 11 — ex1 noise frames

**`11-ex1-noise-frames/`** — job 1 whitelist-parses ex1 too. nobitex's noise is Centrifugo-shaped,
structurally unlike okx's: a bare `connect` reply, publications on other channels, and malformed
book frames are all **silently discarded** (dropped, *not* dead-lettered, never a crash).

| # | File | offset | Expected |
| --- | --- | --- | --- |
| 01 | `connect-ack` | — | discarded — a Centrifugo `{"connect":…}` reply, no `push`. |
| 02 | `foreign-channel` | — | discarded — a `public:trades-BTCUSDT` push; wrong channel prefix. |
| 03 | `rest-snapshot` | — (null) | **ACCEPTED** — baseline, 62950 band, arms the resync. |
| 04 | `malformed-book` | — | discarded — right channel, but the publication has **no `asks`/`bids`**. |
| 05 | `ws-update` | 1000 | **ACCEPTED** — adopts `1000`; the discarded 04 did **not** consume an offset or arm anything. |
| 06 | `ws-update` | 1001 | **ACCEPTED** (+1). |

Noise is interleaved with real frames on purpose — the discard sitting *between* the snapshot (03)
and the first update (05) is what proves a dropped frame does not disturb baseline adoption or
sequence tracking. A discard is **not** a rejection; that distinction is the whole scenario.

**Dead letter: 0 records.** Any record here is a real failure.

---

## 12 — ex1 stale REST replay

**`12-ex1-stale-rest-replay/`** — a REST snapshot has **no sequence offset**, so job 2 can only
order these frames by **event time** (`lastUpdate`). This scenario replays an **old** REST snapshot
after newer WS deltas have already advanced the book: without the event-time guard the stale
snapshot would overwrite the newer book *and* wrongly re-arm the resync. File 04 is byte-identical
to file 01 — the same old book, replayed.

| # | File | offset | `LU` | Expected |
| --- | --- | --- | --- | --- |
| 01 | `rest-snapshot` | — (null) | LU+0 | **ACCEPTED** — baseline, 62650 band, arms the resync. |
| 02 | `ws-update` | 1000 | LU+100 | **ACCEPTED** — adopts `1000` as the baseline. |
| 03 | `ws-update-loud` | 1001 | LU+200 | **ACCEPTED** (+1) — adds a loud `60000` bid wall so a book leak is visible. |
| 04 | `rest-snapshot-stale-replay` | — (null) | **LU+0** | **REJECTED** `out_of_order` — event time LU+0 < last accepted LU+200; must not overwrite the book or re-arm the resync. |
| 05 | `ws-update` | 1002 | LU+300 | **ACCEPTED** (+1) — the stream continues; the stale snapshot did **not** disturb baseline or sequence state. |

The oracle is the dead-letter count, exactly `1` (`out_of_order`). If the guard were missing the
stale snapshot would be accepted (dead-letter `0`), the `60000` wall would vanish, and `1002` would
be adopted as a fresh baseline rather than validated as `1001+1`.

**Dead letter: 1 record** — `out_of_order`.

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
ex8-p1-orderbook-snapshot-flink   # job 5 full book (consumed by the aggregator)
p1-{side}                         # aggregator output (union of all exchanges) → UI
```

(Scenario 07 is the same chain under `ex3-p1-*`; scenarios 08–12 under `ex1-p1-*`.)

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
| 08 | 0 |
| 09 | 1 — `no_baseline` |
| 10 | 2 — `sequence_gap`, `awaiting_snapshot` |
| 11 | 0 |
| 12 | 1 — `out_of_order` |

More than expected means something valid was rejected; fewer means a check is not firing.

If results still look wrong, confirm the reset actually completed — `reset.sh` prints each job
reaching `RUNNING`. A job that failed to resubmit leaves the chain broken and every scenario
downstream of it silent.
