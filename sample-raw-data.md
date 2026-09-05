# Raw Topic Sample Data (`ex{id}-raw`)

> **RESET 2026-07-14**: the 2026-07-13 bulk capture (kafka-ui latest-200 per topic) was
> discarded — samples are being rebuilt **one exchange at a time**. The old file is
> recoverable from git if ever needed. Each section below gets filled as its sample is
> captured and verified.
>
> Findings from the discarded capture (regimes, envelopes, wire types, seq anomalies) are
> summarized in `memory/project_raw_pipeline_decision.md` — treat them as **to re-verify**
> while each exchange's sample is rebuilt, not as confirmed ground truth.
>
> **Message types (rule, FINAL 2026-07-14)**: raw topics carry **snapshot** and **update**
> messages, and MAY contain **other data** (acks, pings, other channels, …). **Anything that
> is not a recognized book message is silently discarded by job 1** — whitelist parse: drop,
> never crash, never dead-letter. User decision 2026-07-14: capturing example non-book frames
> is NOT required; the drop rule is simply "not a recognized book frame ⇒ discard".

| Topic     | Exchange | Sample status                 |
| --------- | -------- | ----------------------------- |
| `ex1-raw` | nobitex  | ✅ captured 2026-07-14; ⚠ REVISED 2026-07-21 (WS = delta) then **REVISED AGAIN 2026-09-02** (WS = snapshot) — two streams, BOTH full snapshots: REST (`action`+`pair`, null-seq) + WS (Centrifugo `pub`, ordered by `pub.offset`) |
| `ex2-raw` | bitpin   | ✅ captured 2026-07-14; ⚠ REVISED 2026-07-25 (WS = delta) then **REVISED AGAIN 2026-09-02** (WS = snapshot) — two streams, BOTH full snapshots: REST (`action`+`pair`, null-seq) + WS (Centrifugo `pub`, ordered by `pub.offset`) |
| `ex3-raw` | wallex   | ✅ captured 2026-07-14 (per-side snapshots) |
| `ex4-raw` | ramzinex | ✅ captured 2026-07-14 (snapshot) |
| `ex5-raw` | bitget   | ✅ captured 2026-07-14; ⚠ **REVISED 2026-08-22** — channel changed `books50` → `depth`/`scale`: now snapshot **+ update**, `seq` GONE, sequence = inner `ts`; **+ a REST snapshot stream captured 2026-08-23** (`data` object, `a`/`b`, NUMERIC levels, injected `pair`). ⚠ **RE-MEASURED live 2026-08-23**: the WS channel sends **no snapshots at all**, the REST body is the only baseline and is now **null-seq**, and the update window is **650 ± 110** |
| `ex6-raw` | bybit    | ✅ captured 2026-07-14 (snapshot + delta; qty="0" delete frame still to capture); **+ a REST snapshot stream captured 2026-08-24** (`result` object, `a`/`b`, string levels, injected `action`/`pair`) — **null-seq**: `result.u` is on a DIFFERENT counter from the WS `data.u` |
| `ex7-raw` | ompfinex | **POSTPONED** (2026-07-14, raw-data issue) — out of initial scope |
| `ex8-raw` | okx      | ✅ captured 2026-07-14 (snapshot + update; qty="0" delete CONFIRMED on wire) |
| `ex9-raw` | lbank    | ✅ captured 2026-08-25 (snapshot only — every frame is a full book; **no sequence field anywhere on the wire**, so null-seq/jump 0, ordered by `TS` alone) |

---

## NiFi-injected fields: `simulation` and `id`

**Every sample below shows them.** They are not part of any exchange's wire format — NiFi adds
them to each record it publishes to a raw topic, and job 1 reads them off the payload root.

| Field        | Added      | Meaning |
| ------------ | ---------- | ------- |
| `simulation` | 2026-08-03 | `0` or absent = live data, `1` = simulation data. Other values are undefined and carried through verbatim |
| `id`         | 2026-08-03 | A **fresh UUID per record**, minted by NiFi when it writes that record. It is the first link of the lineage chain: job 1 copies it into the emitted event's `source_ids`. Named `sink_id` until 2026-08-04 |

**⚠ `id` is mandatory — job 1 DROPS any payload without one** (`dropped-no-id` counter, WARN log).
That makes NiFi a hard dependency: a processor that does not inject `id` loses 100% of its data,
silently apart from the counter, so **NiFi's change must ship BEFORE the job jars, never after**. A
missing `simulation` is by contrast benign and simply reads as `0`.

Both ride in the same place, which depends only on the shape of the payload root:

- **ex1, ex2, ex4, ex5, ex6, ex7, ex8, ex9 — object root** ⇒ injected as **root fields**, alongside whatever
  the exchange already sends. For the Centrifugo payloads (ex1/ex2 WS, ex4) that means the OUTER
  object, next to `push` — not inside `data`.
- **ex3/wallex — array root** ⇒ there is no root field to inject into, so both go in a **trailing
  metadata object as element index 2**: `["{market}@{side}", [levels…], {"simulation":1,"id":"…"}]`.
  **Never a fourth element** — job 1 reads index 2 and drops any envelope longer than 3.

The samples below all show `"simulation": 1`; a live feed sends `0` (or omits it). The `id` values
are illustrative — every real record carries its own.

---

## ex1-raw — nobitex

**⚠ REVISED 2026-07-21 — the "only snapshots" assumption was WRONG** (nobitex serves the initial
book over REST and — we believed then — only **deltas** over WebSocket). **⚠ REVISED AGAIN
2026-09-02 — that correction itself was wrong**: fresh live captures (`nobitex-snapshots.txt`)
show every WS push resends the WHOLE book — a level absent from one push and present in the last
is a silent delete, with no zero-quantity entry marking it, which a true delta feed cannot do. So
`ex1-raw` publishes **two distinct payloads, BOTH full snapshots**:

1. **REST snapshot** — NiFi tags it `"action": "snapshot"` and **injects the market as a
   top-level `"pair"` field** (the REST body has no symbol of its own). `type = "snapshot"`,
   `sequence_id = null` (no offset on the wire).
2. **WebSocket snapshot** — the Centrifugo push we already consumed, **unchanged** and with **no
   `action` field** → also `type = "snapshot"`, but ordered by its own counter rather than event
   time: `sequence_id = pub.offset`, `sequence_jump = 0` (unchecked on a snapshot — job 2 never
   jump-checks these, only rejects `seq <= last` as `stale_or_duplicate`).

Both branches are null-seq-or-sequenced SNAPSHOTS now, so the null-seq REST resync bootstrap in
job 2 (`baselinePending`) is set on every accepted REST snapshot but never consumed — the same
"set but never consumed" shape ex3/ex9 already have, since there is no `update` type on this
exchange for a WS event to adopt a baseline from any more (see memory/project_type_validator.md).

**REST snapshot sample** (level arrays trimmed):

```json
{"id":"9f2b1c74-3d5e-4a81-b0c6-71e2d4a95c38","simulation":1,"action":"snapshot","pair":"BTCUSDT","status":"ok","lastUpdate":1784614865284,"lastTradePrice":"65708.96","bids":[["65660","0.000615"],["65636","0.002543"]],"asks":[["65708.76","0.00672"],["65708.79","0.09133"]]}
```

**WebSocket snapshot sample** (Centrifugo envelope; pretty-printed; level arrays trimmed — a real
message carried **24 levels per side**, so depth is NOT the fixed 50 bitpin uses; possibly
variable):

```json
{
  "id": "4c8e0a12-7b93-4f6d-9e21-0a5c3b8d71f4",
  "simulation": 1,
  "push": {
    "channel": "public:orderbook-BTCUSDT",
    "pub": {
      "data": {
        "asks": [
          ["62678", "0.000963"],
          ["62679.87", "0.004151"],
          ["62679.91", "0.004663"]
        ],
        "bids": [
          ["62669", "0.010863"],
          ["62600", "0.110842"],
          ["62571.82", "0.031963"]
        ],
        "lastTradePrice": "62669",
        "lastUpdate": 1784021328931
      },
      "offset": 33259
    }
  }
}
```

Parsing notes (job 1):

- **Branch discriminator**: top-level `action == "snapshot"` ⇒ REST snapshot path; otherwise
  try the Centrifugo `push` (WS snapshot); anything else is noise → dropped.
- **REST snapshot market**: from the injected top-level `"pair"` field (`BTCUSDT`). The REST
  body has no channel and no `symbol`.
- **WS snapshot market**: from the channel `public:orderbook-{market}` (here `BTCUSDT`) — differs
  from bitpin's `orderbook:{market}`. NO `symbol` field inside `data`; the channel is the ONLY
  market key on the WS side.
- **Levels** (both payloads): `bids`/`asks` are `[price, qty]` **string** pairs ✅ (asks listed
  first; asks price-ascending, bids price-descending). Prices may lack decimals (`"62678"`). Each
  payload is the FULL current book — a price absent from one push and present in the last is a
  silent delete, not "nothing changed there."
- **`lastTradePrice` is a string** ✅ (unlike bitpin's numeric `price`); `lastUpdate` is
  epoch-millis as a JSON number → event time — both metadata, not book levels.
- **Ordering / job-2 rule**: the REST snapshot has no offset → `sequence_id = null` (event-time
  ordered, out-of-order-only check). The WS snapshot uses `pub.offset` as `sequence_id` with
  `sequence_jump = 0` — also an out-of-order-only check (`seq <= last` → `stale_or_duplicate`),
  never a jump/gap check, because job 2 never jump-checks a snapshot of any kind.
- **Multi-doc records: CLOSED 2026-07-14 (user)** — ex1 records always contain ONE JSON
  document; the discarded-capture 2-newline-concatenated-docs lead was an artifact. No
  splitting logic in job 1.

## ex2-raw — bitpin

**⚠ REVISED 2026-07-25 — the "full snapshot on every message" assumption was WRONG**, exactly as
it was for ex1. **⚠ REVISED AGAIN 2026-09-02 — that correction itself was wrong**, exactly as it
was for ex1: fresh live captures (`bitpin-snapshots.txt`) show every WS push resends the WHOLE
book (same silent-delete tell as ex1 — a level dropped between pushes with no zero-quantity entry
marking it). So `ex2-raw` publishes **two distinct payloads, BOTH full snapshots**:

1. **REST snapshot** — NiFi tags it `"action": "snapshot"` and **injects the market as a
   top-level `"pair"` field** (the REST body has no symbol of its own). `type = "snapshot"`,
   `sequence_id = null` (no offset on the wire).
2. **WebSocket snapshot** — the Centrifugo push we already consumed, **unchanged** and with **no
   `action` field** → also `type = "snapshot"`, ordered by its own counter: `sequence_id =
   pub.offset`, `sequence_jump = 0` (unchecked on a snapshot — job 2 never jump-checks these, only
   rejects `seq <= last` as `stale_or_duplicate`).

Both branches are null-seq-or-sequenced SNAPSHOTS now — the same shape as ex1 (see its section
above), including the null-seq REST resync bootstrap being set but never consumed since there is
no `update` type on this exchange for a WS event to adopt a baseline from any more (see
memory/project_type_validator.md).

**REST snapshot sample** (level arrays trimmed):

```json
{"id":"2d7f6b90-8c14-4e3a-9d57-6b2f0c81ae43","simulation":1,"action":"snapshot","pair":"BTC_USDT","event_time":1784008564112,"asks":[["62714.50","0.01387100"],["62720.77","0.00970970"]],"bids":[["62672.30","0.01003106"],["62655.92","0.01368489"]]}
```

**⚠ The injected `pair` must be the DB market string `BTC_USDT`** (underscore), matching
`exchange_markets.market` for ex2 and the WS channel suffix — job 1's lookup key
`"2|{market}"` is exact and case-sensitive, so `BTCUSDT` would drop silently as
`dropped-unknown-market`. Confirmed with the user 2026-07-25.

**WebSocket snapshot sample** (Centrifugo envelope; pretty-printed; level arrays trimmed — the real
message carried **50 levels per side**):

```json
{
  "id": "e1a45c38-0f27-4b6e-8c93-5d71a2b0e964",
  "simulation": 1,
  "push": {
    "channel": "orderbook:BTC_USDT",
    "pub": {
      "data": {
        "bids": [
          ["62672.30", "0.01003106"],
          ["62655.92", "0.01368489"],
          ["62653.15", "0.00645139"]
        ],
        "asks": [
          ["62714.50", "0.01387100"],
          ["62720.77", "0.00970970"],
          ["62727.04", "0.00679679"]
        ],
        "volume_ask": "3.04336136",
        "volume_bid": "2.83692543",
        "symbol": "BTC_USDT",
        "event": "market_data",
        "price": 62687.34,
        "event_time": "2026-07-14T05:56:09.833955Z"
      },
      "offset": 11286199
    }
  }
}
```

Parsing notes (job 1):

- **Branch discriminator**: top-level `action == "snapshot"` ⇒ REST snapshot path; otherwise
  try the Centrifugo `push` (WS snapshot); anything else is noise → dropped.
- **REST snapshot market**: from the injected top-level `"pair"` field. The REST body has no
  channel and no `symbol`.
- **WS snapshot market**: from `push.channel` (`orderbook:{market}`, here `BTC_USDT`) and
  duplicated in `data.symbol`.
- **Levels** (both payloads): `bids`/`asks` are `[price, qty]` **string** pairs ✅
  (BigDecimal-from-string, no numeric-literal hazard for the levels). Bids sorted
  price-descending, asks ascending.
- **⚠ `data.price` is a JSON number** (last price, not a book level) — irrelevant to the book,
  but if ever read, it needs `USE_BIG_DECIMAL_FOR_FLOATS`.
- **`event` is always `market_data`** on the WS side — it is NOT a snapshot/update
  discriminator; the REST `action` field is.
- **⚠ `event_time` is the event time on BOTH payloads but in DIFFERENT wire types** (user
  2026-07-25): the WS `data.event_time` is an **ISO-8601 string** with microseconds
  (`Instant.parse`), the REST `event_time` is **epoch millis as a JSON number** (read verbatim).
  Same field name, two types — don't share a code path. It is the ONLY ordering signal on the
  REST snapshot, which is what job 2's null-seq `out_of_order` guard reads; the WS snapshot is
  ordered by `pub.offset` instead (`stale_or_duplicate` only, never jump-checked).
- **Ordering / job-2 rule**: the REST snapshot has no offset → `sequence_id = null` (resync).
  The WS delta uses `pub.offset` as `sequence_id` with `sequence_jump = 1`, so job 2 does real
  contiguity gap detection (was an out-of-order-only check when we thought it was a snapshot
  feed).
- `volume_ask`/`volume_bid`: metadata, not book levels.

## ex3-raw — wallex

**Captured 2026-07-14** (supplied by team). Regime **re-confirmed: full snapshot per SIDE** —
"only has snapshot and asks and bids are not in same message" (user statement). Each Kafka
record carries ONE side of the book; the two sides arrive as separate messages.

Samples (pretty-printed; level arrays trimmed — each real message carried **50 levels**):

```json
["BTCUSDT@buyDepth", [
  {"price": 62525.04, "quantity": 0.000451, "sum": 28.19879304},
  {"price": 62424.28, "quantity": 0.02624,  "sum": 1638.0131072},
  {"price": 62200,    "quantity": 0.068493, "sum": 4260.2646}
], {"simulation":1,"id":"7b3e9d02-5a6c-4f18-b2e7-9c04d81a6f35"}]
```

```json
["BTCUSDT@sellDepth", [
  {"price": 62579.56, "quantity": 0.004585, "sum": 286.9272826},
  {"price": 62619.76, "quantity": 0.002,    "sum": 125.23952},
  {"price": 62634.08, "quantity": 0.048566, "sum": 3041.88672928}
], {"simulation":1,"id":"c5d81f47-2e60-4a93-8b15-3f7c9e2d40a8"}]
```

Parsing notes (job 1):

- **Envelope**: NOT Centrifugo — the top level is a JSON **array**:
  `["{market}@{side}", [levels…], {"simulation": N, "id": "<uuid>"}]`. Market key + side both live
  in that first string (`BTCUSDT@buyDepth` / `BTCUSDT@sellDepth`); `buyDepth` = bids, `sellDepth`
  = asks.
- **⚠ `simulation` and `id` ride in a THIRD element (user 2026-08-03)**, not as root fields. Every
  other exchange has an OBJECT payload root, so NiFi injects them there directly; ex3's root is an
  array, so they are appended as a trailing metadata object instead — **both in the SAME object at
  index 2**, never as a fourth element (job 1 reads index 2 and drops any envelope longer than 3).
- **⚠ The 2-element form is effectively dead** (since 2026-08-03). Job 1 still parses it — it reads
  as `simulation = 0` and no `id` — but a payload with no `id` is then DROPPED, so ex3 must publish
  the 3-element form in practice.
- **⚠ Levels are objects with JSON-NUMBER `price`/`quantity`** (re-confirms the discarded-capture
  lead) — parsing MUST use Jackson `USE_BIG_DECIMAL_FOR_FLOATS` so BigDecimal comes from the
  decimal literal, never via double. Prices may lack decimals (`62200`).
- **`sum` = price × quantity per level** (verified on the sample; NOT cumulative) — derived
  notional, metadata; ignore for the book.
- **Sorting**: buyDepth (bids) price-descending, sellDepth (asks) price-ascending. 50 levels
  per message in both samples.
- **Per-side snapshots**: one message replaces ONE side only — the shared Avro event must
  express "snapshot of side X", and job 5 must merge sides (replace one side at a time)
  instead of assuming every snapshot carries both.
- **No seq field, no timestamp, no event/type field anywhere** — ⚠ the ONLY exchange with
  no usable ordering field, so the job-2 out-of-order check (REVISED 2026-07-14) cannot
  apply here: with 1 partition, Kafka offset = arrival order, which is exactly what we
  cannot validate against. ex3 gets no out-of-order protection.

## ex4-raw — ramzinex

**Captured 2026-07-14** (supplied by team). Regime **re-confirmed: full snapshot on every
message** (both sides present) — "for ramzinex we have snapshot" (user statement). Centrifugo
push envelope re-confirmed.

Sample (pretty-printed; level arrays trimmed — the real message carried **50 levels per side**):

```json
{
  "id": "6a0c2e91-4d38-4b75-9f62-8e1b3d5c07af",
  "simulation": 1,
  "push": {
    "channel": "orderbook:12",
    "pub": {
      "data": {
        "buys": [
          [62423.72, 0.011617, 725.17635524, false, null, 65, 1784025165152],
          [62423.71, 0.00005, 3.1211855, false, null, 10, 1784024541991],
          [62400, 0.00615, 383.76, false, null, 52, 1784024541991]
        ],
        "sells": [
          [64490, 0.011219, 723.51331, false, null, 65, 1784024634304],
          [64467.99, 0.0054599, 351.98877860100004, false, null, 50, 1784025196620],
          [62616.58, 0.00159, 99.5603622, false, null, 32, 1784025263854]
        ]
      },
      "offset": 5412464
    }
  }
}
```

Parsing notes (job 1):

- **Envelope**: Centrifugo `push` → `pub` → `data` like ex1/ex2, but the channel market key is
  a **numeric market id**: `orderbook:12` — NOT a symbol string. No symbol anywhere in `data`;
  the channel's `12` must match `exchange_markets.market` for ramzinex.
- **Sides named `buys`/`sells`** (not bids/asks), both in the same message.
- **⚠ Levels are 7-element arrays with JSON-NUMBER price/quantity** (re-confirms the
  discarded-capture lead) — `USE_BIG_DECIMAL_FOR_FLOATS` required. Prices may lack decimals
  (`62400`). Layout: `[price, quantity, notional, false, null, smallInt, epochMillis]`.
- **Elements 3–7 are metadata — ignore**: element 3 = price × quantity (verified on the
  sample; shows binary-float artifacts like `694.9048600000001`, i.e. producer computed it
  with doubles — one more reason to never touch doubles ourselves); element 4 always `false`,
  element 5 always `null` (meaning unknown); element 6 a small int (10–74 in this sample,
  meaning unverified — possibly order count); element 7 epoch-millis last-update per level.
- **⚠ Sorting: BOTH sides price-descending** — buys best-first (descending is natural), but
  `sells` are also descending, so the **best ask is the LAST element**. Don't assume
  best-first ordering when parsing. 50 levels per side in this sample.
- **No snapshot/update discriminator, no seq field in `data`** — **ordering field for the
  job-2 out-of-order check = `pub.offset`** (REVISED 2026-07-14, user — drop
  stale/out-of-order snapshots; no gap/jump rule). The per-level epoch-millis (element 7)
  is per-level, not per-message — not an ordering candidate.

## ex5-raw — bitget

**⚠ REVISED 2026-08-22 — the "snapshot-only" assumption was WRONG, and the ordering field is
GONE.** The feed moved off the `books50` channel onto the price-**grouped** `depth` channel
(`arg.params.scale`), which changes three things at once:

1. **`action` now has TWO values**, `"snapshot"` and `"update"` — bitget is a true delta feed,
   the fourth in scope after ex1/ex2 (REST+WS), ex6 and ex8. Qty `"0"` = level delete,
   **confirmed on the wire** (see the update sample).
2. **`seq` and `pseq` no longer exist.** The `checksum` that replaced them is a CRC
   book-integrity value — **not monotonic, not a sequence, unusable by job 2.**
3. `arg` gained `params.scale` and `instType` changed `"SPOT"` → `"sp"`; `arg.channel` is now
   `depth`, so depth is no longer encoded in the channel name the way `books50` did.

Because `seq` is gone the sequence id of a WS **update** is the inner **`ts`** (STRING epoch
millis), which is also the event time — the same double duty ex8/okx gives its `ts`. But where okx
publishes on an exact 300 ms cadence, bitget's is a wall clock on a **variable** cadence, so ex5 is
the ONE exchange with a nonzero `sequence_jump_tolerance`.

**⚠ REVISED 2026-08-23 (2) — measured against the live dev feed, 4569 frames / 36 minutes,
BTCUSDT only. Two things documented here were wrong:**

1. **The WS `depth` channel sends NO snapshots.** 3538 updates, **0** `action:"snapshot"` frames.
   The REST endpoint below is ex5's ONLY baseline source. (The snapshot sample in this section is
   a real capture, but it is not something the live feed currently produces.)
2. **The cadence is bimodal**, not 600: a 575–625 mass **plus a real 725–775 cluster**. Only
   93.2% of update→update transitions fell inside `600 ± 10`. The window is now **jump 650 ±
   110** = `[540, 760]` = 99.83% of 3537 live transitions; a genuinely missed tick (~1200 ms)
   still falls outside, so gap detection survives. See memory/project_type_validator.md.

**Snapshot sample** (level arrays trimmed — the real message carried **~160 asks and ~160 bids**;
depth is no longer fixed by the channel name):

```json
{
  "action": "snapshot",
  "arg": {
    "instType": "sp",
    "channel": "depth",
    "instId": "BTCUSDT",
    "params": { "scale": "0.01" }
  },
  "data": [
    {
      "asks": [
        ["77208.71", "0.755945"],
        ["77209.31", "0.140000"],
        ["77209.32", "0.259388"]
      ],
      "bids": [
        ["77208.70", "0.141942"],
        ["77208.54", "0.005000"],
        ["77206.03", "0.000019"]
      ],
      "checksum": 0,
      "ts": "1787404282388"
    }
  ],
  "ts": 1787404282388
}
```

**Update sample** (level arrays trimmed; note the qty-`"0"` deletes and the brand-new levels):

```json
{
  "action": "update",
  "arg": {
    "instType": "sp",
    "channel": "depth",
    "instId": "BTCUSDT",
    "params": { "scale": "0.01" }
  },
  "data": [
    {
      "asks": [
        ["77208.71", "0"],
        ["77209.31", "0"],
        ["77209.34", "0.005000"],
        ["77213.59", "0.005970"]
      ],
      "bids": [
        ["77209.33", "1.636034"],
        ["77208.71", "0.423759"],
        ["77201.53", "0"]
      ],
      "checksum": -1105358608,
      "ts": "1787404282410"
    }
  ],
  "ts": 1787404282410
}
```

**⚠ The two samples above are 22 ms apart** (`…388` → `…410`), nowhere near 600 — and a WS
snapshot→update pair like this does not occur on the live feed at all, since the channel sends no
snapshots. The transition that DOES occur is REST snapshot → WS update, and that one is no longer
window-checked: the REST body is null-seq, so job 2 takes the `baselinePending` bootstrap and the
next update re-anchors the baseline unconditionally. See the REST section below.

Parsing notes (job 1):

- **Envelope**: NOT Centrifugo — bitget's own WS shape: top-level `action` / `arg` / `data` /
  `ts`, the same family as ex8/okx. Market key is `arg.instId` (`BTCUSDT`, must match
  `exchange_markets.market`).
- **`data` is an ARRAY** containing the book object (one element in both samples) — the parser
  must unwrap the array, not treat `data` as an object. It emits one event per element, so one
  Kafka record can fan out into several events.
- **`action` is the regime discriminator**, now `"snapshot" | "update"`. Any other value is noise
  and is discarded per the message-types rule above.
- **Levels**: `asks`/`bids` are `[price, qty]` **string** pairs ✅ (BigDecimal-from-string, no
  numeric-literal hazard). Asks price-ascending, bids price-descending (best-first both sides) on
  BOTH frames. Prices may lack decimals.
- **A side may be absent on an update** — the parser nulls the missing side rather than dropping
  the frame (job 5 reads null as "leave this side alone"), matching ex8/okx. Both captured frames
  happen to carry both sides.
- **Sequence**: inner `ts` (parse the string to long), **jump 650, tolerance 110**. It is the ONLY
  ordering signal on the wire, and it is a clock, not a counter — the band is fitted to the
  measured distribution, not to a cadence bitget publishes. `checksum` is metadata — bitget
  intends it for CRC verification of the top of the book, which is the divergence detector this
  feed actually wants and this pipeline does not implement (todo.md).
- **Two timestamps**: inner `ts` is a **string** epoch-millis (`"1787404282388"`), top-level `ts`
  a JSON **number**. In both captures they are equal, unlike the old `books50` frames where the
  outer one was slightly later. Only the inner one is read.
- **`scale` is a price-GROUPING bucket** (`"0.01"` here), like okx's `grouping` — it explains the
  level spacing, it is not a wire rule and job 1 ignores it.

### ex5 REST snapshot (SECOND stream, captured 2026-08-23)

`ex5-raw` carries **two** streams, the same split ex1 and ex2 have: the WS `depth` feed above, and
bitget's REST depth endpoint. NiFi tags the REST body with `"action": "snapshot"` and injects the
market as a top-level `"pair"` (the response carries no symbol of its own), exactly as it does for
ex1/ex2.

```json
{
  "code": "00000",
  "msg": "success",
  "requestTime": 1787465707150,
  "data": {
    "a": [[1.4482, 433.2497], [1.4483, 927.4044], [1.4484, 1432.4082]],
    "b": [[1.4481, 83.5936], [1.448, 433.2497], [1.4479, 3312.1526]],
    "ts": "1787465707152"
  },
  "pair": "XRPUSDT",
  "action": "snapshot",
  "simulation": 0,
  "id": "12caf5fe-b438-493e-9b42-75bdf31d92e6"
}
```

(Level arrays trimmed — the real response carried ~195 asks and ~195 bids. `simulation` and `id`
are NiFi's, shown here because this capture came off the raw topic; the committed fixture omits
them, like every other fixture, and the shared tests inject them.)

Parsing notes (job 1) — it differs from the WS frame on **every** axis that matters:

- **`action` is `"snapshot"` on BOTH streams, so it cannot be the discriminator.** Same trap as
  ex1/ex2. The parser branches on the shape of `data`: an **object** is the REST body, an
  **array** is a WS frame.
- **Market key is the injected root `pair`** (`XRPUSDT`), not `arg.instId` — there is no `arg`.
- **`data` is a single OBJECT**, not an array, so this stream can never fan one record out into
  several events the way the WS one can.
- **The sides are `a` / `b`**, not `asks` / `bids`. Both are required: this is a full book, never
  a per-side snapshot, so a body missing either side is dropped rather than half-applied.
- **Levels are JSON NUMBERS**, not string pairs — the one place ex5 shares a hazard with ex3/ex4.
  They go through `Levels.fromNumericArrays`, i.e. BigDecimal from the decimal literal then
  `toPlainString`, never via double. Scale is the literal's own: `1.448` stays `"1.448"`.
- **`sequence_id` is NULL and `sequence_jump` is 0** — the `baselinePending` bootstrap ex1/ex2
  give their REST bodies. `data.ts` is still the **event time**; it is a real timestamp, just not
  a comparable sequence. **⚠ REVISED 2026-08-23 (2) — this replaces the original decision to
  sequence it by its own `data.ts` at 600 ± 10, which caused a live resync loop.** Measured on the
  dev feed: the REST `ts` is on the endpoint's clock, ranging −706..+662 ms against the WS update
  just before it and **behind it 57% of the time**, and the update just after it landed inside the
  old window only **9.9%** of the time. So ~90% of resyncs gapped instantly — accept → gap → empty
  the book → request another snapshot → repeat, **28.6 book resets and 28.7 requests per minute**,
  with `control-plane` saturated. Replaying the same 36-minute capture through null-seq +
  `650 ± 110` gives **0.1 resets/min and 0.1 requests/min**. Never compare the two clocks.
- **`requestTime` is ignored** — it is the API round trip, not the book's timestamp.
- **`code` / `msg` are not inspected.** An error body has no `data.a` / `data.b`, so the shape
  whitelist already discards it; a second check would be dead weight.

## ex6-raw — bybit

**Captured 2026-07-14** (supplied by team). Regime **re-confirmed: snapshot/delta** —
"bybit is snapshot/update" (user statement). The FIRST of two exchanges in scope with true
delta semantics (the other is ex8/okx); the discriminator is `type: "snapshot" | "delta"`.

**Sequence rule (user-confirmed 2026-07-14): the sequence id is `u`, and the expected jump
is 1** (contiguous — snapshot `u: 126776811` → delta `u: 126776812` in the samples below).
The separate `data.seq` field is NOT contiguous (…484 → …490 across the same two messages) —
do not use it for gap detection.

Snapshot sample (pretty-printed; level arrays trimmed — the real message carried **50 levels
per side**, matching the depth in the topic name `orderbook.50.BTCUSDT`):

```json
{
  "id": "b92c4a07-6e83-4d15-9270-1f5a8c3b60de",
  "simulation": 1,
  "topic": "orderbook.50.BTCUSDT",
  "ts": 1784027470176,
  "type": "snapshot",
  "data": {
    "s": "BTCUSDT",
    "b": [
      ["62724.1", "0.407233"],
      ["62723.6", "0.00012"],
      ["62722.6", "0.002"]
    ],
    "a": [
      ["62724.2", "0.529827"],
      ["62724.3", "0.029207"],
      ["62724.4", "0.029554"]
    ],
    "u": 126776811,
    "seq": 111416318484
  },
  "cts": 1784027470170
}
```

Delta sample (verbatim, complete — deltas carry ONLY the changed levels):

```json
{
  "id": "0e6d3f81-7a49-4c26-b358-2d9e4f10c7a5",
  "simulation": 1,
  "topic": "orderbook.50.BTCUSDT",
  "ts": 1784027470196,
  "type": "delta",
  "data": {
    "s": "BTCUSDT",
    "b": [
      ["62709.4", "0.096404"]
    ],
    "a": [
      ["62724.2", "0.529037"]
    ],
    "u": 126776812,
    "seq": 111416318490
  },
  "cts": 1784027470192
}
```

Parsing notes (job 1):

- **NOT Centrifugo** — bybit's own WS shape: top-level `topic` / `ts` / `type` / `data` /
  `cts`. Fifth distinct envelope shape in the set.
- **Market key**: `data.s` (`BTCUSDT` → `exchange_markets.market`); also embedded in the
  `topic` string (`orderbook.{depth}.{symbol}` — depth 50 encoded there, like bitget's
  old `books50`; bitget's current `depth` channel no longer encodes it).
- **`type` is the regime discriminator**: `"snapshot"` (full book, 50 levels/side) or
  `"delta"` (only changed levels — a delta may touch one side only, or replace/insert/delete
  levels). This is what job 2's snapshot/update classification reads.
- **Sides are `b` (bids) / `a` (asks)** — abbreviated keys. On the snapshot: bids
  price-descending, asks price-ascending — best-first on both sides. Delta level order
  presumably follows the same convention (single-level sides here — unverified).
- Levels are `[price, qty]` **string** pairs ✅ (no JSON-number hazard). Prices may lack
  decimals (`"62720"`).
- **Sequence**: `u` with jump 1 (see rule above) — the first exchange where job 2's
  `sequence_id`/`sequence_jump` gap rules apply for real. `seq` is bybit-internal
  (cross-topic per docs) — treat as metadata. `u` gaps in the topic mean real
  upstream/NiFi-side loss (the discarded capture showed gaps between ~30% of consecutive
  records) — **DECIDED 2026-07-14 (user): skip the NiFi investigation; job 2's gap rule
  absorbs it** (on gap: drop until the next snapshot re-syncs the book).
- **Delta delete = qty `"0"`** — lead from the discarded capture, NOT shown in these samples;
  capture a real qty-"0" delta frame to confirm.
- **Two timestamps**, both JSON numbers: `ts` (outer, likely gateway send time) and `cts`
  (earlier — likely matching-engine time). Metadata.
- **Still to capture**: a qty-"0" delete delta frame.

**Re-confirmed 2026-08-24** against a fresh WS snapshot + delta pair (`u` 210920912 → 210920913,
jump 1 ✅; `seq` 112975848012 → 112975848022, jump 10 — still unusable). Both match the parser as
written; no WS change was needed. The new capture does settle one open shape question:

- **An unchanged side on a delta arrives as a present-but-EMPTY array**, not as an absent key —
  the captured delta carried `"b": []` with four ask changes. This is safe, but only because of
  where job 5 clears: it clears a side before merging **only when the type is `snapshot`**, so on
  an update an empty array merges nothing. The absent-key (null) and empty-array cases therefore
  behave identically on updates and differ only on snapshots. Both are now pinned by tests
  (`BybitParserTest.emptySideOnDeltaIsEmptyNotNull`, `Ex6RestSnapshotResync` source 02).

### ex6 REST snapshot (SECOND stream, captured 2026-08-24)

`ex6-raw` carries **two** streams, the same split ex1, ex2 and ex5 have: the WS feed above, and
bybit's `/v5/market/orderbook` REST endpoint. NiFi tags the REST body with `"action": "snapshot"`
and injects the market as a top-level `"pair"`, exactly as it does for the others.

```json
{
  "retCode": 0,
  "retMsg": "OK",
  "result": {
    "s": "BTCUSDT",
    "a": [["77443.5", "0.185647"], ["77446.5", "0.00166"], ["77446.6", "0.000097"]],
    "b": [["77443.4", "0.313301"], ["77442.3", "0.0006"], ["77441.4", "0.015528"]],
    "ts": 1787491955753,
    "u": 38992362,
    "seq": 113017010359,
    "cts": 1787491955741
  },
  "retExtInfo": {},
  "time": 1787491955827,
  "action": "snapshot",
  "pair": "BTCUSDT",
  "id": "2fae9d46-6171-40aa-924a-6ed690f32440",
  "simulation": 0
}
```

(Level arrays trimmed — the real response carried 50 levels per side. `simulation` and `id` are
NiFi's, shown here because this capture came off the raw topic; the committed fixture omits them,
like every other fixture, and the shared tests inject them.)

Parsing notes (job 1):

- **The discriminator is clean, unlike ex5's.** The book is under **`result`**, not `data`, and the
  WS frame has no `action` field at all — so a single `result`-is-an-object check separates the two
  streams. (ex1/ex2/ex5 all had to fall back to something subtler because `action` reads
  `"snapshot"` on both of their streams.)
- **Market key is `result.s`**, bybit's own symbol — which keeps the key derivation identical to
  the WS branch. The injected root `pair` agrees with it and is redundant here; ex1/ex2/ex5 need
  theirs only because those REST bodies carry no symbol at all.
- **Both sides required.** A depth response is always a full book, never per-side, so a body
  missing `a` or `b` is dropped rather than half-applied (it would silently wipe the other side).
- **Levels are `[price, qty]` string pairs** — same as the WS feed. ex6 has **no** JSON-number
  hazard on either stream, unlike ex5 whose REST body switched to numeric literals.
- **⚠ `sequence_id` is NULL and `sequence_jump` is 0 — `result.u` is NOT on the WS counter.** This
  is the single most important fact about this stream, and the captures prove it arithmetically:
  the REST body is **24.3 hours LATER** than the WS pair above, yet its `u` is **171,928,550
  LOWER** (38,992,362 vs 210,920,912). A monotonic counter cannot run backwards, so these are two
  separate counters — most plausibly because the REST endpoint's `updateId` is scoped per request
  depth rather than to the `orderbook.50` topic (**the reason is unconfirmed; the incomparability
  is not**). Adopting `result.u` would make the next WS delta read as a ~172M jump — or, since the
  REST value is the *smaller* one, as an immediate `stale_or_duplicate` — either way: accept →
  reject → empty the book → request another snapshot → repeat. **That is exactly the live resync
  loop ex5 was burned by on 2026-08-23** (28.6 book resets/min, `control-plane` saturated). So ex6
  takes the same fix ex1/ex2/ex5 use: null-seq, and job 2's `baselinePending` bootstrap orders this
  body by event time and lets the first WS delta after it adopt its own `u` as the baseline. **Never
  compare the two counters.**
- **`result.seq` is unusable too**, for the reason it always was on this exchange: it moves 10 per
  `u` and is bybit-internal cross-topic metadata.
- **Event time is `result.cts`** — the matching-engine time, the same field the WS branch reads, so
  both ex6 streams share one event-time clock. This is cleaner than ex5, whose REST body offered
  only a gateway `ts`.
- **`result.ts` and the top-level `time` are ignored** — the gateway send time and the API round
  trip respectively (`time` is ex5's ignored `requestTime`).
- **`retCode` / `retMsg` are not inspected.** A bybit error body answers `"result": {}`, which has
  no `a`/`b` arrays, so the shape whitelist already discards it; a second check would be dead
  weight.

## ex7-raw — ompfinex

> ⚠ **POSTPONED 2026-07-14 → IN SCOPE 2026-08-24.** `OmpfinexParser` landed on
> `feat/add-ompfinex`. **No captures have been added to this document yet — that is a gap, not
> an omission.** Everything below is the shape the parser reads, transcribed from its javadoc;
> it is NOT a capture and nobody but the parser's author has seen the real frames.

**Regime: REST snapshot + Centrifugo WS delta — a TRUE delta feed** (the ex6/ex8 family), not
ex1/ex2's null-seq resync pattern, even though ompfinex is a Centrifugo exchange like ex1, ex2
and ex4. Discriminator is easy: the REST body has a top-level `action: "snapshot"`, the WS
frame has no `action` field at all.

**REST snapshot.** `{"status":"OK","action":"snapshot","pair":"{market}","data":{"lastUpdateId":N,
"time":...,"bids":[...],"asks":[...]}}`. Market is the top-level `pair` (a NUMERIC string, e.g.
`"14"` — the ex4/ramzinex style, matching `exchange_markets.market`). Levels are `[price, qty]`
string pairs. **Sequence id = `data.lastUpdateId` — a REAL seq, unlike ex1/ex2's null-seq
snapshot and unlike ex6's REST body**; jump 0. Event time = `data.time`, **epoch MICROSECONDS**,
divided by 1000.

**WebSocket update.** Centrifugo push on channel `public-market:r-depth-{market}`.
Binance-style diff-depth: `data.U`/`data.u` are the first/last update ids folded into the
message. Sequence id = `data.u`, **jump = `u - U` (dynamic per message)** — so continuity is
`U_n == seq_{n-1}`, **NOT** Binance's `U_n == seq_{n-1} + 1`. Sides are `a`/`b` (asks/bids),
string pairs, qty `"0"` = delete. A side key is claimed to be always present, possibly as an
empty array; the parser **requires both** and drops the whole message if either is missing
(unlike ex6/ex8, which pass a null side). No message-level timestamp on the wire → event time
is job-1 processing time, the ex4 situation.

**⚠ What still needs capturing, and why each matters:**

| Claim | Why it matters if wrong |
| --- | --- |
| `data.time` is epoch MICROseconds | ÷1000 puts event time in 1970 or year 58000; asserted nowhere in the e2e suite |
| `U_n == u_{n-1}` (the `+0` convention) | off by one ⇒ every delta reads as a gap |
| the first delta after a REST snapshot has `U == lastUpdateId` | if a mid-stream snapshot is not contiguous, ex7 enters the ex5 resync loop |
| both `a` and `b` are always present | a missing key drops the message silently ⇒ an invisible sequence gap |

The evidence behind the `+0` convention is **two consecutive live samples** (second message's
`U` 859075 == first's `u` 859075). That establishes the convention; it does not establish the
mid-stream-resync case, which is the one that bit ex5 and ex6.

Hand-built payloads in this exact shape live in `e2e/scenario/data_ex7.go`. They are fixtures
for the pipeline's arithmetic, **not captures** — do not cite them as wire evidence.

## ex8-raw — okx

**Channel CHANGED 2026-09-05: `books-grouped` → `books`, on the PUBLIC endpoint.** The feed now
comes from `wss://ws.okx.com:8443/ws/v5/public`, channel `books`, replacing the grouped book on
okx's undocumented `wss://wspri.okx.com:8443/ws/v5/ipublic`. Everything below describes the new
shape; the old one is summarised at the end of this section so old captures stay readable.

**Why the switch (measured, see [[project_pair_extractor]] § ex8):** `books-grouped` carried NO
counter at all and had to be sequenced by its `ts`, which advances in exact multiples of 300 ms but
SKIPS steps — only 79.5% of live transitions were `+300`, so ~20% of updates dead-lettered as false
`sequence_gap`s and `ex8-p1` was accepting **3.8%** of its traffic. No tolerance could fix it (every
window from `±0` to `±299` scored the identical 79.52%). `books` carries a real chained counter, so
contiguity becomes exact instead of inferred.

**Sequence rule: `seqId`, with a DYNAMIC jump of `seqId - prevSeqId`.** Every frame names its own
predecessor — `prevSeqId` is the `seqId` of the message before it. Stamping the jump per message
makes job 2's `seq == lastSeq + jump` reduce algebraically to **`prevSeqId == lastSeq`**, okx's own
documented rule, enforced exactly with no window and no tolerance. This is the ex7/ompfinex pattern
(`u - U`), the platform's second dynamic jump. Measured over **6,516 consecutive live transitions on
5 markets: 6,516 chained, 0 broken**, while the raw `seqId` step took 90–172 DISTINCT values per
market (3 … 960) — so no fixed jump could ever have worked here.

Snapshot sample (captured live 2026-09-05, `ZEC-USDT`; level arrays trimmed from the real
**400 per side** — every remaining value verbatim). This is also the fixture
`ex8-snapshot.json`:

```json
{
  "id": "a304f0d3-8062-48ab-b971-fc638d9f3f79",
  "simulation": 0,
  "arg": {
    "channel": "books",
    "instId": "ZEC-USDT"
  },
  "action": "snapshot",
  "data": [
    {
      "asks": [
        ["1012.6", "0.07818", "0", "1"],
        ["1012.62", "0.07406", "0", "1"],
        ["1012.63", "0.8", "0", "1"]
      ],
      "bids": [
        ["1012.51", "0.0322", "0", "1"],
        ["1012.5", "0.10706", "0", "2"],
        ["1012.4", "0.14797", "0", "2"]
      ],
      "ts": "1788613464301",
      "checksum": 0,
      "prevSeqId": -1,
      "seqId": 4429784547
    }
  ]
}
```

Update sample (verbatim and complete — **the frame that immediately followed the snapshot above on
the live socket**, which is why its `prevSeqId` is that snapshot's `seqId`). Fixture
`ex8-update.json`:

```json
{
  "id": "5c1d8e30-6b74-42af-8e95-0a3f7d21b4c6",
  "simulation": 0,
  "arg": {
    "channel": "books",
    "instId": "ZEC-USDT"
  },
  "action": "update",
  "data": [
    {
      "asks": [
        ["1013.67", "0", "0", "0"],
        ["1013.68", "6.6016", "0", "1"]
      ],
      "bids": [
        ["1011.53", "6.64926", "0", "2"],
        ["1011.45", "0", "0", "0"],
        ["973.84", "0.05859", "0", "1"]
      ],
      "ts": "1788613464401",
      "checksum": 0,
      "seqId": 4429784551,
      "prevSeqId": 4429784547
    }
  ]
}
```

Parsing notes (job 1):

- **Envelope unchanged** — still the bitget-family `arg` / `action` / `data`-ARRAY, so the
  discriminator against the REST body (absence of `arg`) is untouched. What changed inside is the
  `arg` (no `grouping` any more) and the per-book object (three new fields).
- **Market key**: `arg.instId` (`ZEC-USDT` → `exchange_markets.market`) — note the DASH, unlike
  every other exchange's `BTCUSDT`. `arg` now has exactly **two** keys, `channel` and `instId`;
  `grouping` is gone with the grouped channel.
- **`action` is the regime discriminator**: `"snapshot"` or `"update"`. **A snapshot arrives on
  every fresh subscribe** and carries `prevSeqId: -1` — a sentinel, not a sequence. Verified on
  5/5 markets: one snapshot each, at index 0, then updates only.
- **Levels are FOUR-element string arrays on BOTH streams now** — `[price, qty, "0", orderCount]`.
  The WS frame used to send two; it now matches the REST body exactly, so `Levels.fromStringPairs`
  (reads elements 0–1, ignores the rest) covers both with no special-casing. Observed 54,101 levels,
  **100% four-element**.
- **Delete = qty `"0"`** — present on both sides in the update sample (ask `1013.67`, bid
  `1011.45`). 13,480 deletes in the capture. Job 5 must remove those levels.
- **Both `asks` and `bids` are ALWAYS present**, but either may be an empty array `[]` when that
  side did not change (3,413 frames changed both sides, 1,900 asks-only, 1,208 bids-only; **zero**
  frames omitted a key). This differs from the old grouped channel, which sent `null` for an absent
  side. The parser's `has()` checks still hold: present-but-empty → empty list, missing → null.
- **Depth**: snapshot is exactly **400 levels per side** on all 5 captured markets. Updates carry
  only changed levels (up to 124 asks / 109 bids observed).
- **Sequence = `seqId`** (JSON **integer**, order 1e10 — not a string, unlike `ts`), jump
  `seqId - prevSeqId`. `prevSeqId` is likewise an integer. A frame missing either is dropped whole:
  without both, the chain cannot be expressed and job 2 would silently mis-validate.
- **`ts` is the event time only** (string epoch-millis, as before) — it is no longer the sequence.
  It is strictly monotonic per market on the capture (0 backwards, 0 equal across 6,516
  transitions), so it remains a sound event-time clock.
- **`checksum`** is okx's CRC32 book-integrity value. **Ignored** — job 5 builds the book and
  nothing in the platform verifies a checksum (same treatment as ex5's). Observed `0` on all 6,521
  captured frames, so do not rely on it being populated.
- **Two okx-documented edge cases need no special-casing.** A no-change keepalive repeats the
  counter (`seqId == prevSeqId`) → jump 0 → job 2 accepts `seq == lastSeq` and the book is left
  alone, which is what a no-op means. A counter RESET (okx may restart `seqId` lower after
  maintenance) breaks the chain → `sequence_gap` → book emptied → control plane asks, which is the
  correct response to a reset. Neither was observed in the 3-minute capture; both are pinned by
  unit tests.

**⚠ For the NiFi team — the resync answer.** Prefer a **RESUBSCRIBE** over the REST body when
answering a `snapshot_request`: a fresh subscribe returns `action: "snapshot"` with 400 levels **on
the feed's own counter**, so job 2 re-seeds `lastSeq` exactly and the next update chains straight to
it. The REST body cannot do that (see the `seqId` note in the REST section below) and costs a
baseline gap every time. The REST branch is kept as a working fallback, not as the preferred path.

<details>
<summary>The previous <code>books-grouped</code> shape (retired 2026-09-05) — for reading old captures</summary>

Captured 2026-07-14 and 2026-09-05 from `wss://wspri.okx.com:8443/ws/v5/ipublic`, channel
`books-grouped`, with `arg.grouping` (`"1"` in the 2026-07-14 capture, `"0.1"` live in September —
it is channel identity, never parsed). Levels were **two**-element string pairs, up to 150 per side,
and the per-book object carried **only** `asks`/`bids`/`ts` — no counter of any kind. The sequence
was `ts` itself with a fixed jump of 300. An absent side was `null`, not `[]`.

That channel does not exist on okx's public API (`60018 … doesn't exist` on both `/ws/v5/public`
and `/ws/v5/business`); it was only ever served by the `ipublic` endpoint.

The 2026-09-05 measurement that retired it, and the "do the WS and REST books share a price grid?"
question it raised — **answered: yes, they matched on all 23 subscribed markets** — are written up
in [[project_pair_extractor]] § ex8. The grid question is moot for `books`, which is not grouped.

</details>

### The REST snapshot — ex8's SECOND stream (added 2026-09-05, captured from `ex8-raw`)

Like ex5, `ex8-raw` carries **two** shapes. This is the body NiFi publishes when it answers a
`snapshot_request` from the control plane, and job 1 had **no branch for it** until 2026-09-05 —
see [[project_pair_extractor]] § ex8 for what that cost.

```json
{
  "id": "a304f0d3-8062-48ab-b971-fc638d9f3f79",
  "simulation": 0,
  "code": "0",
  "msg": "",
  "data": [ {
    "asks": [ ["1011.99", "0.2362", "0", "1"], ["1012", "0.03", "0", "1"] ],
    "bids": [ ["1011.74", "0.26887", "0", "2"], ["1011.72", "1.04305", "0", "2"] ],
    "ts": "1788605352151",
    "seqId": 4428333610
  } ],
  "pair": "ZEC-USDT",
  "action": "snapshot"
}
```

- **NO `arg`.** This is the whole reason the frame used to be dropped: the WS branch reads the
  market from `arg.instId`, which is absent here, so the parser discarded it and the resync answer
  never reached job 2. NiFi stamps the market as a top-level **`pair`** instead (`ZEC-USDT`, dashed,
  same spelling as `arg.instId`), exactly as it does for ex5.
- **`arg` is the discriminator, NOT the shape of `data`.** ex5 can switch on `data` being an object
  vs an array; here `data` is an **array on both streams** and `action` reads `"snapshot"` on both.
  Absence of `arg` is the only reliable signal.
- **Levels are FOUR-element string arrays** — `[price, qty, "0", orderCount]` — where the WS frame
  sends two. `Levels.fromStringPairs` reads elements 0 and 1 and ignores the rest, so one helper
  covers both. (The third element is okx's deprecated liquidated-order count, always `"0"`.)
- **`pair` and `action` are at the END of the object**, after `data`. A capture truncated mid-book
  looks like it has neither — that misread cost a round trip on 2026-09-05.
- **`seqId` — the body still stays NULL-SEQ, but the REASON changed with the `books` channel
  (2026-09-05, second revision).** It is no longer that only one stream has a counter: the WS side
  is now sequenced by `seqId` too, and it is the **same counter** — the REST `ZEC-USDT` body read
  `4428333610` where a WS `ZEC-USDT` frame the same day read `4429784547`, the same space, later
  and larger. The reason it still cannot seed `lastSeq` is subtler and permanent: **a snapshot's
  `seqId` is not any later update's `prevSeqId`.** The counter advances between NiFi's fetch and the
  next WS frame, so seeding it would break the very next chain check rather than repair it. Null
  hands job 2 the `baselinePending` bootstrap instead — order this body by EVENT TIME, then let the
  first WS update after it adopt its own `seqId` as the baseline.
  **This is exactly why a RESUBSCRIBE is the better resync answer** (see the WS section above): the
  WS snapshot re-seeds the counter exactly and costs no baseline gap at all.

### ✅ CLOSED — the price-grid question, answered then made moot

**Answered 2026-09-05 (measured), then retired the same day (channel switch).** The old worry was
that the WS channel was price-GROUPED while the REST depth endpoint is not, so after a resync the
book could hold REST prices the WS deltas could never address (a delete at `1011.9` not removing
`1011.99`). The comparison needed a WS frame and a REST body **for the same market**, which no
capture had.

It was measured over all 23 subscribed markets, both streams each: **every market's `arg.grouping`
equalled the tick its REST body quotes at** — BTC/BNB `0.1`, ETH/SOL/AAVE/OKB/**ZEC** `0.01`,
AVAX/GRAM/HYPE/LINK/NEAR/UNI `0.001`, ADA/DOT/SUI/WLD/XRP `0.0001`, DOGE/TRX/XLM `0.00001`,
PEPE/SHIB `1e-9`. No mismatch anywhere; the ZEC worry was unfounded, since ZEC's grouping IS two
decimals. *(Method caveat: the grid was inferred from the decimal places of quoted top-of-book
prices, so it is a lower bound on coarseness — it agreed with `grouping` on all 23, which is what
makes it convincing.)*

**And it is now moot regardless: `books` is not a grouped channel**, so there is no second grid and
no `grouping` field. Kept here only so the question is not re-opened from an old capture.

## ex9-raw — lbank

**Captured 2026-08-25** (four consecutive WebSocket frames, supplied by the user). `LBankParser`
landed on `feat/add-lbank` (commit `977e770`) and the rest of the pipeline followed on 2026-08-26.

**Regime: SNAPSHOT ONLY.** Every frame carries the whole book under `depth`. There is no delta
channel, no `action`/`type`-style regime discriminator to read, and nothing for job 2 to make
contiguous — an accepted frame replaces the book outright. This makes ex9 the **second
snapshot-only exchange** after ex3/wallex, and unlike ex3 it sends BOTH sides in every frame, so
no side is ever null.

**⚠ There is no sequence field anywhere on the wire.** Not a counter, not an update id, not a
`lastUpdateId`. **User decision 2026-08-26: `sequence_id` is NULL and `sequence_jump` is 0** —
`TS` is deliberately NOT re-used as a sequence the way ex5's and ex8's `ts` are, because a
timestamp-as-sequence imposes a publish cadence the exchange never promised (ex5 needed a
`650 ± 110` window for exactly that reason, and got a resync loop out of it). A null sequence
puts ex9 on job 2's **event-time branch**, where the whole test is "not older than the last
accepted frame" — which is all a full-snapshot feed needs.

Verbatim frame (levels trimmed from **50 per side** to 3; the four captured frames all carried
exactly 50 and 50):

```json
{
  "id": "e2b1c9f4-7a35-4d68-91c0-5f3ba8e47d21",
  "simulation": 1,
  "depth": {
    "asks": [
      ["79654.45", "1.04718"],
      ["79654.46", "0.00083"],
      ["79654.47", "0.00016"]
    ],
    "bids": [
      ["79654.44", "2.89166"],
      ["79654.43", "0.00083"],
      ["79654.42", "0.00016"]
    ]
  },
  "SERVER": "V3",
  "count": 200,
  "limit": 50,
  "type": "fdepth",
  "pair": "btc_usdt",
  "TS": "2026-08-25T17:46:51.723"
}
```

- **Market key**: the top-level `pair` — **lowercase with an underscore** (`btc_usdt`), the only
  exchange in the set that spells it that way (ex1/ex2/ex5/ex6 send `BTCUSDT`, ex8 `BTC-USDT`,
  ex4/ex7 a numeric string). It matches `exchange_markets.market` verbatim, and **nothing in job 1
  normalizes case** — the lookup key is `"{exchange_id}|{market}"`, so a case change on either
  side drops 100% of ex9 silently.
- **`TS` is the event time and the ONLY ordering field.** ISO-8601 local date-time with
  **millisecond precision and NO zone marker** — the only exchange in the set whose timestamp is
  not epoch millis. **User-confirmed 2026-08-26: it is UTC**, and the parser reads it with
  `ZoneOffset.UTC`. If that is ever revised, `LBankParser`'s one `ZoneOffset` is the whole change;
  an 8-hour error would be invisible in the levels and would only surface as a staleness alarm.
- **Sides are `asks` / `bids`** under `depth`, `[price, qty]` **string** pairs ✅ (no JSON-number
  hazard). Asks price-ASCENDING, bids price-DESCENDING — best-first on both sides, the ex1/ex6
  convention, not ramzinex's both-descending.
- **Both side keys are always present** in all four captures. The parser **requires both** and
  drops the whole frame if either is missing — the ex7 rule, not ex6/ex8's null side. That is
  safe here precisely because the feed is snapshot-only: a half-frame would silently wipe a side.
- **No qty-`"0"` deletes**, and there cannot be: a snapshot IS the book, so a level that
  disappeared is simply absent from the next frame.
- **`type` is `"fdepth"`** — futures depth — in all four captures. **The parser does NOT whitelist
  it** (user decision 2026-08-26): frames are selected by SHAPE (`pair` + textual `TS` +
  `depth.asks` + `depth.bids` as arrays), because lbank's other channels (pings, subscribe acks,
  ticks, and the incremental `incrDepth` book, whose levels hang off a differently-named key) all
  fail that check already. If the spot `depth` channel is ever subscribed, it parses with no
  change. ⚠ The corollary: a future lbank channel that happens to carry `pair`, `TS` and a
  `depth` object WOULD be misread as a book.
- **`SERVER` / `count` / `limit` are ignored.** `limit` (50) matches the level count; `count`
  (200) does not match anything in the frame and its meaning is unverified.

**⚠ The captures arrived NEWEST-FIRST** (`51.723`, `51.221`, `50.723`, `50.216`), which is an
artifact of how they were pasted, not evidence about wire order. Nothing in the set proves lbank
never re-sends an older book — that is exactly what job 2's `out_of_order` guard is there for.

**⚠ Equal timestamps are ACCEPTED, not rejected** (user decision 2026-08-26). Job 2's null-seq
guard is `event_time < lastEventTime`, strictly older — so two frames sharing a `TS` both pass
and the book is simply re-emitted unchanged. On a feed that publishes every ~500 ms this is a
duplicate snapshot, which is harmless; it is written down here because it is a decision, not an
oversight.

Fixture: `flink/normalizer/job-pair-extractor/src/test/resources/fixtures/ex9-snapshot.json`
(this frame, without the NiFi fields — those are injected by the shared tests). Hand-built
payloads in this shape live in `e2e/scenario/data_ex9.go`; they are fixtures for the pipeline's
arithmetic, **not captures**.
