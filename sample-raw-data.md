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
| `ex1-raw` | nobitex  | ✅ captured 2026-07-14; ⚠ REVISED 2026-07-21 — two streams: REST snapshot (`action`+`pair`) + WS delta (update) |
| `ex2-raw` | bitpin   | ✅ captured 2026-07-14; ⚠ REVISED 2026-07-25 — two streams: REST snapshot (`action`+`pair`) + WS delta (update) |
| `ex3-raw` | wallex   | ✅ captured 2026-07-14 (per-side snapshots) |
| `ex4-raw` | ramzinex | ✅ captured 2026-07-14 (snapshot) |
| `ex5-raw` | bitget   | ✅ captured 2026-07-14; ⚠ **REVISED 2026-08-22** — channel changed `books50` → `depth`/`scale`: now snapshot **+ update**, `seq` GONE, sequence = inner `ts`; **+ a REST snapshot stream captured 2026-08-23** (`data` object, `a`/`b`, NUMERIC levels, injected `pair`). ⚠ **RE-MEASURED live 2026-08-23**: the WS channel sends **no snapshots at all**, the REST body is the only baseline and is now **null-seq**, and the update window is **650 ± 110** |
| `ex6-raw` | bybit    | ✅ captured 2026-07-14 (snapshot + delta; qty="0" delete frame still to capture) |
| `ex7-raw` | ompfinex | **POSTPONED** (2026-07-14, raw-data issue) — out of initial scope |
| `ex8-raw` | okx      | ✅ captured 2026-07-14 (snapshot + update; qty="0" delete CONFIRMED on wire) |

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

- **ex1, ex2, ex4, ex5, ex6, ex8 — object root** ⇒ injected as **root fields**, alongside whatever
  the exchange already sends. For the Centrifugo payloads (ex1/ex2 WS, ex4) that means the OUTER
  object, next to `push` — not inside `data`.
- **ex3/wallex — array root** ⇒ there is no root field to inject into, so both go in a **trailing
  metadata object as element index 2**: `["{market}@{side}", [levels…], {"simulation":1,"id":"…"}]`.
  **Never a fourth element** — job 1 reads index 2 and drops any envelope longer than 3.

The samples below all show `"simulation": 1`; a live feed sends `0` (or omits it). The `id` values
are illustrative — every real record carries its own.

---

## ex1-raw — nobitex

**⚠ REVISED 2026-07-21 — the "only snapshots" assumption was WRONG.** nobitex serves the initial
book over a REST API and then only **deltas** over WebSocket; we had been treating every WS
message as a full snapshot. The NiFi team now publishes **two distinct payloads** to `ex1-raw`:

1. **REST snapshot** — NiFi tags it `"action": "snapshot"` and **injects the market as a
   top-level `"pair"` field** (the REST body has no symbol of its own). This is the full book →
   `type = "snapshot"`, `sequence_id = null` (no offset on the wire).
2. **WebSocket delta** — the Centrifugo push we already consumed, **unchanged** and with **no
   `action` field** → `type = "update"`, `sequence_id = pub.offset`, `sequence_jump = 1`
   (Centrifugo offsets increment by exactly one per publication).

Because the REST snapshot carries no offset, job 2 treats a null-seq snapshot as a **resync
signal**: the first WS update after it adopts its offset as the baseline (see
memory/project_type_validator.md).

**REST snapshot sample** (level arrays trimmed):

```json
{"id":"9f2b1c74-3d5e-4a81-b0c6-71e2d4a95c38","simulation":1,"action":"snapshot","pair":"BTCUSDT","status":"ok","lastUpdate":1784614865284,"lastTradePrice":"65708.96","bids":[["65660","0.000615"],["65636","0.002543"]],"asks":[["65708.76","0.00672"],["65708.79","0.09133"]]}
```

**WebSocket delta sample** (Centrifugo envelope; pretty-printed; level arrays trimmed — a real
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
  try the Centrifugo `push` (WS delta); anything else is noise → dropped.
- **REST snapshot market**: from the injected top-level `"pair"` field (`BTCUSDT`). The REST
  body has no channel and no `symbol`.
- **WS delta market**: from the channel `public:orderbook-{market}` (here `BTCUSDT`) — differs
  from bitpin's `orderbook:{market}`. NO `symbol` field inside `data`; the channel is the ONLY
  market key on the WS side.
- **Levels** (both payloads): `bids`/`asks` are `[price, qty]` **string** pairs ✅ (asks listed
  first; asks price-ascending, bids price-descending). Prices may lack decimals (`"62678"`).
- **`lastTradePrice` is a string** ✅ (unlike bitpin's numeric `price`); `lastUpdate` is
  epoch-millis as a JSON number → event time — both metadata, not book levels.
- **Ordering / job-2 rule**: the REST snapshot has no offset → `sequence_id = null` (resync).
  The WS delta uses `pub.offset` as `sequence_id` with `sequence_jump = 1`, so job 2 does real
  contiguity gap detection (was an out-of-order-only check when we thought it was a snapshot feed).
- **Multi-doc records: CLOSED 2026-07-14 (user)** — ex1 records always contain ONE JSON
  document; the discarded-capture 2-newline-concatenated-docs lead was an artifact. No
  splitting logic in job 1.

## ex2-raw — bitpin

**⚠ REVISED 2026-07-25 — the "full snapshot on every message" assumption was WRONG**, exactly as
it was for ex1. bitpin serves the initial book over a REST API and then only **deltas** over
WebSocket; we had been treating every WS message as a full snapshot. NiFi publishes **two
distinct payloads** to `ex2-raw`:

1. **REST snapshot** — NiFi tags it `"action": "snapshot"` and **injects the market as a
   top-level `"pair"` field** (the REST body has no symbol of its own). This is the full book →
   `type = "snapshot"`, `sequence_id = null` (no offset on the wire).
2. **WebSocket delta** — the Centrifugo push we already consumed, **unchanged** and with **no
   `action` field** → `type = "update"`, `sequence_id = pub.offset`, `sequence_jump = 1`
   (Centrifugo offsets increment by exactly one per publication).

Because the REST snapshot carries no offset, job 2 treats a null-seq snapshot as a **resync
signal**: the first WS update after it adopts its offset as the baseline (see
memory/project_type_validator.md) — the same exchange-agnostic path ex1 uses.

**REST snapshot sample** (level arrays trimmed):

```json
{"id":"2d7f6b90-8c14-4e3a-9d57-6b2f0c81ae43","simulation":1,"action":"snapshot","pair":"BTC_USDT","event_time":1784008564112,"asks":[["62714.50","0.01387100"],["62720.77","0.00970970"]],"bids":[["62672.30","0.01003106"],["62655.92","0.01368489"]]}
```

**⚠ The injected `pair` must be the DB market string `BTC_USDT`** (underscore), matching
`exchange_markets.market` for ex2 and the WS channel suffix — job 1's lookup key
`"2|{market}"` is exact and case-sensitive, so `BTCUSDT` would drop silently as
`dropped-unknown-market`. Confirmed with the user 2026-07-25.

**WebSocket delta sample** (Centrifugo envelope; pretty-printed; level arrays trimmed — the real
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
  try the Centrifugo `push` (WS delta); anything else is noise → dropped.
- **REST snapshot market**: from the injected top-level `"pair"` field. The REST body has no
  channel and no `symbol`.
- **WS delta market**: from `push.channel` (`orderbook:{market}`, here `BTC_USDT`) and
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
  REST snapshot, which is what job 2's null-seq `out_of_order` guard reads.
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

## ex7-raw — ompfinex

> ⚠ **POSTPONED 2026-07-14** (team decision): known issue with its raw data — excluded from
> the initial pipeline scope.

## ex8-raw — okx

**Captured 2026-07-14** (supplied by team — which also settles the earlier "no `ex8-raw`
topic yet" caveat: the feed is live). Regime **re-confirmed: snapshot/update** — "okx has
both" (user statement). The SECOND exchange in scope with true delta semantics (after
ex6/bybit); the discriminator is `action: "snapshot" | "update"`.

**Sequence rule (user-confirmed 2026-07-14): the sequence id is `ts`, and the expected jump
is 300** — the epoch-millis timestamp inside the data object doubles as the sequence
(`"1784028204900"` → `"1784028205200"`, exactly +300 in the samples below; i.e. a fixed
300 ms publish cadence). Note it is a **string** on the wire — parse to long for the gap
math. There is no `u`/`seq`-style counter field at all.

Snapshot sample (pretty-printed; level arrays trimmed — the real message carried
**150 levels per side** (both sides exactly 150 here — likely a fixed depth, unverified)):

```json
{
  "id": "a47f0b25-3c9e-4681-9d40-7b2c5e83f16d",
  "simulation": 1,
  "arg": {
    "channel": "books-grouped",
    "instId": "BTC-USDT",
    "grouping": "1"
  },
  "action": "snapshot",
  "data": [
    {
      "asks": [
        ["62770", "2.21924167"],
        ["62771", "0.17447383"],
        ["62772", "0.19067482"]
      ],
      "bids": [
        ["62769", "0.50795335"],
        ["62768", "0.02744953"],
        ["62767", "0.20630833"]
      ],
      "ts": "1784028204900"
    }
  ]
}
```

Update sample (verbatim, complete — updates carry ONLY the changed levels; note the
qty-`"0"` delete at ask `62773` and the brand-new ask level `62931`):

```json
{
  "id": "5c1d8e30-6b74-42af-8e95-0a3f7d21b4c6",
  "simulation": 1,
  "arg": {
    "channel": "books-grouped",
    "instId": "BTC-USDT",
    "grouping": "1"
  },
  "action": "update",
  "data": [
    {
      "asks": [
        ["62771", "0.29045069"],
        ["62772", "0.12"],
        ["62773", "0"],
        ["62777", "0.35307699"],
        ["62779", "1.33057882"],
        ["62780", "0.33476925"],
        ["62784", "0.8498818"],
        ["62789", "0.01864785"],
        ["62797", "2.14864649"],
        ["62802", "1.17385946"],
        ["62809", "0.51130367"],
        ["62814", "0.02415278"],
        ["62822", "0.56817495"],
        ["62827", "0.19123979"],
        ["62931", "0.10148108"]
      ],
      "bids": [
        ["62769", "0.55175335"],
        ["62767", "0.28491215"],
        ["62765", "0.15675841"],
        ["62762", "1.30193303"],
        ["62758", "0.09900068"],
        ["62757", "0.00001599"],
        ["62750", "1.31062803"],
        ["62678", "0.0092566"]
      ],
      "ts": "1784028205200"
    }
  ]
}
```

Parsing notes (job 1):

- **NOT Centrifugo — same envelope family as bitget (ex5)**: `arg` / `action` / `data`-ARRAY.
  Differences from bitget: no top-level `ts`, no `instType`, and the price-grouping bucket is
  spelled `grouping` rather than `params.scale`. Not a new envelope shape — a variant of ex5's.
  Since ex5's 2026-08-22 revision the two are closer still: both are `snapshot`/`update` delta
  feeds keyed on a string `ts`. The one rule that differs is the cadence — okx's 300 ms is
  exact, while bitget's is a variable wall clock needing a `650 ± 110` window.
- **Market key**: `arg.instId` (`BTC-USDT` → `exchange_markets.market`) — note the DASH,
  unlike every other exchange's `BTCUSDT`. Channel identity is `arg.channel`
  (`books-grouped`) + `arg.grouping` (`"1"`): a price-GROUPED book with bucket size 1 —
  which is why every price in the sample is a bare integer.
- **`action` is the regime discriminator**: `"snapshot"` (full book) or `"update"` (only
  changed levels — may include deletes and brand-new prices). This is what job 2's
  snapshot/update classification reads. (Third exchange with an explicit discriminator,
  after bitget's `action` and bybit's `type` — and since 2026-08-22 bitget's `action` carries
  both values too, so this is no longer the only `action`-based delta feed.)
- **`data` is an ARRAY** wrapping a single book object (like bitget) — parser must unwrap.
  Whether >1 element can occur is unverified.
- **Sides are `asks` / `bids`**, `[price, qty]` **string** pairs ✅ (no JSON-number
  hazard). Asks price-ascending, bids price-descending — best-first both sides, on snapshot
  AND update. Prices here are integers because of grouping `"1"`, not a wire rule.
- **Delete = qty `"0"` CONFIRMED on the wire** (ask `62773` in the update) — the first
  captured delete frame in the whole sample set. Job 5 must remove that level; the update
  also inserts levels absent from the snapshot (`62931`).
- **Sequence**: `ts` with jump 300 (see rule above) — a time-based sequence, unlike bybit's
  counter. Second exchange where job 2's `sequence_id`/`sequence_jump` gap rules apply for
  real; per-exchange config must support both a counter (`u`, jump 1) and a millis
  timestamp (`ts`, jump 300).
- The inner `ts` is the ONLY timestamp on the message (string epoch-millis) — it is both
  the event time and the sequence id.
