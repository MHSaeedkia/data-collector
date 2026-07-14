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
| `ex1-raw` | nobitex  | ✅ captured 2026-07-14 (snapshot; always single-doc per user) |
| `ex2-raw` | bitpin   | ✅ captured 2026-07-14 (snapshot) |
| `ex3-raw` | wallex   | ✅ captured 2026-07-14 (per-side snapshots) |
| `ex4-raw` | ramzinex | ✅ captured 2026-07-14 (snapshot) |
| `ex5-raw` | bitget   | ✅ captured 2026-07-14 (snapshot; `seq` used for out-of-order check only) |
| `ex6-raw` | bybit    | ✅ captured 2026-07-14 (snapshot + delta; qty="0" delete frame still to capture) |
| `ex7-raw` | ompfinex | **POSTPONED** (2026-07-14, raw-data issue) — out of initial scope |
| `ex8-raw` | okx      | ✅ captured 2026-07-14 (snapshot + update; qty="0" delete CONFIRMED on wire) |

---

## ex1-raw — nobitex

**Captured 2026-07-14** (supplied by team). Regime **re-confirmed: full snapshot on every
message** — "we have only snapshots" (user statement). Centrifugo push envelope re-confirmed.

Sample (pretty-printed; level arrays trimmed — the real message carried **24 levels per side**,
so depth is NOT the fixed 50 bitpin uses; possibly variable):

```json
{
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

- **Envelope**: same Centrifugo `push` → `pub` → `data` as bitpin, but the channel format
  differs: `public:orderbook-{market}` (here `BTCUSDT`) vs bitpin's `orderbook:{market}`.
  NO `symbol` field inside `data` — the channel string is the ONLY market key.
- **Levels**: `bids`/`asks` are `[price, qty]` **string** pairs ✅ (asks listed first in the
  payload; asks price-ascending, bids price-descending). Prices may lack decimals (`"62678"`).
- **`lastTradePrice` is a string** ✅ (unlike bitpin's numeric `price`); `lastUpdate` is
  epoch-millis as a JSON number — both metadata, not book levels.
- **No snapshot/update discriminator** — no `event`/type field at all; snapshot regime implied
  by the feed.
- **No seq field in `data`**; **ordering field for the job-2 out-of-order check =
  `pub.offset`** (REVISED 2026-07-14, user: snapshot feeds still need out-of-order
  detection — drop any message whose ordering value is not greater than the last seen.
  No gap/jump rule; gaps self-heal on the next snapshot). `data.lastUpdate` (epoch-millis)
  is a fallback candidate if `pub.offset` ever proves unreliable.
- **Multi-doc records: CLOSED 2026-07-14 (user)** — ex1 records always contain ONE JSON
  document; the discarded-capture 2-newline-concatenated-docs lead was an artifact. No
  splitting logic in job 1.

## ex2-raw — bitpin

**Captured 2026-07-14** (supplied by team). Regime **re-confirmed: full snapshot on every
message** — "always we have snapshot" (user statement). Centrifugo push envelope re-confirmed.

Sample (pretty-printed; level arrays trimmed — the real message carried **50 levels per side**):

```json
{
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

- **Envelope**: Centrifugo `push` → `pub` → `data`; market key is in `push.channel`
  (`orderbook:{market}`, here `BTC_USDT`) and duplicated in `data.symbol`.
- **Levels**: `bids`/`asks` are `[price, qty]` **string** pairs ✅ (BigDecimal-from-string,
  no numeric-literal hazard for the levels). Bids sorted price-descending, asks ascending.
- **⚠ `data.price` is a JSON number** (last price, not a book level) — irrelevant to the book,
  but if ever read, it needs `USE_BIG_DECIMAL_FOR_FLOATS`.
- **No snapshot/update discriminator** — `event` is always `market_data`; snapshot regime is
  implied by the feed, not flagged per message.
- **No seq field in `data`**; **ordering field for the job-2 out-of-order check =
  `pub.offset`** (REVISED 2026-07-14, user — drop stale/out-of-order snapshots; no
  gap/jump rule). `event_time` (ISO string) is a fallback candidate.
- `volume_ask`/`volume_bid`, `event_time`: metadata, not book levels.

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
]]
```

```json
["BTCUSDT@sellDepth", [
  {"price": 62579.56, "quantity": 0.004585, "sum": 286.9272826},
  {"price": 62619.76, "quantity": 0.002,    "sum": 125.23952},
  {"price": 62634.08, "quantity": 0.048566, "sum": 3041.88672928}
]]
```

Parsing notes (job 1):

- **Envelope**: NOT Centrifugo — the top level is a 2-element JSON **array**:
  `["{market}@{side}", [levels…]]`. Market key + side both live in that first string
  (`BTCUSDT@buyDepth` / `BTCUSDT@sellDepth`); `buyDepth` = bids, `sellDepth` = asks.
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

**Captured 2026-07-14** (supplied by team). Regime **re-confirmed: full snapshot on every
message** (both sides present) — "bitget (ex5) is all snapshot" (user statement), and the
message itself says so: `action: "snapshot"` — the FIRST exchange with an explicit
snapshot/update discriminator on the wire.

Sample (pretty-printed; level arrays trimmed — the real message carried **50 levels per side**,
matching the `books50` channel name):

```json
{
  "action": "snapshot",
  "arg": {
    "instType": "SPOT",
    "channel": "books50",
    "instId": "BTCUSDT"
  },
  "data": [
    {
      "asks": [
        ["62815", "0.021591"],
        ["62815.9", "0.001"],
        ["62817.32", "0.015919"]
      ],
      "bids": [
        ["62814.99", "6.180672"],
        ["62814.77", "0.1612"],
        ["62814.23", "0.910845"]
      ],
      "ts": "1784026071995",
      "seq": 655666926391,
      "pseq": 0
    }
  ],
  "ts": 1784026072003
}
```

Parsing notes (job 1):

- **Envelope**: NOT Centrifugo — bitget's own WS shape: top-level `action` / `arg` / `data` /
  `ts`. Market key is `arg.instId` (`BTCUSDT`, must match `exchange_markets.market`); `arg.channel`
  is `books50` (depth encoded in the channel name), `arg.instType` is `SPOT`.
- **`data` is an ARRAY** containing the book object (one element in this sample) — the parser
  must unwrap the array, not treat `data` as an object.
- **`action: "snapshot"` is an explicit discriminator** — unlike ex1–ex4, no need to infer the
  regime from the feed. Snapshot-only per the user, so other `action` values are not expected
  on the book channel (any unrecognized frame is discarded per the message-types rule above).
- **Levels**: `asks`/`bids` are `[price, qty]` **string** pairs ✅ (BigDecimal-from-string, no
  numeric-literal hazard). Asks price-ascending, bids price-descending (best-first both sides,
  unlike ramzinex). Prices may lack decimals (`"62815"`, `"62800"`).
- **`seq`: ordering field for the job-2 out-of-order check** (REVISED 2026-07-14, user:
  snapshot feeds still need out-of-order detection — no gap/jump rule, but drop any message
  whose `seq` is not greater than the last seen). Note the discarded capture showed
  non-monotonic `seq` in the topic — under the revised rule that is no longer a reason to
  distrust the field: it is exactly the out-of-order arrival the check exists to drop.
  Inner `ts` (string epoch-millis) is a fallback candidate. `pseq` stays metadata.
- **Two timestamps**: inner `ts` is a **string** epoch-millis (`"1784026071995"`), top-level
  `ts` a JSON **number** (1784026072003, slightly later — likely send time). Metadata.

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
  `books50`).
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
  Differences from bitget: no top-level `ts`, no `instType`, `arg` carries a `grouping`
  field, and `action` has TWO values (`snapshot`/`update`) instead of snapshot-only. Not a
  new envelope shape — a variant of ex5's.
- **Market key**: `arg.instId` (`BTC-USDT` → `exchange_markets.market`) — note the DASH,
  unlike every other exchange's `BTCUSDT`. Channel identity is `arg.channel`
  (`books-grouped`) + `arg.grouping` (`"1"`): a price-GROUPED book with bucket size 1 —
  which is why every price in the sample is a bare integer.
- **`action` is the regime discriminator**: `"snapshot"` (full book) or `"update"` (only
  changed levels — may include deletes and brand-new prices). This is what job 2's
  snapshot/update classification reads. (Third exchange with an explicit discriminator,
  after bitget's `action` and bybit's `type`.)
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
