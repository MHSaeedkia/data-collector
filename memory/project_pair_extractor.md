---
name: pair-extractor
description: M2 DONE 2026-07-15 — job-pair-extractor module (raw pipeline job 1): parser conventions, per-exchange event_time/seq stamping, drop rules, and the decisions made at implementation
metadata:
    type: project
---

# Job 1 — pair extractor (Milestone 2, done 2026-07-15)

`flink/normalizer/job-pair-extractor/` ([[normalizer-scaffold]] conventions), consumes
`^ex[0-9]+-raw$` → emits `ex{exchange_id}-p{pair_id}-raw-flink`. 26 tests green; live smoke
passed for ex1 (snapshot) and ex8 (update incl. qty-"0" delete) on the local stack.

## ex1 nobitex — snapshot/update split (REVISED 2026-07-21, was "only snapshots")

**⚠ REVISED AGAIN 2026-09-02 — the "WS = delta" call below was itself wrong. See the dated
section near the end of this file.** WS pushes are full snapshots too, ordered by their own
`pub.offset` but never jump-checked.

**The original "ex1 is a full snapshot on every message" assumption was WRONG.** nobitex serves
the initial book over REST and then only **deltas** over WebSocket. NiFi now publishes TWO
payload shapes to `ex1-raw`, and `NobitexParser` branches on them:

- **REST snapshot**: detected by top-level `"action":"snapshot"`. NiFi **injects the market as a
  top-level `"pair"` field** (the REST body has no symbol) → `type="snapshot"`,
  `sequence_id=null`, `sequence_jump=0`, event_time=`lastUpdate`. Levels are `[price,qty]` string
  pairs, same as the WS side.
- **WS delta**: the existing Centrifugo push (channel `public:orderbook-{market}`), **no `action`
  field** → `type="update"`, `sequence_id=pub.offset`, **`sequence_jump=1`** (Centrifugo offsets
  increment by exactly one). Was `type="snapshot"`/jump 0 before this change.
- Noise (acks/pings/malformed) still dropped by the whitelist rule.

**Coupled job-2 change** (see [[type-validator]]): the REST snapshot has no offset, so job 2
treats a **null-seq snapshot as a resync** — the first WS update after it adopts its offset as
the baseline (new `baselinePending` state, exchange-agnostic; ex3 also hits it harmlessly).
**Deploy note: parser + job 2 + NiFi's REST feed must cut over together** — flipping WS to
`update` without REST snapshots flowing makes every ex1 update reject `no_baseline`.

Fixtures: `ex1-snapshot.json` is now the REST payload; the old WS Centrifugo message moved to
`ex1-update.json` (BitpinParserTest's foreign-channel case + smoke both follow it).

## ex2 bitpin — same split (2026-07-25, was "snapshot on every message")

**⚠ REVISED AGAIN 2026-09-02 — same correction as ex1, same reason. See the dated section near
the end of this file.**

**The exact same wrong assumption, corrected the same way.** bitpin also serves the initial book
over REST and only deltas over WS, so `BitpinParser` is now structurally identical to
`NobitexParser`:

- **REST snapshot**: `"action":"snapshot"` + NiFi-injected top-level `"pair"` → `type="snapshot"`,
  `sequence_id=null`, jump 0, event_time = `event_time`.
- **WS delta**: the Centrifugo push on `orderbook:{market}` → `type="update"`,
  `sequence_id=pub.offset`, `sequence_jump=1`, event_time = `data.event_time`.

**⚠ `event_time` has TWO wire types inside ex2** (user, 2026-07-25, revising the initial
same-format assumption): the WS field is an **ISO-8601 string** (`Instant.parse`), the REST field
is **epoch millis as a JSON number** (read verbatim via `asLong`). Same name, different type — the
two branches must not share a timestamp code path, and the required-field checks
(`isTextual` vs `isIntegralNumber`) are what enforce it.

**`pair` must be `BTC_USDT` with the underscore** (user-confirmed 2026-07-25) — the DB market
string and the WS channel suffix agree, and job 1's `"2|{market}"` lookup is exact, so `BTCUSDT`
would drop silently. This is the standing NiFi-contract trap ([[type-validator]] / the lbank note
below).

**No job-2 change was needed** — `baselinePending` and the null-seq `out_of_order` guard were built
exchange-agnostic for ex1, so ex2 inherited both. **Same coupled-deploy warning as ex1**: parser +
NiFi's ex2 REST feed cut over together or every ex2 update rejects `no_baseline`.

Runnable e2e coverage: [[e2e-harness]] scenarios 13–17 (mirror of ex1's 08–12), including one
source that is a REST snapshot with a *string* `event_time` — the drop-with-no-dead-letter failure
the two wire types make possible.

**New cross-exchange hazard:** ex1 and ex2 REST snapshots are now structurally IDENTICAL except
for the timestamp field's NAME — both are `action`+`pair`+epoch-millis (`lastUpdate` vs
`event_time`). Each parser's snapshot branch requires its own name, so each rejects the other's
payload; tests pin both directions. Production never cross-parses (`Parsers.byExchangeId()` routes
by topic), but **do not relax those field checks to "action only"** — the name is the only
discriminator left.

Fixtures: `ex2-snapshot.json` is now the REST payload; the old WS message moved to
`ex2-update.json` (Nobitex/Ramzinex foreign-frame tests + smoke follow it). Unit-verified only —
**not run live**.

## Decisions made at implementation (were open in todo.md)

- ~~**ex9 lbank is SEEDED BUT NOT IMPLEMENTED**~~ **RESOLVED 2026-08-26** — see the dated ex9
  section at the end of this file. The 2026-07-20 seed (teammate commit `195a735`) sat unparsed
  until `LBankParser` landed 2026-08-25 (`977e770`) and the rest of the pipeline followed. The
  prediction recorded here held on both counts: wire symbols ARE lowercase-underscore
  (`btc_usdt`), and lbank DOES reuse okx's `market_id`s, so jobs 3–6 needed nothing.
- **ex7-raw stays in the source pattern**; scope lives ONLY in `Parsers.byExchangeId()`
  and `PairExtractFunction` drops unparsered exchanges via the `dropped-no-parser`
  counter. Rationale: one place to change when ex7 lands; also safely absorbs any future
  `ex{n}-raw` topic (warmup.sh is DB-driven, so new subscribed exchanges get topics).
  **This paid off twice**: landing ompfinex (2026-08-24) and lbank (2026-08-25) each cost
  exactly one map entry and no job, source-pattern or topic change anywhere. The map is now
  **1–9, i.e. every seeded exchange**, so `dropped-no-parser` no longer fires for anything in
  the DB — only for a future `ex{n}-raw` topic nobody has added to the map. See the dated ex7
  and ex9 sections at the end of this file.
- **Offsets: `latest`** (consistent with the aggregator — live feed, no replay).
- **event_time stamping per exchange** (job 2 and audits read this): ex1 `data.lastUpdate`,
  ex2 `data.event_time` (ISO-8601 → epoch millis), ex5 inner string `ts` (which is ALSO its
  sequence id since 2026-08-22), ex6 `cts`
  (matching-engine time — chosen over outer `ts` as the analog of okx's data ts; revisit if
  the team prefers gateway time), ex8 `ts` (also the sequence id). **ex3 and ex4 have NO
  message-level timestamp → job-1 processing time** (`System.currentTimeMillis()`), flagged
  per [[raw-pipeline-decision]]. **ex7 is the first exchange to MIX the two** (2026-08-24):
  its REST snapshot carries a real wire time (`data.time`, epoch MICROseconds, divided by
  1000) while its WS deltas carry none and get processing time. See the dated ex7 section
  for why that mix is worth watching.
- **Wire level order is passed through untouched** (including ex4 ramzinex's DESCENDING
  sells — best ask LAST). Sorting is job 5's concern; nothing in jobs 2–4 assumes order.
- **Drop counters** (Flink metrics on the flatMap): `dropped-no-parser`,
  `dropped-unparseable`, `dropped-unknown-market` (+ WARN log with the market string).
  Nothing job-1 drops is dead-lettered — dead-letter starts at job 2.

## Parser conventions (module `io.tibobit.normalizer.pairextract`)

- `parser/RawExchangeParser.parse(byte[]) → List<ParsedBookEvent>` (market string + event;
  exchange_id AND pair_id stamped by `PairExtractFunction` after the
  `RefreshingLookup`/`ExchangeMarketsLoader` resolution, key `"{exchange_id}|{market}"`,
  query = whole `exchange_markets` table).
- Whitelist rule: **return empty list** for frames that don't match the exchange's book
  shape (acks, pings, other channels); **throwing is also fine** — the caller catches
  everything, counts, drops. Never emit a partial book (a malformed level fails the whole
  frame — `Levels` helpers throw).
- Shared helpers: `Json.MAPPER` (USE_BIG_DECIMAL_FOR_FLOATS — mandatory for ex3/ex4
  JSON-number levels, [[bigdecimal-rules]]; numeric → `decimalValue().toPlainString()`),
  `Centrifugo.push()` (ex1/ex2/ex4 envelope), `Levels.fromStringPairs/fromNumericArrays/
  fromPriceQuantityObjects`.
- ex6/ex8 sides: **missing key → null side, present-but-empty → empty list** (a delta may
  touch one side; null-vs-empty semantics per [[avro-schema-orderbook]]). ex3: exactly one
  side set, the other null; `sequence_id` null. **ex3's two per-side messages are NOT combined
  here** — job 1 stays stateless; the two-sided merge is the book-build step's job (step 5),
  decided 2026-07-15, see [[raw-pipeline-decision]].
- Test fixtures = verbatim samples from `sample-raw-data.md` in
  `src/test/resources/fixtures/` — if a sample is re-captured, update BOTH places.

## Latency timings (added 2026-07-15, see [[raw-pipeline-decision]] / [[avro-schema-orderbook]])

`PairExtractFunction.flatMap` stamps `pipeline_timings.pair_extract_in` (captured once at
`flatMap` entry = "came from the raw topic") and `pair_extract_out` (`now`, before each
`collect`) on every emitted event. All events parsed from one message share the same `_in`.
It reads `event.getPipelineTimings()` (parsers leave it an empty non-null instance). Downstream
stages stay null until their jobs run.

## E2E smoke test (added 2026-07-15)

`flink/normalizer/smoke-pair-extractor.sh` — repeatable live test of job 1 against the running
stack. For each of the 10 fixtures it produces the verbatim raw JSON to `ex{id}-raw` and reads
back the emitted Confluent-Avro event on `ex{id}-p1-raw-flink`, asserting
exchange_id/pair_id/type/sequence_id/side-shape. Preconditions: stack up, DB warmed, job
submitted (checks Flink REST for a RUNNING `normalizer-pair-extractor`, else errors). All warmed
BTC markets map to `market_id 1`, so every fixture routes to `p1` (asserted `EXPECTED_PAIR_ID`).

- **Determinism is the whole trick**: a fresh console-consumer with `auto.offset.reset=latest`
  races the emit — if the record lands just before the consumer positions, `latest` skips it
  (this was a real flaky FAIL on ex4, green in isolation). Fixed by snapshotting the output
  topic's end offset with `kafka-get-offsets` BEFORE producing, then reading `--partition 0
  --offset <that>`. No group, no sleep, no race. Don't "simplify" it back to a plain consumer.
- **Decoded Avro union-wrapping** (kafka-avro-console-consumer / Avro JsonEncoder): `sequence_id`
  → `{"long":N}` or `null`; `asks`/`bids` → `{"array":[…]}` or `null`. Assertions use
  `.sequence_id.long`, `.asks.array|length`, etc. log4j also prints to **stdout**, so the record
  line is isolated with `grep '^{'`.
- **Assertions are build-independent** (exchange_id/pair_id/type/seq/shape), NOT pipeline_timings —
  the script only prints a soft "(no pipeline_timings on this build)" note when the field is absent.
  As of 2026-07-15 the timings-enabled build IS deployed and the registry schema updated (below), so
  a green run shows NO such note = timings are on the wire. Keeping the assertion build-independent
  still protects against a future timings-less build.
- **Sink needs the registry schema to carry `pipeline_timings`** (fixed 2026-07-15). The rebuilt
  (timings-enabled) job hit `NullPointerException: Schema.getField("pipeline_timings") is null` at
  `RawOrderBookEventSerializer.toGenericRecord` (job RUNNING→FAILED at first emit) because the
  registry's `raw-order-book-event` subject was still v1 (no timings) while `schemas/
  raw_order_book_event.avsc` + the code both had it. Fix: register the updated avsc (idempotent POST
  like warmup.sh → v2, additive nullable field, compat-passed). The serializer fetches the schema
  from the registry at runtime, so a stale registry breaks the sink even with correct code+jar.
  Re-run `scripts/warmup.sh` (or POST the one subject) after any raw-pipeline schema change.

## Gotchas

- **Postgres JDBC driver is NOT in the Flink image** — it ships in the job's shaded jar
  (parent pom manages `org.postgresql:postgresql` 42.7.3 compile-scope; jackson-databind is
  managed `provided` at the image's 2.14.3). Jobs 3/4 (also DB-reading) reuse both entries.
- **`ExchangeMarketsLoader.load()` MUST `Class.forName("org.postgresql.Driver")` before
  `DriverManager.getConnection`** (fixed 2026-07-15). The driver class + `META-INF/services/
  java.sql.Driver` ARE in the shaded jar, but DriverManager's lazy ServiceLoader auto-registration
  runs under the parent classloader and doesn't reliably see a driver in Flink's child-first
  user-code classloader → intermittent `No suitable driver found for jdbc:postgresql://…` at
  `open()` (job dies INITIALIZING→FAILED). It's NONDETERMINISTIC: works when DriverManager was
  already warmed by an earlier job on that TaskManager, fails cold — which is why an earlier smoke
  run passed and a later cold submit didn't. Jobs 3/4 (also DB-reading) need the same explicit load.
- kafka-avro-console-consumer (in the schema-registry container) is the quickest smoke-read
  of the output topics; kafka-ui has no serde binding for `*-raw-flink` topics until M8.

**Why:** jobs 2–6 consume this job's output and inherit these semantics; the open decisions
above were resolved here, not in the earlier design docs.
**How to apply:** when adding an exchange, add ONE parser + one `Parsers` map entry + a
fixture section in sample-raw-data.md; when changing event_time/ordering semantics, update
job 2's expectations too.

**2026-08-03 — `simulation` flag.** All 7 parsers lift it off the payload via `Json.simulation(carrier)`. ⚠ **ex3/wallex's envelope grew a THIRD element** — `["{market}@{side}", [levels…], {"simulation":N}]` — because its root is an array with no field to inject; the other six read it as a root field. `WallexParser` takes 2 or 3 elements, drops 4+. Cross-parser rule is tested once in `SimulationFlagTest`, not per parser. See [[simulation-flag]].

**2026-08-22 — ex4/ramzinex re-verified against a SECOND live capture** (user-supplied,
`orderbook:11`, `pub.offset` 10298388, 50+50 levels, rial-scale prices ~1.9M). Ran
`RamzinexParser` on the raw frame: every documented assumption held — Centrifugo envelope,
numeric channel id as the market key (`"11"`), `buys`→bids / `sells`→asks, 7-element
JSON-number levels with only `[price, qty]` read, exact decimal literals preserved
(`1398.2677`, `450`, no scientific notation), `seq = pub.offset` with jump 0, type
`snapshot`, event time = processing time. **BOTH sides price-descending re-confirmed** (best
bid FIRST `1910670`, best ask LAST `1910700`, spread +30) — the single riskiest ex4
assumption, now corroborated by two independent captures a month apart.
Two things the second capture changed:
- **Element 6 range is wider than sample-raw-data.md records** (5–221 here vs the documented
  "10–74"), and it tracks quantity magnitude (`0.23`→5, `1398.2677`→111, `10043.5`→221) —
  so it is a UI depth-bar scale, NOT an order count. Still ignored; the doc's guess at its
  meaning is the only thing stale.
- **Market `"11"` has no `exchange_markets` row in the local seed** (ramzinex's local rows are
  `12/2/13/3/643/…`, no `11`), so this frame drops on `dropped-unknown-market` locally. Consistent
  with the known local-seed-vs-server drift in [[db-schema]] — verify the server row exists AND
  that its `price_amount_rebase` is `-1` if pair 11 is IRT-quoted: a 1.9M price is rial-scale,
  and a missing `-1` would publish prices 10× too high.
The raw frame as supplied carries no NiFi `id`/`simulation`, so it parses but `PairExtractFunction`
drops it on `dropped-no-id` — expected for a pre-NiFi capture, not a defect.

**2026-08-23 — ex5/bitget gained a SECOND stream: the REST depth snapshot.** `ex5-raw` now
carries the same REST+WS split ex1 and ex2 have. NiFi tags the REST body `"action":"snapshot"`
and injects the market as a top-level `pair`, exactly as it does for those two — so, as with
them, **`action` cannot be the discriminator**: it reads `"snapshot"` on both streams. The
parser branches on the shape of `data` (object = REST, array = WS), which is the only field that
differs unconditionally.

It differs from the WS frame on every axis that matters, which is why it is a whole branch and
not a few `path()` fallbacks: market key `pair` vs `arg.instId`; `data` a single OBJECT vs an
array (so this stream can never fan one record into several events); sides spelled `a`/`b` vs
`asks`/`bids`; and **levels are JSON NUMBERS**, so it reuses ex4's `Levels.fromNumericArrays`
(BigDecimal from the literal, then `toPlainString` — `1.448` stays `"1.448"`). Both sides are
required: it is always a full book, never a per-side snapshot.

> ⚠ **SUPERSEDED 2026-08-23 (2) by commit 65f2f5d — the paragraph below records a decision
> that was REVERSED.** The ex5 REST snapshot is now **null-seq / jump 0** and the WS window is
> **650 ± 110**; sequencing it by its own `data.ts` caused a live resync loop. Kept for the
> reasoning trail only. The correction lives in [[project_type_validator]] and
> `sample-raw-data.md § ex5 REST`.

**Sequencing — the decision worth remembering.** The REST snapshot carries `data.ts` as its
sequence id with jump 600 and tolerance 10, i.e. it is treated *identically to a WS snapshot*.
This was the user's explicit choice on 2026-08-23, taken over the offered alternative of the
null-seq `baselinePending` bootstrap that ex1/ex2 give their REST bodies. The consequence, and
the reason it is written down: **the first WS update after a resync must land 590–610 ms after
the REST book's own timestamp**, or job 2 dead-letters it `sequence_gap`. That is the
snapshot→update risk already logged against the WS feed, now extended to the resync path — see
[[project_type_validator]] and todo.md. `requestTime` is ignored (it is the API round trip, not
the book), and `code`/`msg` are not inspected because an error body has no `data.a`/`data.b` and
already fails the shape whitelist.

**No job-2 change was needed** — a REST snapshot produces the same `RawOrderBookEvent` a WS one
does, so every rule downstream was already written for it. The change is job 1 only, plus tests
and docs: `BitgetParser` (split into `parseRestSnapshot` / `parseWsFrames`), the new fixture
`ex5-rest-snapshot.json` (the real XRPUSDT capture, trimmed to 3 levels a side, `id`/`simulation`
omitted like every other fixture), 2 new `BitgetParserTest` cases, the ex5 rows of
`SimulationFlagTest`/`RecordIdTest`, e2e `Ex5RestSnapshotResync`, and `sample-raw-data.md § ex5`.
All 231 normalizer tests green; both parser mutations bite (kill the `data`-shape discriminator,
or read `requestTime` instead of `data.ts` → `parsesRestSnapshot` fails).


## 2026-08-24 — ex6/bybit gained a SECOND stream: the REST depth snapshot

`ex6-raw` now carries the same REST+WS split ex1, ex2 and ex5 have. Three captures were supplied
(WS snapshot, WS delta, REST `/v5/market/orderbook` response); NiFi always attaches `id` and
`simulation` to all of them.

**The two WS captures needed no change at all.** They matched `BybitParser` as already written —
`u` 210920912 → 210920913 (jump 1 ✅), `seq` 112975848012 → 112975848022 (jump 10, still
unusable), event time `cts`. Re-confirmation, not rework.

**The discriminator is genuinely easy here, and that is worth knowing** because every previous
REST+WS exchange made it hard. ex1/ex2/ex5 all inject `action:"snapshot"` onto the REST body while
their WS stream *also* says `"snapshot"`, so each needed something subtler (ex5 branches on the
shape of `data`). On ex6 the REST book lives under **`result`** and the WS book under **`data`**,
and the WS frame has no `action` field at all — so `root.path("result").isObject()` is sufficient
and unambiguous. Don't copy ex5's shape-trap reasoning onto ex6.

Other REST/WS differences: market key is **`result.s`** (bybit's own symbol — the WS branch uses
`data.s`, so the key derivation stays identical across both streams; the injected root `pair`
agrees and is redundant, unlike ex1/ex2/ex5 whose REST bodies carry no symbol). Both sides
required (always a full book). **Levels are string pairs on BOTH streams** — ex6 has no
JSON-number hazard anywhere, unlike ex5 whose REST body switched to numeric literals. Event time
is **`result.cts`**, the same matching-engine field the WS branch reads, so both ex6 streams share
one event-time clock — cleaner than ex5, which only had a gateway `ts`.

**⚠ The sequencing decision, and the reason this one was not a judgement call.** The REST snapshot
is **null-seq / jump 0**, because `result.u` is demonstrably not on the WS counter — and the proof
is arithmetic rather than statistical, which is why no live measurement was needed this time. The
REST capture is **24.3 hours LATER** than the WS pair yet its `u` is **171,928,550 LOWER**
(38,992,362 vs 210,920,912). A monotonic counter cannot run backwards, so they are two separate
counters. (Most plausibly the REST `updateId` is scoped per request depth rather than to the
`orderbook.50` topic — **that explanation is unconfirmed; the incomparability is not.**) Adopting
it would break in whichever direction the numbers happened to fall: forward → `sequence_gap`;
backward, which is the actual case → immediate `stale_or_duplicate`. Either way it is
**exactly the ex5 resync loop** (accept → reject → empty the book → request another snapshot →
repeat, which cost 28.6 resets/min on the dev server). So ex6 takes the `baselinePending`
bootstrap: job 2 orders the body by event time and the first WS delta after it adopts its own `u`
as the baseline. **Never compare the two counters.** `result.seq` is unusable for the reason it
always was here (moves 10 per `u`, cross-topic metadata). `result.ts` and the top-level `time` are
ignored; `retCode`/`retMsg` are not inspected because a bybit error body answers `"result": {}`,
which has no `a`/`b` and already fails the shape whitelist.

**A shape question the new capture settled.** An unchanged side on a live delta arrives as a
present-but-**EMPTY** array (`"b": []`), not as an absent key. This is safe, but the reason is
narrower than the ex6 docs previously implied: job 5 clears a side before merging **only when the
type is `snapshot`**, so on an update an empty array merges nothing. Null (absent key) and empty
therefore behave identically on updates and differ only on snapshots. The old `data_ex6.go` header
said a present-but-empty array "clears it" without that qualifier — corrected, and both cases are
now pinned by tests.

**No job-2 change was needed** — the null-seq `baselinePending` path is exchange-agnostic and
already existed for ex1/ex2/ex5. Job 1 only, plus tests and docs: `BybitParser` split into
`parseRestSnapshot` / `parseWsFrame`, the new fixture `ex6-rest-snapshot.json` (the real BTCUSDT
capture trimmed to 3 levels a side, `id`/`simulation` omitted like every other fixture), 5 new
`BybitParserTest` cases (3 → 8), the ex6 rows of `SimulationFlagTest`/`RecordIdTest`, e2e
`Ex6RestSnapshotResync`, `sample-raw-data.md § ex6`. All 126 job-1 + common tests green.
**⚠ e2e NOT run live** — the harness needs the docker stack and does a destructive `down -v`; the
scenario compiles and vets clean but has not been executed.

## 2026-08-24 — ex7/ompfinex LANDED, resolving the 2026-07-14 postponement

A teammate implemented `OmpfinexParser` on `feat/add-ompfinex` (commits `80fa07d`, `cb4b3a7`,
`63b6588`) together with six e2e scenarios. Reviewed 2026-08-24; the code is sound, the
**evidence behind it is the weak part** — read the caveats below before trusting ex7 in
production.

**The regime: REST snapshot + Centrifugo WS delta, a TRUE delta feed** (the ex6/ex8 family), NOT
ex1/ex2's null-seq resync pattern — even though ex7 is a Centrifugo exchange like ex1/ex2/ex4 and
reuses the same `Centrifugo.push` envelope helper. That combination is new: **ex7 is the first
Centrifugo exchange with real delta semantics**, so "Centrifugo ⇒ ex1-style null-seq bootstrap" is
no longer a safe inference. Discriminator is easy, ex6-style: the REST body has a top-level
`action:"snapshot"`, the WS frame has no `action` at all.

**⚠ The sequencing decision, and why it is different from every other exchange.** The parser
stamps `sequence_jump = u - U` **per message** — the first DYNAMIC jump in the platform; every
other exchange has a constant from the exchange's cadence (1, 300, 600±10) or 0. Algebraically
job 2's check `seq == lastSeq + jump` reduces to **`U == lastSeq`**, i.e. Binance-style
diff-depth contiguity but with **`U_n == seq_{n-1}`, NOT Binance's `U_n == seq_{n-1} + 1`**.
Two consequences worth holding onto:

- It is *more* robust than the fixed-jump exchanges, not less. A replayed duplicate carries the
  old `U`, so it fails the equality and falls to `stale_or_duplicate` correctly — it does not hit
  the `jump == 0` hole recorded in todo.md against the fixed-jump feeds.
- But job 2 is no longer independently validating contiguity: both operands come from inside the
  same message. The check is only as good as the exchange's own `U`.

**⚠ The REST snapshot is SEQUENCED (`data.lastUpdateId`), not null-seq — the opposite of the ex6
call.** This is the single riskiest decision on the branch. It is correct ONLY if the REST
`lastUpdateId` sits on the same counter as the WS `u`, which is exactly the thing ex6 proved false
for bybit (24.3 h later, 171,928,550 lower) and which cost ex5 a live resync loop. The claim rests
on **two consecutive live samples** where the second message's `U` (859075) equalled the first's
`u` (859075). That is enough to establish the `+0` convention; it is **not** enough to establish
that a REST snapshot fetched mid-stream lands contiguously with the next delta. In production NiFi
fetches the snapshot while deltas keep flowing, which is the race Binance's own docs prescribe
buffering for. **If `U > lastUpdateId` on a live resync, ex7 enters the ex5 loop**: gap → snapshot
request → snapshot → gap. Nothing in the e2e suite can surface this, because every scenario
constructs `U == prev seq` by hand. **Verify against the live feed before trusting ex7.**

**⚠ Both side keys are REQUIRED, unlike ex6/ex8.** `a` and `b` must each be an array or the WHOLE
message is dropped (both sides, silently, via `dropped-unparseable` — no dead-letter, so a drop
here is an invisible sequence gap). ex6/ex8 instead pass `null` for an absent side. The
justification is a wire claim — "a side key is always present, possibly empty" — and an empty
array is a genuine no-op on an update (job 5 clears a side only when `type == "snapshot"`, the
same narrow reason recorded for ex6). **If that claim is wrong, ex7 loses messages silently.**
Making ex7 tolerate a missing side the way ex6 does would cost two `has()` checks and remove the
whole failure mode; worth considering.

**Event time mixes two clocks** — REST snapshot = `data.time` (epoch MICROseconds ÷ 1000), WS
delta = job-1 processing time. ex4/ramzinex is fully processing-time and therefore internally
consistent; ex7 is the first that is not. Job 2 is unaffected (ex7 is sequenced, so the
event-time guard never runs), but a snapshot stamped with exchange time can land BEHIND deltas
stamped with local time, and the micros÷1000 conversion is asserted **nowhere** — every ex7
scenario sets `IgnoreEventTime`, so a factor-1000 error would leave the suite green.

**What is missing relative to every other parser** (all convention, all cheap):
`sample-raw-data.md` § ex7 still says POSTPONED and holds **no captures**, so none of the four
"CONFIRMED" wire claims in the javadoc are reproducible by anyone else; there is no
`OmpfinexParserTest` (the other seven parsers each have one); there are no `ex7-*.json` fixtures;
and ex7 is absent from the two cross-parser convention tests, `SimulationFlagTest` and
`RecordIdTest`, which enumerate ex1–6+8. The parser *does* call `Json.simulation`/`Json.sourceIds`
correctly, so it would pass those — nothing pins it.

## 2026-08-26 — ex9/lbank LANDED, closing the last seeded-but-unparsed exchange

A teammate implemented `LBankParser` on `feat/add-lbank` (commit `977e770`, 2026-08-25 — parser +
one line in `Parsers.byExchangeId()`, nothing else). Everything around it — javadoc, unit tests,
fixtures, `sample-raw-data.md` § ex9, five e2e scenarios — landed 2026-08-26 after a review of the
parser and four wire samples supplied by the user. **The parser's LOGIC was not changed**: the
review raised three questions and the user's answers confirmed all three of its existing choices.

**The regime: SNAPSHOT ONLY.** Every frame is a whole book under `depth`. No delta channel, no
`action`/`type` regime discriminator to read, nothing to make contiguous. ex9 is the **second
snapshot-only exchange** after ex3/wallex — and the pair are not alike: ex3 sends one SIDE per
message with the other null and has no wire clock at all, while ex9 always sends both sides and
has a real timestamp. So ex9 is the first exchange that is **snapshot-only AND event-time-ordered
for real** — ex3's guard can never fire, because its event time is job-1 processing time which
only moves forward.

**⚠ NO sequence field exists anywhere on the wire** — not a counter, not an update id, not a
`lastUpdateId`. **User decision 2026-08-26: `sequence_id = null`, `sequence_jump = 0`.** The
tempting alternative was `TS`-as-sequence, the way ex5 and ex8 use their `ts`; it was rejected
because a timestamp-as-sequence imposes a publish cadence the exchange never promised, and that is
precisely what cost ex5 a live resync loop and forced the platform's only nonzero
`sequence_jump_tolerance`. A null sequence puts ex9 on job 2's **event-time branch**, where the
whole test is "not older than the last accepted frame" — the user's words: *"it is providing all
the snapshots, so just verifying that timestamp is not out of order is good enough."* That is
sound for a full-snapshot feed, because an accepted frame replaces the book outright, so there is
nothing for a sequence number to protect that ordering does not already cover.

**Consequences worth holding onto:**

- ex9 can reach exactly ONE reject reason, `out_of_order`, and can emit **NO control command
  ever** — job 2 only asks for a snapshot on `no_baseline` or `sequence_gap`, and both live in the
  update branch a null-seq feed never enters. The e2e scenarios all assert an EMPTY control
  stream, which is what makes a spurious request a failure rather than something nobody looks at.
- `baselinePending` is set on every accepted ex9 frame and **never consumed** — exactly ex3's
  situation. Harmless, but it means the flag's name is now misleading for two of the three
  exchanges that set it.
- **No job-2 change was needed.** The null-seq guard was built exchange-agnostic for ex1 in July
  and ex9 inherited it whole — the third exchange to do so, after ex2 and ex3.

**⚠ Equal timestamps are ACCEPTED, not rejected** (user decision 2026-08-26). The guard is
`event_time < lastEventTime`, STRICTLY older. Two ex9 frames sharing a `TS` therefore both pass
and the book follows the last one in — which on a ~500 ms feed means a duplicate snapshot is
re-emitted rather than deduplicated. This is deliberate and is pinned by the
`Ex9DuplicateTimestamp` scenario. **Do not "fix" it for ex9 alone**: the guard is shared with ex3
and the ex1/ex2 REST snapshots, so tightening it to `<=` is a three-exchange change.

**⚠ `TS` is the platform's ONLY non-epoch-millis timestamp** — an ISO-8601 local date-time with
millisecond precision and **no zone marker** (`"2026-08-25T17:46:51.723"`). **User-confirmed
2026-08-26 that it is UTC**; the parser reads it with `ZoneOffset.UTC` and one unit test pins the
exact epoch value. This is the parser's single unverifiable-from-the-payload assumption: lbank
documents `TS` as *server* time, and if that ever turns out to be UTC+8 every ex9 event_time is
8 hours in the future — **invisible in the levels, and it would surface only as a staleness
alarm**. One `ZoneOffset` is the whole change if it is revised.

**Frame selection is by SHAPE, not by `type`** (user decision 2026-08-26). All four captures say
`"type":"fdepth"` — futures depth — but the parser requires `pair` + textual `TS` +
`depth.asks`/`depth.bids` as arrays and ignores `type` entirely. Rationale accepted by the user:
lbank's other channels (pings, subscribe acks, ticks, and the incremental `incrDepth` book, whose
levels hang off a differently-named key) already fail that check, so whitelisting the value would
add a second thing to keep in sync for no coverage today — and the spot `depth` channel would then
parse with no change. **The corollary is the risk**: a future lbank channel carrying `pair`, `TS`
and a `depth` object WOULD be misread as a book.

**Both side keys are REQUIRED** — the ex7 rule, not ex6/ex8's null side. Unlike ex7 this is
clearly correct rather than a bet: on a snapshot feed a half-frame would silently WIPE a side,
because job 5 clears a side when `type == "snapshot"`. All four captures carry both.

**The market key is the case trap the 2026-07-20 note predicted.** `pair` is lowercase with an
underscore (`btc_usdt`) — the only exchange that spells it that way (ex1/ex2/ex5/ex6 send
`BTCUSDT`, ex8 `BTC-USDT`, ex4/ex7 a numeric string). It matches `exchange_markets.market`
verbatim and **nothing in job 1 normalizes case**, so a change on either side drops 100% of ex9
silently. Pinned twice: a unit assertion on the parser and source 08 of `Ex9NoiseFrames`, which
sends `BTC_USDT` and expects nothing out.

**What ex9 has that ex7 still does not**: a captured `sample-raw-data.md` § ex9, an
`ex9-snapshot.json` fixture, an `LBankParserTest` (6 tests), rows in both cross-parser convention
tests (`SimulationFlagTest`, `RecordIdTest`), and five e2e scenarios. ex7 is now the ONLY parser
with none of that — see the 2026-08-24 section above.

**VERIFIED: 269 Java tests green (was 259), and all five e2e scenarios PASS LIVE** (2026-08-26,
first attempt, 29–39 s each) against the local docker stack, run without `stack.Provision`. So
unlike ex7 at the same stage, every wire claim in `LBankParser`'s javadoc is reproducible — the
UTC conversion included, since no ex9 scenario blanks event time. **Still unproven**: whether the
suite BITES (no live mutation check yet, unlike scenario 31), and whether `TS` is really UTC —
that one is unfalsifiable from the payload and needs a wall-clock comparison at capture time.
See [[e2e-harness]].

## 2026-09-02 — ex1/ex2 REVISED AGAIN: WS pushes are snapshots, not deltas

User supplied fresh live captures (`nobitex-snapshots.txt`, `bitpin-snapshots.txt` — several
consecutive WS pushes each) and asked for a full audit of every place the codebase assumed the
2026-07-21/07-25 "WS = delta" classification. **That classification was wrong**, confirmed by
inspection of the new captures: consecutive messages each carry the FULL current book (24–50+
levels per side), and a price level that disappears between two consecutive pushes carries no
`qty=0` entry anywhere in the later message — a true delta feed cannot signal a removal that way.
Both AskUserQuestion answers from the user were explicit: treat WS pushes as `type=snapshot`
(mirroring the existing "a snapshot is never jump-checked" pattern already used for ex4/ex5's
REST-sequenced snapshots), and trust the new captures over the July finding.

**The fix is confined to the two parsers — job 2 (type-validator) and job 5 (book-builder)
needed ZERO code changes**, because both were already exchange-agnostic and already supported
exactly this shape: a `snapshot` type carrying a non-null `sequence_id` (ex4 `pub.offset`, ex5
REST `data.ts`) gets ordered by "not `<= last`" (→ `stale_or_duplicate` if it is) with NO jump
check — the class javadoc's own words are "ordering is the WHOLE test... `sequence_jump` is
deliberately never applied to a snapshot." So `NobitexParser`/`BitpinParser`'s WS branch just
needed `type="update"→"snapshot"` and `sequence_jump=1→0` (kept `sequence_id=pub.offset`, which
is strictly better ordering than falling back to event time). This is the same generalization
[[project_type_validator]] already documents for job 2's snapshot branch — nothing new was built,
an existing code path just started being reached by two more exchanges.

**Consequence for the null-seq REST resync bootstrap (`baselinePending`).** It is still set on
every accepted REST snapshot (unchanged — REST behavior did not change), but for ex1/ex2 it is
now **never consumed**, because consuming it happens only in job 2's `update` branch and neither
exchange sends that type any more. ex1/ex2 join ex3/ex9 in the "flag set but never consumed"
category — see the corrected class javadoc in `TypeValidateFunction.java`.

**Consequence for the control plane.** `no_baseline`/`sequence_gap` and the `snapshot_request`
command they trigger live ENTIRELY in job 2's `update` branch (`askForSnapshot`). Since ex1/ex2
no longer produce that type, **the control plane can no longer engage for these two exchanges at
all** — a dropped/missed WS push is silently accepted as the new book (ordered only by
`seq <= last`, no gap detection), by design, per the chosen tradeoff. This was explicitly
disclosed and accepted via AskUserQuestion, not a silent regression. `ControlEx1NoBaselineThenGap`
and `ControlEx1LaggingRestResync` (e2e scenarios 45/47) were **removed** 2026-09-02 since their
premises are now permanently unreachable through ex1; the generic episode/resync-clears-the-flag
machinery they exercised remains covered by the ex6-flavored pair (44/46) and, at the unit level,
by `TypeValidateFunctionTest`'s cases (relabeled from "ex1 nobitex" to "ex6" since ex6's REST
snapshot is also null-seq and is now the only live example of this bootstrap — [[project_type_validator]]).

**e2e scenario rewrite (`e2e/scenario/data_ex1.go`, `data_ex2.go`).** Every WS-driven
`WantSnapshots` entry had to be recomputed: job 5's snapshot branch CLEARS the side before
applying an event's own levels, so a WS push now REPLACES the book wholesale instead of merging —
a previously-resting level not re-sent by that push is simply gone. The Sources JSON payloads
were left untouched (a small 2–3-level WS fixture is still a valid, if sparse, snapshot); only the
expected outputs changed. Three scenarios per exchange had their whole premise invalidated and
were renamed: `Ex{1,2}RestThenWsResync` → `Ex{1,2}WsSnapshotsReplaceWholesale` (no more
REST-bootstraps-WS; each stream is independently ordered by its own counter, and a huge offset
jump — 1001→9000 in the fixtures — is silently accepted), `Ex{1,2}UpdateBeforeSnapshot` →
`Ex{1,2}WsSnapshotAloneEstablishesBaseline` (a WS push with no prior REST snapshot is now
ACCEPTED immediately, not rejected `no_baseline`), `Ex{1,2}SequenceGap` →
`Ex{1,2}WsGapAcceptedStaleRejected` (an offset skip is silently accepted — no gap, no reset, no
control command — but the surviving `seq <= last` check still catches a genuinely out-of-order WS
push as `stale_or_duplicate`, which the rewritten scenario now demonstrates using the fixture's
own out-of-order step rather than inventing a new one). The other three scenarios per exchange
(`NoiseFrames`, `StaleRestReplay`, `PrecisionDust` + ex1's two `Rebase*`) kept their names and
premises; only their post-WS-event expected book state changed to reflect replace-not-merge —
`PrecisionDust`/`RebaseToman`/`RebaseScaledUnit` each lost a previously-resting level from their
"after WS event" expectation for exactly this reason, which is a good worked example of the bug
this fix closes. `scenarios.go` numbering (01–14, 44–48) was NOT renumbered — retired slots (45,
47) are left with an explanatory comment, per the standing "grep identifiers, not numbers"
convention ([[e2e-harness]]).

**Verification:** all 189 Java tests across `common`+`job-pair-extractor`+`job-type-validator`
green, `job-book-builder`'s 28 tests green untouched (confirms it needed no change), `go build`/
`go vet` clean on `e2e/`. **Not run live** — no docker stack available this session.

**Why:** the exchanges' WS feeds may simply have changed behavior since July, or the original
capture/analysis was wrong; either way, don't re-trust the July "delta" classification without a
fresh capture. **How to apply:** if a THIRD capture ever contradicts this again, re-run the same
audit (parser → job 2 → job 5 → e2e → control-plane → docs) before touching code — this file's
own history is the cautionary tale.
