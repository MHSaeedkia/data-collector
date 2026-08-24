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

- **ex9 lbank is SEEDED BUT NOT IMPLEMENTED** (teammate commit `195a735`, 2026-07-20 — DB seed +
  `markets.csv` only; a repo-wide grep for `lbank` hits nothing else). Job 1 drops every ex9
  message via the same `dropped-no-parser` counter as ex7, so the gap is SILENT — no dead-letter,
  no error, just absent data. Needs wire samples → `sample-raw-data.md` § ex9 → an `LbankParser`.
  Wire symbols are lowercase-underscore (`btc_usdt`) and the lookup key is an exact case-sensitive
  `"{exchange_id}|{market}"`, so a case mismatch would also drop silently. lbank reuses okx's
  `market_id`s, so jobs 3–6 already resolve and need nothing. Tasks in todo.md M9.
- **ex7-raw stays in the source pattern**; scope lives ONLY in `Parsers.byExchangeId()`
  and `PairExtractFunction` drops unparsered exchanges via the `dropped-no-parser`
  counter. Rationale: one place to change when ex7 lands; also safely absorbs any future
  `ex{n}-raw` topic (warmup.sh is DB-driven, so new subscribed exchanges get topics).
  **This paid off 2026-08-24**: landing ompfinex cost exactly one map entry (`7, new
  OmpfinexParser()`) and no job, source-pattern or topic change anywhere. The map is now
  1–8; ex9 lbank is the only remaining drop-with-counter exchange. See the dated ex7
  section at the end of this file.
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
