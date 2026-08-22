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
  (1–6 + 8) and `PairExtractFunction` drops unparsered exchanges via the `dropped-no-parser`
  counter. Rationale: one place to change when ex7 lands; also safely absorbs any future
  `ex{n}-raw` topic (warmup.sh is DB-driven, so new subscribed exchanges get topics).
- **Offsets: `latest`** (consistent with the aggregator — live feed, no replay).
- **event_time stamping per exchange** (job 2 and audits read this): ex1 `data.lastUpdate`,
  ex2 `data.event_time` (ISO-8601 → epoch millis), ex5 inner string `ts` (which is ALSO its
  sequence id since 2026-08-22), ex6 `cts`
  (matching-engine time — chosen over outer `ts` as the analog of okx's data ts; revisit if
  the team prefers gateway time), ex8 `ts` (also the sequence id). **ex3 and ex4 have NO
  message-level timestamp → job-1 processing time** (`System.currentTimeMillis()`), flagged
  per [[raw-pipeline-decision]].
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
