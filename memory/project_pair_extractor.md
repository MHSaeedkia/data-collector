---
name: pair-extractor
description: M2 DONE 2026-07-15 — job-pair-extractor module (raw pipeline job 1): parser conventions, per-exchange event_time/seq stamping, drop rules, and the decisions made at implementation
metadata:
    type: project
---

# Job 1 — pair extractor (Milestone 2, done 2026-07-15)

`flink/normalizer/job-pair-extractor/` ([[normalizer-scaffold]] conventions), consumes
`^ex[0-9]+-raw$` → emits `ex{exchange_id}-p{pair_id}-raw-flink`. 24 tests green; live smoke
passed for ex1 (snapshot) and ex8 (update incl. qty-"0" delete) on the local stack.

## Decisions made at implementation (were open in todo.md)

- **ex7-raw stays in the source pattern**; scope lives ONLY in `Parsers.byExchangeId()`
  (1–6 + 8) and `PairExtractFunction` drops unparsered exchanges via the `dropped-no-parser`
  counter. Rationale: one place to change when ex7 lands; also safely absorbs any future
  `ex{n}-raw` topic (warmup.sh is DB-driven, so new subscribed exchanges get topics).
- **Offsets: `latest`** (consistent with the consolidator — live feed, no replay).
- **event_time stamping per exchange** (job 2 and audits read this): ex1 `data.lastUpdate`,
  ex2 `data.event_time` (ISO-8601 → epoch millis), ex5 inner string `ts`, ex6 `cts`
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
  side set, the other null; `sequence_id` null.
- Test fixtures = verbatim samples from `sample-raw-data.md` in
  `src/test/resources/fixtures/` — if a sample is re-captured, update BOTH places.

## Gotchas

- **Postgres JDBC driver is NOT in the Flink image** — it ships in the job's shaded jar
  (parent pom manages `org.postgresql:postgresql` 42.7.3 compile-scope; jackson-databind is
  managed `provided` at the image's 2.14.3). Jobs 3/4 (also DB-reading) reuse both entries.
- kafka-avro-console-consumer (in the schema-registry container) is the quickest smoke-read
  of the output topics; kafka-ui has no serde binding for `*-raw-flink` topics until M8.

**Why:** jobs 2–6 consume this job's output and inherit these semantics; the open decisions
above were resolved here, not in the earlier design docs.
**How to apply:** when adding an exchange, add ONE parser + one `Parsers` map entry + a
fixture section in sample-raw-data.md; when changing event_time/ordering semantics, update
job 2's expectations too.
