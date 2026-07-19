# Todo — Raw Data Normalization Pipeline

NiFi stops normalizing and instead publishes VERBATIM raw exchange payloads to `ex{id}-raw`
(one topic per exchange); a chain of 6 Flink jobs reproduces the normalization, ending in the
existing `ex{exchange_id}-p{pair_id}-{side}` / `price_level_event.avsc` consolidator input
stream. Full decision + rationale: `memory/project_raw_pipeline_decision.md`
(accuracy > latency → one job per step, every intermediate topic is an audit point).

(History note: todo.md was cleaned 2026-07-13 — Phases 1–5 removed, recoverable from git;
the R3-postponed block lives on in `memory/project_orderbook_consolidator_decision.md`.)

## Decided (2026-07-13)

- [x] Structure: ONE Maven multi-module project (`common/` + one `job-*/` module per job, each
      its own shaded jar; build one via `mvn -pl <module> -am package`); existing flink projects
      stay self-contained as-is
- [x] Pipeline (6 jobs; topic names are the ground truth):
      1. pair extraction: `ex{id}-raw` → `ex{id}-p{id}-raw-flink`
      2. type validation: → `ex{id}-p{id}-type-validated-raw-flink` (rejects → dead-letter)
      3. rebase: → `ex{id}-p{id}-rebased-flink`
      4. precision: → `ex{id}-p{id}-applied-precision-flink` (spelling "precision" assumed)
      5. orderbook build: → `ex{id}-p{id}-orderbook-snapshot-flink`
      6. price-level emit: → existing `ex{id}-p{id}-{side}` topics
- [x] Raw format: verbatim exchange payload (no envelope); job 1 owns ALL per-exchange parsing
- [x] **Parse point**: job 1 converts payloads into ONE common structured Avro event
      (exchange/pair ids, type, sequence fields, asks/bids levels); one shared schema serves
      job 1–4 output topics; `-raw` in later names = "not yet rebased/normalized"
- [x] **Rebase formula**: `value × 10^rebase` per `exchange_markets.price_amount_rebase` /
      `volume_amount_rebase` → `BigDecimal.scaleByPowerOfTen(rebase)`, exact
- [x] **Precision rounding**: truncate — `setScale(precision, RoundingMode.DOWN)`
- [x] **DB reference data** (jobs 1/3/4): periodic refresh from Postgres inside the job
      (env-configurable interval), no restart needed for new markets/rebase/precision rows
- [x] Job-2 rejects go to a dead-letter topic (accuracy-first auditability)

## Proposed defaults (object now or they stick)

- Project dir: `flink/normalizer/`; base package `io.tibobit.normalizer`
- Module names: `common`, `job-pair-extractor`, `job-type-validator`, `job-rebaser`,
  `job-precision-normalizer`, `job-book-builder`, `job-level-emitter`
- Dead-letter topic: `ex{id}-p{id}-rejected-flink`
- Deploy: ONE parameterized `run-job.sh` + `Dockerfile` at the pipeline root taking the module
  name (all jobs share the same Flink base image), NOT per-module copies
- Intermediate event schema/subject: `raw_order_book_event.avsc` / `raw-order-book-event`
  (jobs 1–4), `order_book_snapshot.avsc` / `order-book-snapshot` (job 5),
  `rejected_order_book_event.avsc` / `rejected-order-book-event` (dead-letter)

---

## Milestone 0 — Contracts & prerequisites (blocks everything)

- [ ] Collect sample raw payloads per exchange — **RESET 2026-07-14**: the 2026-07-13 bulk
      capture was discarded (recoverable from git); rebuilding into `sample-raw-data.md`
      **one exchange at a time**, each verified before moving on:
      - [x] ex1 nobitex — captured 2026-07-14 (snapshot regime + Centrifugo envelope
            re-confirmed, channel format differs from ex2; `pub.offset` = out-of-order
            check field (REVISED 2026-07-14) + records always single-doc — see
            sample-raw-data.md)
      - [x] ex2 bitpin — captured 2026-07-14 (snapshot regime + Centrifugo envelope
            re-confirmed; `pub.offset` = out-of-order check field (REVISED 2026-07-14) —
            see sample-raw-data.md)
      - [x] ex3 wallex — captured 2026-07-14 (per-side snapshot regime + numeric JSON
            prices re-confirmed; NOT Centrifugo — `["{market}@{side}", [levels]]` array;
            no seq/timestamp at all ⚠ ONLY exchange with no out-of-order protection —
            see sample-raw-data.md)
      - [x] ex4 ramzinex — captured 2026-07-14 (full-snapshot regime + numeric JSON prices
            re-confirmed; Centrifugo but channel key is a NUMERIC market id `orderbook:12`;
            7-element level arrays, `sells` sorted descending (best ask LAST); no seq —
            `pub.offset` = out-of-order check field (REVISED 2026-07-14) — see
            sample-raw-data.md)
      - [x] ex5 bitget — captured 2026-07-14 (snapshot-only re-confirmed, and explicit on
            the wire: `action: "snapshot"` — first exchange with a discriminator; NOT
            Centrifugo — `action`/`arg`/`data` shape, `data` is an ARRAY; string levels;
            **`seq` = out-of-order check field (REVISED 2026-07-14)** — no gap/jump rule,
            just drop stale snapshots; `pseq` metadata — see sample-raw-data.md)
      - [x] ex6 bybit — captured 2026-07-14 (snapshot + delta samples; regime re-confirmed:
            snapshot/delta via `type` discriminator; NOT Centrifugo — `topic`/`ts`/`type`/
            `data`/`cts` shape; string levels; **sequence id = `u`, jump = 1**
            (user-confirmed, re-confirmed 2026-07-14: "bybit u gap is 1") — `data.seq` is
            NOT contiguous, ignore for gaps; qty-"0" delete frame still to capture — see
            sample-raw-data.md)
      - [~] ex7 ompfinex — POSTPONED 2026-07-14 (team decision — known issue with its raw
            data). Revisit when the raw feed is fixed.
      - [x] ex8 okx — captured 2026-07-14 (snapshot + update samples — topic-existence caveat
            settled, feed is live; regime: snapshot/update via `action` discriminator; bitget-
            family envelope (`arg`/`action`/`data`-array) but grouped book `books-grouped` +
            `grouping`, market key `arg.instId` = `BTC-USDT` with a DASH; string levels;
            **sequence id = `ts` (string epoch-millis), jump = 300** (user-confirmed);
            **qty-"0" delete CONFIRMED on wire** — first delete frame in the set — see
            sample-raw-data.md).
            Scope = **ex1–ex6 + ex8** (ex7 postponed).
      - [x] re-verify while rebuilding — DONE 2026-07-14: three regimes ✅; Centrifugo
            envelope on ex1/2/4 ✅; wallex/ramzinex numeric prices ✅; bitget snapshot-only ✅
            (`seq` = out-of-order field, no gap rule); bybit `u` jump=1 ✅ (re-confirmed);
            ex1 multi-doc records CLOSED (user: always ONE doc, no splitting); bybit `u`
            gaps CLOSED (user: skip NiFi investigation — job 2's drop+await-snapshot gap
            rule absorbs the upstream loss).
            **Job-2 validation scope (REVISED 2026-07-14, user): gap/jump rules ONLY for
            the delta feeds ex6 (`u`/1) + ex8 (`ts`/300); snapshot feeds get an
            OUT-OF-ORDER check instead — drop any snapshot whose ordering field is not
            greater than the last seen (ex1/2/4 `pub.offset`, ex5 `seq`; ex3 has no field
            → no protection possible).**
- [x] **Per-step latency timings (REQUIREMENT 2026-07-15, `memory/project_raw_pipeline_decision.md`)** —
      DONE 2026-07-15. One `pipeline_timings` field per schema, wire type `["null", PipelineTimings]`
      `default: null` (nullable union chosen over non-null-record for one-token backward-compat
      default). `PipelineTimings` = 12 nullable `timestamp-millis` fields:
      `{pair_extract,type_validate,rebase,precision,book_build,level_emit}_{in,out}`, duplicated
      field-for-field (same rule as `PriceLevel`/`Type`):
  - [x] `raw_order_book_event.avsc` (jobs 1–4) + example JSON
  - [x] `order_book_snapshot.avsc` (job 5) + example
  - [x] `rejected_order_book_event.avsc` inlined event — field-for-field identical
  - [x] `price_level_event.avsc` (job 6, frozen consolidator input) — nullable/defaulted,
        backward-compatible so consolidator/`web/` decode unchanged (⚠ NOT re-verified against a
        live registry's compat check yet — do at M8 provisioning)
  - [x] common Java models (`PipelineTimings` POJO + field on Raw/OrderBookSnapshot) + shared
        `PipelineTimingsRecords` serde helper; job 1 stamps `pair_extract_in/out`
        (`_in` at `flatMap` entry, `_out` before `collect`). Jobs 2–6 inherit the field, stamp
        their own two when built. 47 tests green (5 new).
  - [x] warmup.sh needs NO change — `register_schema` reads the `.avsc` files directly, so the
        edits register as new subject versions on next warmup run
- [ ] Coordinate the NiFi contract: verbatim payload bytes, topic `ex{id}-raw` per exchange.
      → verify: written agreement in `memory/project_raw_pipeline_decision.md`
      (topic creation + retention SETTLED 2026-07-14: `warmup.sh` creates `ex{id}-raw` per
      subscribed exchange, retention 7 days for now)
- [x] Design `schemas/raw_order_book_event.avsc` + example JSON — DONE 2026-07-14. Fields as
      proposed, with three refinements driven by the captured wire formats (rationale in
      `memory/project_avro_schema.md`): `asks`/`bids` are NULLABLE (null = side absent — ex3
      per-side snapshots; empty array = exchange reported the side empty); `sequence_id` is
      NULLABLE (null = feed has no ordering field — ex3); `sequence_jump` 0 = snapshot feed
      (out-of-order check only), >0 = delta-feed gap rule (ex6=1, ex8=300)
- [x] `schemas/order_book_snapshot.avsc` + example — DONE 2026-07-14 (`exchange_id`, `pair_id`,
      `event_time`, `last_sequence_id` nullable, `asks[]`/`bids[]` required — full book)
- [x] `schemas/rejected_order_book_event.avsc` + example — DONE 2026-07-14 (inlined
      `RawOrderBookEvent` under `event` + `reject_reason:string` + `rejected_at`)
- [x] Register the 3 new subjects in `scripts/warmup.sh` — DONE 2026-07-14: subjects
      `raw-order-book-event` / `order-book-snapshot` / `rejected-order-book-event` (canonical
      fixed names, NO per-topic subjects); verified against the local registry (ids 4/5/6)

## Milestone 1 — Scaffold `flink/normalizer/`

- [x] Parent `pom.xml` (packaging `pom`, modules list, shared dependencyManagement: Flink,
      kafka connector, avro + confluent registry deps, JUnit5/AssertJ/flink-test-utils/JaCoCo —
      versions copied from `flink/orderbook-consolidator/pom.xml`) → verify: `mvn validate`
      DONE 2026-07-15: `io.tibobit:normalizer-parent`; modules list starts with `common` only
      (user decision — each job-* module is added in its own milestone M2–M7)
- [x] `common/` module (plain jar, no shade): DONE 2026-07-15, artifactId `normalizer-common`,
      package `io.tibobit.normalizer.*`, 18 tests green
      - [x] Models for the shared event/book/rejection shapes (plain POJOs, no Jackson —
            Avro GenericRecord mapping happens in serde classes, consolidator pattern)
      - [x] `AvroSchemaLoader.loadLatest(url, subject)` — registry-only at runtime (port from
            consolidator; schemas NEVER bundled in shaded jars)
      - [x] Avro serde pairs per shared shape (`toGenericRecord`/`fromGenericRecord` as pure
            package-private statics — the consolidator's testable-mapping pattern).
            NOTE: rejected-event shape got a SERIALIZER only — nothing in the pipeline consumes
            dead-letter topics (kafka-ui reads them); add a deserializer if a consumer appears
      - [x] `RefreshingLookup` — periodic-refresh Postgres reference reader: loads a `Map` via
            JDBC in `open()`, re-loads every `REFRESH_INTERVAL` on a schedule; on refresh
            failure keep last-good snapshot + log. TDD with a fake loader fn.
            NOTE: generic `Loader<K,V>` fn — the actual JDBC query closure is built in M2 where
            the exchange_markets query lives
      - [x] BigDecimal helpers: canonicalize (`stripTrailingZeros().toPlainString()`), rebase
            (`scaleByPowerOfTen`), truncate (`setScale(p, DOWN)`) — pure, test-first
      → verify: `mvn -pl common -am test` green ✓ (18/18)
- [x] One parameterized `run-job.sh` + `Dockerfile` + `Makefile` at `flink/normalizer/` root
      (module name as arg; derive jar + main class per module) → verify: script builds a chosen
      module and prints the submit command against a local cluster
      DONE 2026-07-15: main class comes from the jar manifest (each job module's shade config),
      jar located by glob excluding shade's `original-*`; Dockerfile identical to the
      consolidator's (one cluster image hosts all jobs); full submit path verified in M2 when
      the first job module exists
- [x] `docker-compose-normalizer.yml` at repo root (Flink cluster + kafka + schema-registry +
      postgres + kafka-ui, mirroring the consolidator compose incl. `restart: on-failure`,
      log-dir volumes, named volumes) → verify: `docker compose config` passes ✓
      DONE 2026-07-15 (user decision): it is the FULL replacement stack — identical to the
      consolidator compose (same container names/ports/volumes, only one compose runs at a
      time) except Flink image builds from ./flink/normalizer and taskmanager has 8 slots so
      the ONE cluster hosts consolidator + all 6 normalizer jobs

## Milestone 2 — Job 1: pair extractor (`ex{id}-raw` → `ex{id}-p{id}-raw-flink`)

TDD throughout (`memory/project_tdd_workflow.md`): tests first, fixtures from Milestone 0.

- [x] `RawExchangeParser` interface: `byte[] payload → List<RawOrderBookEvent>` (pair still as
      the exchange's market string at this point) + one implementation per exchange, selected
      by `exchange_id` parsed from the source topic name. Test-first against the real fixtures.
      Scope: ex1–ex6 + ex8 parsers (ex7/ompfinex postponed — see M0)
      DONE 2026-07-15: returns `List<ParsedBookEvent>` (market string + event; exchange_id AND
      pair_id both stamped later by PairExtractFunction). Whitelist rule = empty list for
      unrecognized frames, throw for malformed (caller drops both). event_time per exchange:
      ex1 `lastUpdate`, ex2 `event_time` (ISO), ex5 inner `ts`, ex6 `cts`, ex8 `ts`; ex3+ex4
      have NO message-level timestamp → job-1 processing time (flagged in memory)
- [x] Market-string → `pair_id` resolution via `RefreshingLookup` over
      `exchange_markets(exchange_id, market) → market_id`; unknown market string → log + drop
      (+ counter) — NOT dead-letter (dead-letter is job-2's validation concern)
      DONE 2026-07-15: `ExchangeMarketsLoader` (plain JDBC, key `"{exchange_id}|{market}"`);
      drops counted via Flink metrics (dropped-no-parser/-unparseable/-unknown-market)
- [x] Source: `KafkaSource<byte[]>` pattern `^ex[0-9]+-raw$`, earliest-or-latest decision
      (propose `latest`, consistent with consolidator), Kafka metadata needed: topic name (for
      exchange_id) — use a `KafkaRecordDeserializationSchema` that captures topic.
      NOTE: pattern also matches the postponed `ex7-raw` — decide at implementation whether to
      exclude it from the pattern or drop-with-counter on missing parser
      DONE 2026-07-15: `latest` offsets; ex7 DECIDED = drop-with-counter on missing parser
      (scope lives in one place: `Parsers.byExchangeId()`)
- [x] Sink: `KafkaSink` with topic selector `ex{exchange_id}-p{pair_id}-raw-flink`, Avro via
      registry subject `raw-order-book-event` ✓
- [x] `PairExtractorJob.main`: env config (`KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`,
      `POSTGRES_*`, `KAFKA_GROUP_ID=normalizer-pair-extractor`, `REFRESH_INTERVAL`); anonymous
      `KeySelector` classes if keying is needed (Flink lambda inference gotcha)
      → verify: `mvn -pl job-pair-extractor -am test` green; live smoke: publish a fixture
      payload to `ex1-raw`, see the structured event on `ex1-p{id}-raw-flink` via kafka-ui
      DONE 2026-07-15: stateless — no keying needed. 24/24 tests green; live smoke PASSED for
      ex1 (snapshot → `ex1-p1-raw-flink`, exact seq/event_time/level strings) AND ex8 (update
      → jump 300, qty-"0" delete preserved); postgres driver confirmed shaded into the job jar
      (NOT in the Flink image); run-job.sh full submit path verified (M1 leftover)
- [x] Repeatable e2e smoke test — `flink/normalizer/smoke-pair-extractor.sh` (2026-07-15):
      produces each fixture verbatim to `ex{id}-raw`, reads back the Confluent-Avro event on
      `ex{id}-p1-raw-flink`, asserts exchange_id/pair_id/type/sequence_id/side-shape. Covers all
      10 fixtures (ex1–6+8 incl. ex3 per-side split + ex6/ex8 update). Deterministic: snapshots
      the output end-offset before producing then reads from it (no consumer-group latest-reset
      race). Needs stack up + warmed DB + job submitted. → verify: `./smoke-pair-extractor.sh`
      prints "10 passed, 0 failed" (green 2026-07-15).
- [x] Fix 2 runtime blockers surfaced running the timings-enabled build (2026-07-15):
      (1) `ExchangeMarketsLoader` now `Class.forName`s the postgres driver — DriverManager's lazy
      ServiceLoader doesn't see the driver in Flink's child-first classloader (intermittent "No
      suitable driver found", job dies INITIALIZING). (2) Re-registered `raw-order-book-event` to
      v2 with `pipeline_timings` (registry was stale vs schema+code → Avro sink NPE on first emit).
      → verify: smoke green 10/10 with NO "(no pipeline_timings on this build)" note.

## Milestone 3 — Job 2: type validator (→ `ex{id}-p{id}-type-validated-raw-flink` + dead-letter)

DONE 2026-07-15 (`job-type-validator`, package `io.tibobit.normalizer.typevalidate`). See
`memory/project_type_validator.md`. 11 unit tests + live smoke 8/8 green; job RUNNING.

- [x] `TypeValidateFunction` keyed `(exchange_id, pair_id)` `KeyedProcessFunction`, `ValueState
      {lastSeq, awaitingSnapshot}`. Rules RECONCILED with the revised job-2 scope in
      `project_raw_pipeline_decision.md` (the discriminator is `type` + `sequence_id==null`, NOT
      jump alone — delta-feed SNAPSHOT messages also carry jump>0, verified in the ex6/ex8 parsers):
      `sequence_id==null` (ex3) → pass through unchecked; `snapshot` → out-of-order check only
      (reject `stale_or_duplicate` if `seq <= lastSeq`, else re-sync: store seq, clear awaiting);
      `update` → `no_baseline` if lastSeq null / `awaiting_snapshot` if still waiting / valid iff
      `seq == lastSeq + sequence_jump` / `stale_or_duplicate` if `seq <= lastSeq` / else `sequence_gap`
      + set awaitingSnapshot. Valid → main output; rejects → `REJECTED` side output. Stamps
      `type_validate_in/out` (out only on emitted). 11 harness tests cover every branch + keying
      isolation + timings.
- [x] Wiring: `TypeValidatorJob` source (pattern `ex[0-9]+-p[0-9]+-raw-flink`,
      `RawOrderBookEventDeserializer`, latest) → keyBy → `.process` → two sinks (validated topic
      selector `-type-validated-raw-flink` reusing `raw-order-book-event` / dead-letter
      `ex{id}-p{id}-rejected-flink` with `RejectedOrderBookEventSerializer`). No DB, no jackson.
- [x] Repeatable e2e smoke `flink/normalizer/smoke-type-validator.sh` — REWRITTEN 2026-07-15 to the
      RAW-IN whole-chain model (normalizer smoke rule): produces raw OKX payloads to `ex8-raw`, lets
      job1→job2 run, captures `ex8-p1-type-validated-raw-flink`/`-rejected-flink`. Uses ex8 because
      its `ts` sets BOTH event_time and sequence_id (jump 300); event_time = now (execution time),
      key (8,1), monotonic ts for idempotence against the STATEFUL job; precondition checks BOTH jobs
      RUNNING. 8 cases run in order as one full delta lifecycle: baseline → two contiguous updates →
      gap (`sequence_gap`) → held update (`awaiting_snapshot`) → snapshot re-sync →
      contiguous-after-resync → stale snapshot (`stale_or_duplicate`). Asserts event_time == sent ts
      AND the real timing chain `event_time ≤ pair_extract_in ≤ pair_extract_out ≤ type_validate_in ≤
      type_validate_out` (no sentinels — both stages stamp live), plus upstream preservation on reject
      (`.event.…`, type_validate_out null). → verify: "8 passed, 0 failed" (green 2026-07-15; two-hop
      chain, cases may transiently time out on read — re-run).
- [x] Runtime blocker fixed (same class as job 1): re-registered `rejected-order-book-event` to v2
      (was stale v1, no `pipeline_timings` on the inlined event → Avro NPE on the reject sink).

## Milestone 4 — Job 3: rebaser (→ `ex{id}-p{id}-rebased-flink`)

- [x] Stateless `ProcessFunction` (NOT `RichMapFunction` — the flag below resolved to a side
      output): every level's `price × 10^price_amount_rebase`, `quantity × 10^volume_amount_rebase`
      via `scaleByPowerOfTen` (exact); exponents per `(exchange_id, pair_id)` from
      `RefreshingLookup` over `exchange_markets` (`RebaseFactorsLoader`, keyed by `market_id`
      not the market string). **FLAG RESOLVED 2026-07-18 (user): missing row → DEAD-LETTER**
      `no_rebase_row` to `ex{id}-p{id}-rejected-flink` (job 2's topic/schema reused) — not
      drop, not passthrough; passthrough would emit silently corrupt prices downstream.
      Null side stays null (ex3 half-book), empty stays empty. 9 tests green: rebase 0 identity,
      +/- exponents, exactness on a 21-digit value, per-(ex,pair) lookup, both sides/all levels,
      null-vs-empty, dead-letter, timings.
- [x] Wiring: source `^ex[0-9]+-p[0-9]+-type-validated-raw-flink$` → process → sink
      `ex{id}-p{id}-rebased-flink` + reject sink; unkeyed (rebase never depends on another event)
      → verify: 9 module tests green; live smoke `smoke-rebaser.sh` 3/3 green 2026-07-18 with a
      nonzero rebase row (price +2 / volume −3) — raw-in whole-chain per the smoke rule

## Milestone 5 — Job 4: precision normalizer (→ `ex{id}-p{id}-applied-precision-flink`)

- [x] Stateless `RichMapFunction` (as sketched — unlike job 3, no side output is needed):
      `price.setScale(markets.price_precision, DOWN)`,
      `quantity.setScale(markets.quantity_precision, DOWN)`; precisions per `pair_id` from
      `RefreshingLookup` over `markets`; NULL precision column → leave value untouched, and a
      pair with NO markets row is the same passthrough (an un-truncated amount is merely too
      precise, not corrupt — deliberately unlike job 3's dead-letter).
      **DESIGN FLAG RESOLVED 2026-07-18 (user decision, revised same day): nonzero quantity
      truncating to exactly 0 → EMIT `"0"` and KEEP the level.** Job 5 reads that as a
      level-delete, which is intended — a size below the market's `quantity_precision` is not
      representable liquidity. (The earlier "drop the level" answer was reversed; flooring stays
      rejected, it would report more size than the exchange did.) Level count in == level count
      out, which is what keeps this a plain `RichMapFunction`.
      15 module tests green: truncation never rounds up, already-fewer-decimals unchanged,
      precision 0, exactness on 20+ digit values, separate price/qty precisions, per-pair
      lookup, null-precision & unknown-pair passthrough, zero-quantity passthrough, dust → "0"
      (+ an all-dust side keeps every level), null-vs-empty side, both sides all levels,
      timings
- [x] Wiring: source `^ex[0-9]+-p[0-9]+-rebased-flink$` → map → sink
      → verified: module tests 15/15 green + live smoke `smoke-precision.sh` 3/3 green
      2026-07-18 (raw-in whole chain job1→job4; no DB mutation needed — the seeded markets row
      is already non-identity at 2/8, and the seeded rebase row is 0/0 so job 3 stays a no-op)

## Milestone 6 — Job 5: book builder (→ `ex{id}-p{id}-orderbook-snapshot-flink`)

- [x] `BookBuildFunction` keyed `(exchange_id, pair_id)` `KeyedProcessFunction`, `MapState` per
      side keyed by canonicalized price (`stripTrailingZeros().toPlainString()` — MapState is
      hash-based, won't collapse scales, consolidator lesson): snapshot → replace book
      wholesale; update → `quantity > 0` upsert / `== 0` delete (BigDecimal `signum()`);
      emit the FULL book (both sides + `last_sequence_id` + `event_time`) on every change.
      Sequence rules are NOT re-checked (job 2 already validated; topics are single-partition
      so order holds).
      **Snapshot and update share ONE per-level rule — they differ only by "clear the side
      first".** So `quantity == 0` means "no level here" in a snapshot too, which closes the M5
      follow-on hazard (a dust level inside a snapshot can never rest in the book). Those
      deletes originate in job 4's truncation, not only from exchanges — commented at the
      delete branch so nobody debugs a delete that isn't in the raw feed.
      Sides are sorted on emit (asks↑/bids↓) because MapState iteration order is undefined.
      15 module tests green: snapshot replaces / empty side clears / ex3 null side merges /
      upsert / qty-0 delete / delete of unknown price / qty-0 inside a snapshot / canonical
      price identity / sorting / identity+sequence / null sequence / both sides always present /
      emit-on-every-event / timings / per-key isolation
- [x] **⚠ Wallex (ex3) per-side snapshot merge — DONE.** ex3 sends full snapshots ONE
      side per message (`asks=null` xor `bids=null`, no seq/ts). "Replace book wholesale" must
      mean **replace only the non-null side(s), keep the other side's state** — never clear a
      side just because its array is null. Distinguish `null` side ("not in this event, leave
      it") from an empty array (`[]` = "this side reported empty, clear it"). This is THE place
      wallex's two messages combine into one two-sided book (decided against job 1 — [[pair-extractor]]).
      Test `nullSideKeepsOtherSideState`: one-sided snapshot then the other ⇒ BOTH sides populated.
      (Unreachable from the smoke — OKX always sends both sides.)
- [x] Wiring: source `^ex[0-9]+-p[0-9]+-applied-precision-flink$` → keyBy → builder → sink
      (`order-book-snapshot` serde)
      → verified: module tests 15/15 green + live smoke `smoke-book-builder.sh` 3/3 green
      2026-07-18 (raw-in whole chain job1→job5; three ORDERED cases on one accumulating book:
      snapshot seeds → update merges → qty 0 deletes). Gotcha: the registered
      `order-book-snapshot` subject was stale v1 without `pipeline_timings` and had to be
      re-registered — same trap as jobs 1 and 2
- [x] NOTE cold-start limitation (no checkpointing configured — same known gap as the old
      merger): book is empty after restart until the next snapshot; recorded, not solved
      (see the checkpointing open item below)

## Milestone 7 — Job 6: level emitter (→ existing `ex{id}-p{id}-{side}`, `price_level_event.avsc`)

- [x] `LevelDiffFunction` keyed `(exchange_id, pair_id)`, state = last emitted book per side (one
      `MapState` per side, canonicalized price → canonicalized quantity): changed/added price →
      `price_level_event` upsert; vanished price → `quantity="0"`; unchanged book → emit nothing.
      Diff approach CONFIRMED at implementation start by reading the consolidator's
      `PerExchangeBookBuilder` (`signum()==0` → `levels.remove(price)`, unknown-price delete is a
      no-op). 13 tests: first-book-emits-all, unchanged-emits-nothing, added, changed-quantity,
      vanished→0, emptied-side, sides-diffed-independently, not-re-deleted, re-added, identity+
      event_time, canonicalization, timings, per-key isolation
- [x] Sink: per-record topic selector `ex{exchange_id}-p{pair_id}-{side}` on the EXISTING
      `price-level-event` subject; new `PriceLevelEvent` model + serializer in `common` (side is an
      Avro ENUM, not a string). Registry gotcha: the subject was stale v1 without
      `pipeline_timings` — compatibility-checked FIRST (it is frozen and live), then registered
      v2/id 10
      → verified: 13/13 module tests + live smoke `smoke-level-emitter.sh` 3/3 green 2026-07-18
      (assertions are per-level, not per-count, with a clock-derived price band — a diff job's
      output depends on its own previous runs)
- [ ] AFTER M7 (user, 2026-07-18): **compare job 5's book against the book the consolidator
      builds from job 6's levels** for the same `(exchange_id, pair_id)`. They are built two
      different ways — job 5 accumulates from raw events, the consolidator re-accumulates from
      our emitted diffs — so any divergence means job 6's diff lost or invented a level. This is
      the real end-to-end correctness check for M7; not blocking the milestone, do it once the
      chain is live.
- [ ] Still open from M7's original verify line: watch a consolidated book in the `web/` UI end to
      end with the consolidator running unchanged (the smoke asserts the topic contract, not the
      UI)

## Milestone 8 — Infra, provisioning, cutover

- [x] Extend `scripts/warmup.sh` — DONE 2026-07-19: `ex{id}-raw` (2026-07-14) plus the per-pair
      normalizer families `{raw,type-validated-raw,rebased,applied-precision,orderbook-snapshot}
      -flink` at 1h and the shared `rejected-flink` dead-letter at 7d. Created BEFORE the existing
      input/output blocks (sources read `latest` — a late-discovered topic loses what was produced
      in between). Sequential creation kept (parallel xargs was reverted before, don't retry)
- [ ] kafka-ui serde config in `docker-compose-normalizer.yml`: `topicValuesPattern` +
      `schemaNameTemplate` per new topic family → the 3 new canonical subjects (pattern from
      `memory/project_kafka_topic_strategy.md`; registry stays clutter-free)
- [ ] `fake-data-generator/`: new mode emitting realistic RAW exchange payloads to `ex{id}-raw`
      (stand-in for NiFi during dev)
- [x] Root `Makefile`: `refresh-normalizer` target — DONE 2026-07-19 (submits DOWNSTREAM-FIRST:
      consolidator, then jobs 6→1, because every source reads `latest`); `README.md` section for
      the pipeline still TODO
- [ ] Server deploy: build + submit all 6 jobs (`sudo`, Temurin 21 —
      `memory/project_ubuntu_server_env.md`); NOTE all composes share container names/ports —
      revisit "one stack at a time" rule, the normalizer must run ALONGSIDE the consolidator
- [ ] Cutover plan: run new pipeline in parallel with NiFi's current normalized output, compare
      `ex{id}-p{id}-{side}` streams for equality window, then switch NiFi to raw-only; decide
      fate of `flink/orderbook-job/` afterwards

## Open items (decide at the flagged milestone)

- [ ] Job-1 source offsets: `latest` vs `earliest` for `ex{id}-raw`
- [x] ~~Job-3 missing-rebase-row behavior~~ → RESOLVED 2026-07-18: dead-letter `no_rebase_row`
- [x] ~~Job-4 truncate-to-zero hazard~~ → RESOLVED 2026-07-18: emit `"0"` and keep the level, so
      job 5 deletes it (not floor; an earlier "drop the level" answer was reversed same day)
- [ ] Refresh interval default for `RefreshingLookup`
- [ ] Retention values per new topic family
- [ ] Checkpointing/state backend for jobs 2/5/6 (stateful; currently the whole platform runs
      without checkpoints — bigger conversation, not pipeline-specific)
