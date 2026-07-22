# Todo — Raw Data Normalization Pipeline

NiFi stops normalizing and instead publishes VERBATIM raw exchange payloads to `ex{id}-raw`
(one topic per exchange); a chain of 6 Flink jobs reproduces the normalization, ending in the
existing `ex{exchange_id}-p{pair_id}-{side}` / `price_level_event.avsc` consolidator input
stream. Full decision + rationale: `memory/project_raw_pipeline_decision.md`
(accuracy > latency → one job per step, every intermediate topic is an audit point).

(History note: todo.md was cleaned 2026-07-13 — Phases 1–5 removed, recoverable from git;
the R3-postponed block lives on in `memory/deprecated/project_orderbook_consolidator_decision.md`.)

(Deprecation note, 2026-07-21: the consolidator is retired ahead of the aggregator work — its
code is now `flink/DEPRECATED-orderbook-consolidator/` and its memory moved to
`memory/deprecated/`. Pure rename only, mirroring commit 41bdd20. ⚠ DANGLING PATH REFS still
point at the old `flink/orderbook-consolidator/` path and must be cleaned when plan Part D
un-deploys it: `docker-compose-normalizer.yml`, `docker-compose-orderbook-consolidator.yml`,
`Makefile` (`refresh-consolidator`), `README.md`, `scripts/install-deps.sh`,
`flink/normalizer/manual-test-data/reset.sh`. Full plan: `plans/aggregator-gap-drop.md`.)

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
      rejected, it would report more size than the exchange did.)
      **BUG FIXED 2026-07-20 (user decision): prices that COLLIDE after truncation are MERGED
      into one level whose quantity is the SUM.** At `price_precision 2`, 1.234 and 1.235 both
      become 1.23; the job used to emit both, and job 5's price-keyed `MapState` then kept only
      the last, silently losing the other's liquidity. Raw quantities are summed and the sum
      truncated ONCE (not truncate-then-sum — so colliding dusts can add up to a level that
      survives), first-appearance order preserved, same rule on `snapshot` and `update` (on an
      update, summing collided replacements is a deliberate approximation: the job is stateless
      and cannot know what the other collided price still rests at). This RETIRES the old
      "level count in == level count out" invariant — a side can now shrink.
      21 module tests green: truncation never rounds up, already-fewer-decimals unchanged,
      precision 0, exactness on 20+ digit values, separate price/qty precisions, per-pair
      lookup, null-precision & unknown-pair passthrough, zero-quantity passthrough, dust → "0"
      (+ an all-dust side keeps every level), null-vs-empty side, both sides all levels,
      timings, and (2026-07-20) collision merge: sums quantities, keeps first-appearance order,
      sums raw-before-truncating, all-dust collision → one delete, both sides, and
      null-price-precision merging only exact wire duplicates
- [x] Wiring: source `^ex[0-9]+-p[0-9]+-rebased-flink$` → map → sink
      → verified: module tests 21/21 green + live smoke `smoke-precision.sh` 3/3 green
      2026-07-18 (raw-in whole chain job1→job4; no DB mutation needed — the seeded markets row
      is already non-identity at 2/8, and the seeded rebase row is 0/0 so job 3 stays a no-op)
- [ ] Re-run `smoke-precision.sh` live after the 2026-07-20 collision-merge fix — the script now
      sends 3 asks and asserts 2 come out, but has only been syntax-checked, not run.
      The job jar also needs resubmitting for the fix to take effect on a running stack.

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
- [x] Manual test data — DONE 2026-07-20: `flink/normalizer/manual-test-data/` — BTC-USDT/pair-1
      ONLY and every scenario STANDALONE (both user constraints); 7 scenarios (01–06 ex8/okx +
      07 ex3/wallex, 29 payloads), `produce.sh` + `reset.sh`, README with the expected outcome and
      dead-letter count per scenario. Independence = self-contained data + `reset.sh` recycling the
      4 stateful jobs downstream-first. Expectations derived from source, NOT yet run live.
      Details: `memory/project_manual_test_data.md`
- [ ] First live run of `manual-test-data/`: verify `reset.sh`'s Flink REST flow (jar lookup by
      artifactId prefix → resubmit → RUNNING wait) and confirm each scenario's dead-letter count
- [ ] Verify job 1's behaviour on a NON-JSON raw frame (bare `pong`): drop or throw? Then add the
      case to manual-test-data S6 (deliberately left out — unverified, might wedge the job)
- [x] Root `Makefile`: `refresh-normalizer` target — DONE 2026-07-19 (submits DOWNSTREAM-FIRST:
      consolidator, then jobs 6→1, because every source reads `latest`); `README.md` section for
      the pipeline still TODO
- [ ] Server deploy: build + submit all 6 jobs (`sudo`, Temurin 21 —
      `memory/project_ubuntu_server_env.md`); NOTE all composes share container names/ports —
      revisit "one stack at a time" rule, the normalizer must run ALONGSIDE the consolidator
- [ ] Cutover plan: run new pipeline in parallel with NiFi's current normalized output, compare
      `ex{id}-p{id}-{side}` streams for equality window, then switch NiFi to raw-only; decide
      fate of `flink/orderbook-job/` afterwards

## Milestone 9 — ex9 lbank (NEW EXCHANGE, normalizer side not implemented)

Teammate commit `195a735` ("add lbank to postgres sqls", 2026-07-20) added exchange id **9 lbank**
to `postgres/02_seed.sql` (+ 27 `exchange_markets` rows, all rebase `0,0`, status `unsubscribe`)
and to `markets/markets.csv` (all `disable`). **That is the ONLY place lbank exists** — a repo-wide
grep for `lbank` hits those two files and nothing else. No parser, no fixtures, no wire samples, no
normalizer support. Job 1 will silently drop every ex9 message via the "no parser" counter until
the work below is done.

Wire symbols are lowercase-underscore (`btc_usdt`), unlike okx's `BTC-USDT`.

**Already covered, do NOT redo:**

- Topic patterns in all 6 jobs are regexes (`ex[0-9]+-...`), so ex9 is picked up with no code change.
- `scripts/warmup.sh` has no status filter on its `exchange_markets` query — it will create the ex9
  raw + stage + dead-letter topics on the next run. Re-running it IS the provisioning step.
- lbank's markets map onto the SAME `market_id`s as okx (1, 3, 5…), so `markets` precision rows and
  job 3's rebase rows already resolve. Jobs 3–6 need no lbank-specific work.

**Blocking, in order:**

- [ ] **Capture real lbank wire samples → `sample-raw-data.md` § ex9** (same reset-then-capture
      method as the 2026-07-14 pass). This BLOCKS the parser — every other parser's shape was read
      off the wire, never guessed, and the ordering/delete semantics below cannot be inferred from
      lbank's docs alone.
- [ ] Classify the feed regime from those samples — it decides job 2's behaviour, which is the one
      thing that differs per exchange (`memory/project_raw_pipeline_decision.md`):
      - snapshot-per-message (like ex1/2/4/5) → out-of-order drop check only; or
      - true delta (like ex6/ex8) → needs the ordering field + its jump, and `type` snapshot/update
      - is there an ordering field at all, or is it ex3/wallex-style with none (no protection)?
      - is `quantity 0` a level delete on the wire? (confirm on the wire, don't assume)
      - one book per message or a `data` array of several?
      - what stamps `event_time` — a wire timestamp or processing time?
- [ ] `LbankParser` + register `9, new LbankParser()` in `Parsers.byExchangeId()`; javadoc in the
      per-exchange format the other 7 parsers use (market key, level shape, seq field + jump,
      event time, § reference). Emit the market string EXACTLY as lbank sends it — the lookup key
      is `"{exchange_id}|{market}"`, an exact case-sensitive match against the seeded `btc_usdt`,
      so an uppercase wire symbol silently drops every message. Verify the case on the samples.
- [ ] `LbankParserTest` + fixtures under `job-pair-extractor/src/test/resources/fixtures/`,
      matching the existing per-parser test shape; extend `smoke-pair-extractor.sh`'s fixture set
- [ ] Update `Parsers` class javadoc (it currently states the in-scope set as ex1–6 + ex8) and
      `memory/project_pair_extractor.md` ("7 parsers (ex1–6+8)")
- [ ] Coordinate the collection side: NiFi must actually publish lbank to `ex9-raw`, and the seeded
      rows are `unsubscribe`/`disable` — nothing flows until someone flips them. Ask the team who
      owns that before assuming it is done.

## Milestone 10 — ex1 nobitex snapshot/update split (2026-07-21)

The "ex1 is a full snapshot on every WS message" assumption was WRONG — nobitex snapshots come
over REST, WS carries only deltas. NiFi now publishes two payloads to `ex1-raw`.

- [x] `NobitexParser`: branch on top-level `action=="snapshot"` (REST, market from injected `pair`
      field, `type=snapshot`/null seq) vs Centrifugo push (WS, `type=update`/`seq=pub.offset`/
      `jump=1`); noise still dropped. Fixtures: `ex1-snapshot.json` = REST payload, old WS message
      → `ex1-update.json`. 4 parser tests green.
- [x] `TypeValidateFunction`: null-seq snapshot is a resync → new `baselinePending` state, first
      update after it adopts its offset as baseline. Exchange-agnostic (ex3 harmless, ex6/ex8
      unchanged). 3 new harness tests green.
- [x] `TypeValidateFunction` (2026-07-21, follow-up): null-seq snapshots had NO ordering check, so
      an OLD ex1 REST snapshot replayed after newer WS deltas overwrote the newer book AND re-armed
      the resync. Fix: track `lastEventTime` state; a null-seq event with `event_time < lastEventTime`
      is rejected `out_of_order` (new reason) BEFORE it re-arms baseline. There is no sequence on
      these frames, so event time is the only ordering signal. Exchange-agnostic (also guards ex3).
      16 harness tests green (2 new). **Not run live.**
- [x] Updated `smoke-pair-extractor.sh` (ex1 snapshot=null-seq + new ex1-update case) and
      `sample-raw-data.md § ex1`.
- [x] Manual-test scenarios 08–12 (`manual-test-data/`, 2026-07-21): 08 rest→ws resync + re-anchor
      (0 DL), 09 update-before-snapshot → `no_baseline` (1), 10 offset gap → `awaiting_snapshot` →
      REST resync (2), 11 Centrifugo noise drops (0), 12 stale REST replay after WS deltas →
      `out_of_order` (1, matches the event-time guard fix). `produce.sh` extended to shift ex1
      `lastUpdate` (offsets left literal). README documented. **Unit/dry-run verified only — needs a
      live run.**
- [ ] **Deploy is coupled — cut over together**: parser + job 2 + NiFi's REST feed must land at
      once. Flipping WS to `update` before REST snapshots flow makes every ex1 update reject
      `no_baseline`. Coordinate with the NiFi team's rollout.
- [ ] Run `smoke-pair-extractor.sh` live once NiFi's REST snapshot is actually on `ex1-raw`
      (parser change is unit-verified only; the resync path has never run against the live stack).
- [ ] Confirm with NiFi: is the injected field exactly `"pair"` and always the exchange market
      string (e.g. `BTCUSDT`, matching `exchange_markets`)? A case/format mismatch drops silently
      (`dropped-unknown-market`), same trap as the lbank note in [[pair-extractor]].

## Milestone 11 — gap-driven book drop via a new full-book aggregator (2026-07-21)

Full plan + rationale: `plans/aggregator-gap-drop.md`. Problem: on a sequence gap job 2 only
dead-letters the offending update and emits nothing downstream, so the consolidator keeps serving
that exchange's pre-gap diverged book (regresses the `orderbook-job` "gap ⇒ drop the exchange"
decision, `memory/project_orderbook_aggregation.md`). Fix: job 2 emits a `type="reset"` marker →
job 5 empties that exchange's book → a NEW terminal aggregator (consuming job 5's full books
directly) drops it from the union. Deprecates the consolidator + job 6. **NOT started — on hold
until the user authorizes coding.**

Verify FIRST (cheap, before writing code):

- [ ] Nothing besides the consolidator consumes the frozen `ex{id}-p{id}-{side}` per-level topics
      (believed true; confirm — gates deprecating job 6)
- [x] Jobs 3 (rebase) + 4 (precision) forward a null-sided `type="reset"` event untouched —
      CONFIRMED by reading the code 2026-07-21: neither inspects `type`; `RebaseFunction.rebaseLevels`
      and `PrecisionFunction.applyLevels` both return null for a null side. Job 3's only dead-letter
      is `no_rebase_row` (missing exchange_markets row) — the reset inherits the gap event's
      exchange/pair which job 1 already resolved, so as safe as any event. No job 3/4 change needed.
- [x] ~~`RawOrderBookEvent.type` is a plain string with no Avro enum constraint~~ → **WRONG,
      found live 2026-07-22.** `type` IS an Avro enum (`["snapshot","update"]`); serializing the
      new `"reset"` symbol NPE'd the TaskManager (`getEnumOrdinal("reset")` == null), the job
      FAILED, so the reset never reached job 5 and the book was never cleared. Fix: added `"reset"`
      to the `Type` enum in `schemas/raw_order_book_event.avsc` + re-registered (BACKWARD, v2/id 7).
      No Java rebuild (registry is the source of truth); running jobs must be resubmitted
      (`make run-normalizer-jobs`, NOT `refresh-normalizer`) to re-fetch. See `project_type_validator.md`.

### A. Job 2 emits a reset marker on gap

- [x] `TypeValidateFunction.processElement`, the true-gap `else` branch: ALSO emits a synthetic
      reset `RawOrderBookEvent` via `emitReset()` — `type=RESET` (`"reset"`, new package-private
      constant), `sequence_id=null`, `asks=null`, `bids=null`, same `exchange_id`/`pair_id`,
      `event_time` from the gap event. Uses a FRESH `PipelineTimings` (not the gap event's) so
      stamping `type_validate_out` on the reset doesn't leak onto the still-dead-lettered event.
      Once per episode (only the not-awaiting→awaiting transition reaches the branch). Offending
      update STILL dead-lettered. Done 2026-07-21.
- [x] Test `gapEmitsResetMarkerOncePerEpisode` (reset on main output, correct fields, dead-letters
      the update, no 2nd reset while awaiting) + 4 existing gap tests switched to a `validBusiness()`
      helper that filters resets. 17 tests green.

### B. Job 5 turns a reset into an emptied book

- [x] `BookBuildFunction`: branch for `type == "reset"` → `asks.clear()` + `bids.clear()` (else the
      existing snapshot/update path), then the shared emit produces an empty `OrderBookSnapshot`.
      Done 2026-07-21.
- [x] Test `resetEmptiesBook` (both sides empty + state truly cleared: a following update builds on
      the empty book). 16 tests green.

### C. New aggregator module (`flink/normalizer/job-aggregator/`) — DONE 2026-07-21 (code+tests)

- [x] Module scaffolded, flat package `io.tibobit.normalizer.aggregate`, main `AggregatorJob`
      (`env.execute("normalizer-aggregator")`); registered in `flink/normalizer/pom.xml`. Shaded jar
      builds, Main-Class correct.
- [x] Source: `KafkaSource<OrderBookSnapshot>` regex `ex[0-9]+-p[0-9]+-orderbook-snapshot-flink`,
      `OrderBookSnapshotDeserializer` (common), `OffsetsInitializer.latest()`
- [x] Split: `SnapshotSplitter` flatMap → two per-side `ExchangeBook`s (asks, bids), levels stamped
      with exchange_id; null side treated as empty
- [x] `CrossExchangeConsolidator` ported (union never-sum; sort asks↑/bids↓ tie-break qty desc) keyed
      `(pair_id, side)`; empty `ExchangeBook` (reset ⇒ empty book) replaces the entry + contributes
      nothing ⇒ exchange drops out. Models `ExchangeBook`/`ConsolidatedLevel`/`ConsolidatedOrderBook`
      + `ConsolidatedOrderBookSerializer` ported (serializer now uses common `AvroSchemaLoader`)
- [x] Sink: inline `KafkaSink` dynamic topic `p{id}-{side}`, subject `consolidated-order-book-event`
      (FROZEN web contract — unchanged)
- [x] Tests: ported stage-2 tests (9) + `SnapshotSplitter` tests (3), incl. "empty book ⇒ exchange
      removed from union, others intact". 12 green. `run-job.sh` is module-driven (no change);
      `./run-job.sh job-aggregator` runs it standalone.
- [ ] **DEFERRED to Part D (cutover):** wiring into the active `docker-compose-normalizer.yml` +
      Makefile `refresh-normalizer`. Auto-deploying the aggregator while the consolidator is still
      deployed would put TWO producers on the frozen `p{id}-{side}` topics — must swap, not add.

### D. Deprecate consolidator + job 6 (level-emitter)

- [x] Rename `flink/orderbook-consolidator/` → `flink/DEPRECATED-orderbook-consolidator/` + move its
      memory to `memory/deprecated/` (done 2026-07-21, commit d3883d0 — pure rename, mirrors 41bdd20)
- [ ] Add a DEPRECATED banner to `job-level-emitter` (mirror the `orderbook-job` pattern); KEEP the
      code, do not delete
- [ ] Stop deploying the consolidator + job 6: remove from the active `docker-compose-normalizer.yml`
      and the Makefile refresh flow; clean the 6 DANGLING path refs to the old
      `flink/orderbook-consolidator/` path (see the deprecation note at the top of this file)
- [ ] `reset.sh`: swap the consolidator → the new aggregator in the stateful-job reset list

### Verification (whole milestone)

- [ ] Manual-test-data: extend `10-ex1-sequence-gap` to assert the consolidated `p1-{side}` book
      DROPS ex1's levels at the gap and RESTORES them after the REST resync
- [ ] Live smoke: run the full chain (`docker-compose-normalizer.yml`), produce scenario 10, confirm
      in kafka-ui / web UI that ex1 vanishes from the consolidated book on the gap and returns on
      resync (all ex1 scenarios are still "not yet run live" — first live run is the real check)
- [ ] **Redo the live gap→drop test for EVERY delta feed** (the fix is exchange-agnostic — no code
      to repeat, only verification): after resubmitting jobs for the enum fix, run snapshot→updates→
      gap for **ex1 nobitex, ex6 bybit, ex8 okx** and confirm each drops out of `p{id}-{side}` on the
      gap and returns on resync. As of 2026-07-22 the enum fix is registered but NO feed verified live.
- [x] Memory/todo: update `project_type_validator.md` (reset emission), `project_book_builder.md`
      (reset branch), add `project_aggregator.md`, update `MEMORY.md` index + this file
- [x] Smoke: `smoke-aggregator.sh` added (2026-07-22), stale `smoke-level-emitter.sh` removed —
      raw ex8→whole chain→p1-{side}, asserts ex8 by exchange_id (frozen web contract has no
      timings), 4 cases incl. gap⇒reset⇒ex8 drops⇒resync returns. **Syntax-checked only, NOT run
      live** — case 3 needs the `"reset"` enum registered + jobs resubmitted, same as the manual test.

## Open items (decide at the flagged milestone)

- [ ] Job-1 source offsets: `latest` vs `earliest` for `ex{id}-raw`
- [x] ~~Job-3 missing-rebase-row behavior~~ → RESOLVED 2026-07-18: dead-letter `no_rebase_row`
- [x] ~~Job-4 truncate-to-zero hazard~~ → RESOLVED 2026-07-18: emit `"0"` and keep the level, so
      job 5 deletes it (not floor; an earlier "drop the level" answer was reversed same day)
- [ ] Refresh interval default for `RefreshingLookup`
- [ ] Retention values per new topic family
- [ ] Checkpointing/state backend for jobs 2/5/6 (stateful; currently the whole platform runs
      without checkpoints — bigger conversation, not pipeline-specific)
