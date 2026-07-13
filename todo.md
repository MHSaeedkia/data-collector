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

- [x] Collect REAL sample raw payloads per exchange — DONE 2026-07-13, see `sample-raw-data.md`
      (fetched from live `ex{id}-raw` topics via kafka-ui). Found **7 live exchanges**, not 3:
      nobitex, bitpin, wallex, ramzinex, bitget, bybit, ompfinex (okx in DB, no topic yet).
      Three regimes: full-snapshot-per-msg (ex1/2/4), per-SIDE snapshot no-seq (ex3),
      delta-with-seq (ex5/6/7). Remaining gaps for fixtures:
      - [ ] capture a bybit `type:snapshot` + bitget `action:update` + ompfinex initial-book
            frame (none in the latest-200 window)
      - [ ] investigate seq anomalies before job-2 rules: bitget non-monotonic `seq`,
            bybit `u` gaps, ompfinex `U`/`u` range gaps (NiFi losing messages?)
      - [ ] ex1 had multi-document Kafka records (2 newline-joined JSON docs, mixed channels) —
            job 1 must split records
- [ ] Coordinate the NiFi contract: verbatim payload bytes, topic `ex{id}-raw` per exchange,
      who creates the raw topics, retention. → verify: written agreement in
      `memory/project_raw_pipeline_decision.md`
- [ ] Design `schemas/raw_order_book_event.avsc` + example JSON — the ONE structured event for
      job 1–4 topics. Proposed fields: `exchange_id:int`, `pair_id:int`,
      `type:enum(snapshot,update)`, `sequence_id:long`, `sequence_jump:long`,
      `event_time:long timestamp-millis`, `asks`/`bids`: arrays of `{price:string,
      quantity:string}` (both sides in one event — intermediate topics have no side segment;
      split happens in job 6). Mirror `price_level_event.avsc` conventions
- [ ] `schemas/order_book_snapshot.avsc` + example — job-5 output: full maintained book
      (`exchange_id`, `pair_id`, `event_time`, `last_sequence_id`, `asks[]`, `bids[]`)
- [ ] `schemas/rejected_order_book_event.avsc` + example — dead-letter envelope: the rejected
      event + `reject_reason:string` (+ `rejected_at`)
- [ ] Register the 3 new subjects in `scripts/warmup.sh` (canonical fixed-name subjects only —
      NO per-topic `<topic>-value` subjects, see `memory/project_kafka_topic_strategy.md`)

## Milestone 1 — Scaffold `flink/normalizer/`

- [ ] Parent `pom.xml` (packaging `pom`, modules list, shared dependencyManagement: Flink,
      kafka connector, avro + confluent registry deps, JUnit5/AssertJ/flink-test-utils/JaCoCo —
      versions copied from `flink/orderbook-consolidator/pom.xml`) → verify: `mvn validate`
- [ ] `common/` module (plain jar, no shade): 
      - [ ] Models for the shared event/book/rejection shapes (plain POJOs, no Jackson —
            Avro GenericRecord mapping happens in serde classes, consolidator pattern)
      - [ ] `AvroSchemaLoader.loadLatest(url, subject)` — registry-only at runtime (port from
            consolidator; schemas NEVER bundled in shaded jars)
      - [ ] Avro serde pairs per shared shape (`toGenericRecord`/`fromGenericRecord` as pure
            package-private statics — the consolidator's testable-mapping pattern)
      - [ ] `RefreshingLookup` — periodic-refresh Postgres reference reader: loads a `Map` via
            JDBC in `open()`, re-loads every `REFRESH_INTERVAL` on a schedule; on refresh
            failure keep last-good snapshot + log. TDD with a fake loader fn
      - [ ] BigDecimal helpers: canonicalize (`stripTrailingZeros().toPlainString()`), rebase
            (`scaleByPowerOfTen`), truncate (`setScale(p, DOWN)`) — pure, test-first
      → verify: `mvn -pl common -am test` green
- [ ] One parameterized `run-job.sh` + `Dockerfile` + `Makefile` at `flink/normalizer/` root
      (module name as arg; derive jar + main class per module) → verify: script builds a chosen
      module and prints the submit command against a local cluster
- [ ] `docker-compose-normalizer.yml` at repo root (Flink cluster + kafka + schema-registry +
      postgres + kafka-ui, mirroring the consolidator compose incl. `restart: on-failure`,
      log-dir volumes, named volumes) → verify: `docker compose config` passes

## Milestone 2 — Job 1: pair extractor (`ex{id}-raw` → `ex{id}-p{id}-raw-flink`)

TDD throughout (`memory/project_tdd_workflow.md`): tests first, fixtures from Milestone 0.

- [ ] `RawExchangeParser` interface: `byte[] payload → List<RawOrderBookEvent>` (pair still as
      the exchange's market string at this point) + one implementation per exchange, selected
      by `exchange_id` parsed from the source topic name. Test-first against the real fixtures
- [ ] Market-string → `pair_id` resolution via `RefreshingLookup` over
      `exchange_markets(exchange_id, market) → market_id`; unknown market string → log + drop
      (+ counter) — NOT dead-letter (dead-letter is job-2's validation concern)
- [ ] Source: `KafkaSource<byte[]>` pattern `^ex[0-9]+-raw$`, earliest-or-latest decision
      (propose `latest`, consistent with consolidator), Kafka metadata needed: topic name (for
      exchange_id) — use a `KafkaRecordDeserializationSchema` that captures topic
- [ ] Sink: `KafkaSink` with topic selector `ex{exchange_id}-p{pair_id}-raw-flink`, Avro via
      registry subject `raw-order-book-event`
- [ ] `PairExtractorJob.main`: env config (`KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`,
      `POSTGRES_*`, `KAFKA_GROUP_ID=normalizer-pair-extractor`, `REFRESH_INTERVAL`); anonymous
      `KeySelector` classes if keying is needed (Flink lambda inference gotcha)
      → verify: `mvn -pl job-pair-extractor -am test` green; live smoke: publish a fixture
      payload to `ex1-raw`, see the structured event on `ex1-p{id}-raw-flink` via kafka-ui

## Milestone 3 — Job 2: type validator (→ `ex{id}-p{id}-type-validated-raw-flink` + dead-letter)

- [ ] `TypeValidator` keyed `(exchange_id, pair_id)` `KeyedProcessFunction`, `ValueState
      {lastSeq, awaitingSnapshot}`; rules ported from orderbook-job Phase 3 semantics:
      snapshot = unconditional baseline (stores seq, clears awaiting); update valid iff
      `seq == lastSeq + sequence_jump`; `seq <= lastSeq` → reject `stale_or_duplicate`;
      gap → reject `sequence_gap` + set awaitingSnapshot (updates rejected `awaiting_snapshot`
      until next snapshot); leading update before any snapshot → reject `no_baseline`.
      Valid → main output; rejects → side output with reason. Test-first via
      `KeyedOneInputStreamOperatorTestHarness` covering every branch above
- [ ] Wiring: source (pattern `^ex[0-9]+-p[0-9]+-raw-flink$`) → keyBy → validator → two sinks
      (validated topic selector / dead-letter `ex{id}-p{id}-rejected-flink` with
      `rejected-order-book-event` serde)
      → verify: module tests green; live smoke: feed an out-of-order sequence, see the reject
      + reason on the dead-letter topic

## Milestone 4 — Job 3: rebaser (→ `ex{id}-p{id}-rebased-flink`)

- [ ] Stateless `RichMapFunction`: every level's `price × 10^price_amount_rebase`,
      `quantity × 10^volume_amount_rebase` via `scaleByPowerOfTen` (exact); rebase values per
      `(exchange_id, pair_id)` from `RefreshingLookup` over `exchange_markets`. Missing row →
      decide drop-vs-passthrough at implementation (flag!). Test-first: rebase 0 identity,
      positive/negative exponents, exactness (no double anywhere)
- [ ] Wiring: source `^ex[0-9]+-p[0-9]+-type-validated-raw-flink$` → map → sink
      → verify: module tests green; live smoke with a nonzero rebase row in postgres

## Milestone 5 — Job 4: precision normalizer (→ `ex{id}-p{id}-applied-precision-flink`)

- [ ] Stateless `RichMapFunction`: `price.setScale(markets.price_precision, DOWN)`,
      `quantity.setScale(markets.quantity_precision, DOWN)`; precisions per `pair_id` from
      `RefreshingLookup` over `markets`; NULL precision column → leave value untouched.
      **DESIGN FLAG to resolve here**: nonzero quantity truncating to exactly 0 becomes a
      level-delete downstream — decide accept vs floor before coding. Test-first: truncation
      never rounds up, already-fewer-decimals unchanged, zero-quantity passthrough, null
      precision passthrough
- [ ] Wiring: source `^ex[0-9]+-p[0-9]+-rebased-flink$` → map → sink
      → verify: module tests green

## Milestone 6 — Job 5: book builder (→ `ex{id}-p{id}-orderbook-snapshot-flink`)

- [ ] `BookBuilder` keyed `(exchange_id, pair_id)` `KeyedProcessFunction`, `MapState` per side
      keyed by canonicalized price (`stripTrailingZeros().toPlainString()` — MapState is
      hash-based, won't collapse scales, consolidator lesson): snapshot → replace book
      wholesale; update → `quantity > 0` upsert / `== 0` delete (BigDecimal `signum()`);
      emit the FULL book (both sides + `last_sequence_id` + `event_time`) on every change.
      Sequence rules are NOT re-checked (job 2 already validated; topics are single-partition
      so order holds). Test-first via harness: replace/upsert/delete/emit-shape/canonical-price
- [ ] Wiring: source `^ex[0-9]+-p[0-9]+-applied-precision-flink$` → keyBy → builder → sink
      (`order-book-snapshot` serde)
      → verify: module tests green
- [ ] NOTE cold-start limitation (no checkpointing configured — same known gap as the old
      merger): book is empty after restart until the next snapshot; record, don't solve now

## Milestone 7 — Job 6: level emitter (→ existing `ex{id}-p{id}-{side}`, `price_level_event.avsc`)

- [ ] `LevelDiffEmitter` keyed `(exchange_id, pair_id)`, state = last emitted book per side:
      diff incoming full book vs last — changed/added prices → emit `price_level_event` upsert;
      vanished prices → emit `quantity="0"` delete; first book → emit all levels. Confirm this
      diff approach at implementation start (recorded as the likely-correct option — the
      consolidator only removes levels on qty=0). Test-first: add/change/vanish/first-book/
      no-change-no-emit
- [ ] Sink: per-record topic selector `ex{exchange_id}-p{pair_id}-{side}` reusing the EXISTING
      `price-level-event` registry subject — output must be byte-identical in format to what
      the consolidator already consumes (its tests/serde define the contract)
      → verify: module tests green; live smoke: full chain `ex1-raw` fixture → consolidated
      book visible in `web/` UI with the consolidator running unchanged

## Milestone 8 — Infra, provisioning, cutover

- [ ] Extend `scripts/warmup.sh`: create `ex{id}-raw` + the 5 per-pair intermediate/dead-letter
      topic families for subscribed exchange_markets; decide retention per family (raw + audit
      topics probably longer than 1h — pick values); sequential creation (parallel xargs was
      reverted before, don't retry)
- [ ] kafka-ui serde config in `docker-compose-normalizer.yml`: `topicValuesPattern` +
      `schemaNameTemplate` per new topic family → the 3 new canonical subjects (pattern from
      `memory/project_kafka_topic_strategy.md`; registry stays clutter-free)
- [ ] `fake-data-generator/`: new mode emitting realistic RAW exchange payloads to `ex{id}-raw`
      (stand-in for NiFi during dev)
- [ ] Root `Makefile`: `refresh-normalizer` target; `README.md` section for the pipeline
- [ ] Server deploy: build + submit all 6 jobs (`sudo`, Temurin 21 —
      `memory/project_ubuntu_server_env.md`); NOTE all composes share container names/ports —
      revisit "one stack at a time" rule, the normalizer must run ALONGSIDE the consolidator
- [ ] Cutover plan: run new pipeline in parallel with NiFi's current normalized output, compare
      `ex{id}-p{id}-{side}` streams for equality window, then switch NiFi to raw-only; decide
      fate of `flink/orderbook-job/` afterwards

## Open items (decide at the flagged milestone)

- [ ] Job-1 source offsets: `latest` vs `earliest` for `ex{id}-raw`
- [ ] Job-3 missing-rebase-row behavior; job-4 truncate-to-zero hazard
- [ ] Refresh interval default for `RefreshingLookup`
- [ ] Retention values per new topic family
- [ ] Checkpointing/state backend for jobs 2/5/6 (stateful; currently the whole platform runs
      without checkpoints — bigger conversation, not pipeline-specific)
