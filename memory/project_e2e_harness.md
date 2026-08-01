---
name: e2e-harness
description: e2e/ is a standalone Go app (`go run .`); it recreates the docker compose stack, registers the Avro schemas, creates the Kafka topics for one exchange/pair, and builds and submits the normalizer Flink jobs.
metadata:
    type: project
---

# `e2e/` — warmup

`e2e/` is a standalone Go application (`go run .` from inside `e2e/`), not a `go test` package.
Module path is `orderbook-e2e`, so internal imports are `orderbook-e2e/<pkg>`.

## Layout

- `e2e/config/` — `config.Load(".env")` returns
  `Config{SchemaRegistryURL, SchemasDir, KafkaBroker, FlinkAPI, NormalizerDir, ComposeFile}`.
- `e2e/stack/` — `stack.Provision(ctx, composeFile)` runs `docker compose down -v` then
  `up -d --wait`, streaming compose's output.
- `e2e/schemaregistry/` — `schemaregistry.RegisterDir(registryURL, dir)` globs `*.avsc` from `dir`
  and POSTs each to `/subjects/{subject}/versions` as `{"schemaType":"AVRO","schema":...}`.
- `e2e/topics/` — `topics.Create(ctx, broker, exchangeID, pairID)` creates the 9 topics one
  exchange/pair pipeline needs, over the Kafka admin API (`kadm`), 1 partition / 1 replica each.
  `topics.Delete(ctx, broker, exchangeID, pairID)` removes the same 9 and waits until the broker
  stops listing them.
- `e2e/flink/` — `flink.CancelJobs(ctx, api)` cancels every running job and waits for each to reach
  a terminal state; `flink.RunJobs(ctx, api, normalizerDir)` builds the normalizer modules with
  `mvn`, then uploads and starts the 6 job jars over the Flink REST API. `RunJobs` does not cancel:
  the caller must have run `CancelJobs` first.
- `e2e/producer/` — `producer.SendJSON(ctx, broker, topic, doc)` compacts the document and produces
  it as one record (`kgo.ProduceSync`).
- `e2e/events/` — Go mirrors of the Avro records the pipeline carries, for decoding what comes
  back off the topics: `OrderbookSnapshot`, `PriceLevel`, `PipelineTimings`, `StepTimings`,
  `AvroTime`.
- `e2e/consumer/` — `consumer.ReadSnapshots(ctx, broker, registryURL, topic, wait)` and
  `consumer.ReadRejections(...)` read every record off a topic in offset order (both over the same
  unexported `readRecords`) and decode each (Confluent wire format, schema fetched by the id in the
  record) into an `events.OrderbookSnapshot` / into its `reject_reason` string.
- `e2e/warmup/` — `warmup.Run(ctx, cfg, exchangeID, pairID)` is the whole pre-scenario sequence:
  `flink.CancelJobs` → `topics.Delete` → `topics.Create` → `flink.RunJobs`.
- `e2e/scenario/` — `scenario.Run(ctx, cfg, s)` is the whole test:
  `schemaregistry.RegisterDir` → `warmup.Run` → `Scenario.produce` (every `Sources` entry to
  `ex{ExchangeID}-raw`, in order) → `Scenario.verify` (read
  `ex{id}-p{id}-orderbook-snapshot-flink` and then `ex{id}-p{id}-rejected-flink` back and check each
  against its wanted stream — generic `compare[T]`, length then `DeepEqual` per index).
  `scenario.go` holds the type and the run logic; `data.go` holds the shared conventions comment
  plus `NobitexSnapshot`, and `data_ex1/2/3/8.go` hold the 17 scenario cases as exported
  package-level vars. It imports `config`, `consumer`, `events`,
  `producer`, `schemaregistry` and `warmup`, so nothing below it may import `scenario`.
- `e2e/main.go` — `main()` and nothing else: load config, make a context, optionally
  `stack.Provision`, then loop the `scenarios` list calling `scenario.Run` on each.

Verified 2026-07-28 against the live stack: 4 subjects (ids 1–4) and the 9 `ex1-p1-*` / `ex1-raw` /
`p1-asks` / `p1-bids` topics, retentions confirmed via `kafka-topics --describe`; with `Delete`
in front, a second run deletes the 9 and recreates them empty. All 6 normalizer jobs reach RUNNING; a second run cancels
the 6 from the first (all reach CANCELED) before deleting the topics and resubmitting.

## Decisions

- **Each concern is its own package; `main.go` holds `main()` and nothing else** (2026-07-29). Every
  step — config, registry, topics, flink, producer, consumer, warmup, and the run itself
  (`scenario`) — is a sibling package, so `main` is load-config → `scenario.Run` and there is no
  logic in package `main` that another entry point could not reuse.
- **Topics are created over the Kafka admin API, never `docker exec kafka-topics`.** `warmup.sh` is
  only the reference for *which* topics exist and with what retention; the harness must not shell
  out to a container. `kadm.CreateTopic` returning `kerr.TopicAlreadyExists` is treated as success,
  which is the `--if-not-exists` behaviour.
- **A run starts by recreating the whole stack: `docker compose down -v` + `up -d --wait`.** The
  `-v` is the point — the Kafka, registry, postgres and NiFi volumes go with it, so nothing from a
  previous run survives (this is what `make refresh-normalizer` does on the server). Consequences
  to know: postgres is reseeded from `postgres/*.sql`, so the run sees the *local* seed, and the
  NiFi flow is wiped — the harness produces to `ex{id}-raw` itself, so it does not need NiFi.
  `up` is not given `--build`: a missing image is still built, and job code reaches the cluster as
  jars from the harness's own `mvn` build, not baked into the image. `--wait` replaces a
  hand-written readiness poll. This is the one place the harness talks to docker — the ban on
  shelling out to a container (`docker exec kafka-topics`) is about doing *pipeline* work that way,
  not about owning the stack's lifecycle.
- **Schemas are registered inside `scenario.Run`, after provisioning.** `RegisterDir` used to run in
  `main` before the run; with `down -v` in front of it that registered into a registry that was
  then destroyed, leaving the jobs with no subjects.
- **Bringing the stack to a clean start is one call, `warmup.Run`, not four in `scenario.Run`.** The four
  steps are a single unit with an order that must not drift, so they live behind one entry point in
  their own package; `scenario.Run` is then just warmup → produce → verify. `warmup` imports `config`,
  `flink` and `topics`, so nothing below it may import `warmup`.
- **Teardown order is jobs, then topics: cancel → delete → create → submit.** The jobs go down
  first because a running job holds the topics it consumes open while they are being deleted, and
  one left alive would attach to the recreated topics mid-setup and read the harness's own
  provisioning as pipeline input. That is why `RunJobs` no longer cancels — cancelling is a
  separate, earlier step (`flink.CancelJobs`), not part of submission.
- **Every run starts from an empty broker.** The warmup deletes the 9 topics before creating them, so
  no record from a previous run can be read as a result of this one — offsets, retained payloads and
  half-consumed stages all go away with the topic. Deletion is asynchronous: `DeleteTopics` returns
  before the topics are gone and creating one still marked for deletion fails, so `Delete` polls
  `ListTopics` until none of the names are listed. `UnknownTopicOrPartition` per topic is success —
  it is already absent.
- **Creation order matters.** The five normalizer stage topics are created before anything else:
  normalizer sources read from `latest`, so a topic missing when its job starts is discovered late
  and whatever was produced in between is lost. The same rule fixes job submission order:
  DOWNSTREAM-FIRST (aggregator → book-builder → precision → rebaser → type-validator →
  pair-extractor), so no job is started before the one it feeds.
- **Jobs are submitted over the Flink REST API; only the build shells out.** `flink.RunJobs` does
  `GET /jobs` + `PATCH /jobs/{id}?mode=cancel`, `POST /jars/upload` (multipart) and
  `POST /jars/{jarID}/run`, polling `GET /jobs/{id}` for the state. `exec` is used for `mvn` alone —
  no `docker`, no `curl`, no `jq`. The entry point is each jar manifest's `Main-Class`, so no
  per-module class mapping is needed.
- **One reactor build, not six.** `run-job.sh` builds each module with `-pl <module> -am`, which
  rebuilds `common` six times for the same jars; the harness runs `mvn -f <dir>/pom.xml package -q
  -DskipTests` once and then globs each `<module>/target/*-1.0-SNAPSHOT.jar`, skipping shade's
  `original-*` copy.
- **The scenario payload goes in raw, as the exchange sends it.** `Scenario.produce` produces each
  `Sources` entry verbatim (compacted) to `ex{ExchangeID}-raw` — the same topic NiFi writes to —
  so the run exercises the whole pipeline from the pair-extractor down. No key, no Avro encoding:
  the raw topic carries plain exchange JSON. Auto topic creation is off on the broker, so producing
  before `topics.Create` fails with `UNKNOWN_TOPIC_OR_PARTITION`.
- **A scenario declares its EXPECTED OUTPUT STREAMS PER TOPIC, not an expectation per source.**
  `Scenario{ExchangeID, PairID, Sources, WantSnapshots, WantRejects}` (2026-07-29). The reason is that a raw event has
  THREE possible fates, not two: a snapshot, a dead-letter on `ex{id}-p{id}-rejected-flink`, or
  nothing at all (job 1 drops noise frames / unknown markets — drop ≠ reject, see
  [[pair-extractor]]). Four sources can legitimately mean three snapshots and one rejection, so
  source index ≠ snapshot index and a per-source `Wanted…` field would need a "nothing here" case.
  The rejected alternative was `TestPayload{SourceData, *WantedSnapshot, WantedRejectReason}`
  filtered into two sequences at assert time; **the user chose the per-topic form and was right** —
  the scenarios are 4–6 events, so "which source caused snapshot 2?" is answered by counting, and
  the failure mode of miscounting while editing a scenario is a LOUD length mismatch, not a silent
  pass. `compare[T]` checks length first (that is what catches the book builder emitting too few or
  too many books) then `DeepEqual` per index.
- **`exchangeID`/`pairID` live IN the `Scenario`, they are not parameters of `scenario.Run`**
  (2026-07-29 refactor). It used to be `runTest(cfg, pairID, exchangeID, …)` — the opposite id order from
  `warmup.Run` / `topics.Create` / `topics.Delete`, which are all `(exchangeID, pairID)`. It was
  harmless only because the one call site passed `1, 1`; the first scenario on a second exchange
  would have provisioned one pipeline and asserted against another. Two positional `int64`s of the
  same type next to each other is the hazard, so they were folded into the struct instead of
  reordered. **Everything else in the harness keeps `(exchangeID, pairID)` — do not add a function
  that takes them the other way round.**
- **A sequence gap costs one dead-letter AND one extra, EMPTY snapshot** (2026-07-29, read out of
  `TypeValidateFunction.emitReset` / `BookBuildFunction`). On the not-awaiting → awaiting transition
  job 2 dead-letters the gap event *and* emits a synthetic `type=reset` marker onto the main stream;
  job 5 answers a reset by clearing both `MapState`s and emitting the book empty. So the snapshot
  stream for a gap scenario is `…, {Asks: {}, Bids: {}}, <resync snapshot>, …`, and every pre-gap
  level is gone until a snapshot re-seeds. Counting dead-letters is therefore NOT a complete oracle
  for a gap scenario — the empty book has to be in `WantSnapshots` too. Fires exactly once per gap
  episode; the `awaiting_snapshot` rejections that follow emit nothing.
- **Scenario timestamps stay on the fixed synthetic base `1800000000000` (`2027-01-15T08:00:00Z`) —
  do NOT shift them onto wall-clock** (2026-07-29). A future-dated book can poison the AGGREGATOR's
  stored last-event-time, but that does not reach this harness: it asserts on the book-builder
  topic, not the aggregator, and recreates every topic and all job state per run. Keeping the base
  fixed is what makes `EventTime` assertable at all — shifting it would make the field wall-clock
  and unassertable. **Do not "fix" the scenarios by shifting them.**
- **`EventTime` is second-resolution and ex3 has none at all.** The consumer formats with
  `time.RFC3339`, which has no fractional part, so okx's 300 ms steps collapse onto one string —
  consecutive ex8 snapshots are separated by their levels, not their timestamps. ex3 wallex carries
  no wire timestamp whatsoever (job 1 stamps `System.currentTimeMillis()`), so `Scenario` has an
  `IgnoreEventTime bool` that blanks the field on both sides before comparing; it exists for that
  one exchange and nothing else should set it.
- **The 17 scenarios run in one process, sequentially, and a failure does not abort the run**
  (2026-07-29). `main.go` holds a `scenarios` list of `{name, Scenario}` in numbered order (01–17);
  the name is only in that list, not on `Scenario`, so the 17 data vars stayed
  untouched. Each `scenario.Run` re-registers the schemas and re-warms *its own* exchange (cancel →
  delete topics → create → resubmit the 6 jobs), which is what makes them independent and also what
  makes the suite slow — that per-scenario warmup is the cost, not the 60 s snapshot wait. Because
  it is that slow, failures are collected and listed at the end rather than `log.Fatal`-ing on the
  first one; the process exits 1 if any failed. `stack.Provision` stays commented out at the top —
  the per-scenario warmup already gives a clean slate, and `down -v` per scenario would be absurd.
- **Index-matching within a topic assumes order is preserved end to end.** The raw topic has ONE
  partition and no job sets parallelism (no `setParallelism` in any job, no `parallelism.default` in
  compose — only `taskmanager.numberOfTaskSlots: 8`, so Flink's default of 1 applies; INFERRED from
  absence, confirm on the job graph). Repartitioning or parallelism > 1 breaks it and the assertion
  would have to key on something inside the event. **Known latent trap: jobs 2 and 3 sink to the
  SAME `rejected-flink` topic**, so order is only guaranteed within one job — a scenario mixing a
  type-validator reject (`sequence_gap`, `no_baseline`, `awaiting_snapshot`, `stale_or_duplicate`,
  `out_of_order`) with a rebaser `no_rebase_row` has no defined relative order and would need
  multiset comparison. All 17 manual scenarios reject at job 2 only, so it does not bite yet.
- **Only the rejection's `reject_reason` is asserted.** `rejected_at` is wall-clock and the envelope
  echoes the rejected event back verbatim (with `pipeline_timings`), so neither is assertable;
  the position in the stream already identifies which source produced it. This matches the oracle
  the manual scenarios document themselves with (a dead-letter count + reasons).
- **The consumer reads until the topic goes QUIET, it does not stop at a count.** `readRecords`
  waits up to `wait` for a first record, then keeps reading until 2s pass with nothing new.
  Stopping at the wanted count would make an over-emitting pipeline look correct. An EMPTY topic is
  not an error — a scenario that expects no rejection is normal, so emptiness is judged by the
  caller's length check, not by the consumer. Reading from the start of the topic is safe because
  the warmup deletes and recreates it, so it holds only this run's records.
- **Two waits: `snapshotWait` 60s, `rejectWait` 10s.** The snapshot budget is 60s because the
  payload has six jobs to cross. The dead-letter topic is read only AFTER the snapshot topic has
  settled, and jobs 2 and 3 are upstream of the book builder, so anything they were going to reject
  is already there — otherwise every scenario expecting zero rejections would pay a second full
  60s of waiting for nothing.
- **Decoding is goavro + a schema fetched by the record's own id.** The stage topics carry Confluent
  wire format (`0x00`, big-endian schema id, Avro datum), so `schemaregistry.SchemaByID` resolves
  the id via `GET /schemas/ids/{id}` — never a local `.avsc`, which would silently drift from what
  the job actually wrote. `goavro.NewCodec` then decodes it.
- **`events.OrderbookSnapshot`'s JSON tags do NOT match goavro's textual output.** Verified by
  round-tripping the real `order_book_snapshot.avsc` through goavro: it writes `event_time` as epoch
  millis (not ISO-8601), names union branches by their FULL Avro name (`io.tibobit.orderbook.
  PipelineTimings`, `long.timestamp-millis`) where the structs say `PipelineTimings` and `long` —
  the structs describe some other decoder's JSON. `consumer.decodeSnapshot` therefore unmarshals into its
  own small wire struct and fills `OrderbookSnapshot` itself, formatting `event_time` as RFC3339
  UTC and leaving `PipelineTimings` nil (wall-clock, not assertable). `events` is otherwise unused
  and still carries the mismatched tags.
- **The expected snapshot is derived from the source payload by hand, not captured.**
  `Scenario.WantSnapshots[0]` is what the book builder must emit for the ex1/pair-1 BTCUSDT
  payload: `event_time` is nobitex's `lastUpdate`, `exchange_markets(1,'BTCUSDT')` rebases by
  `10^0` on both sides so the numbers are unchanged, `markets(1)` truncates price to 2 and quantity
  to 8 decimals, and `Decimals.canonicalize` then strips trailing zeros (`0.50000000` → `0.5`,
  `62650.00` → `62650`). Asks sort ascending, bids descending. Change the seed's rebase or precision
  columns and this literal has to change with them. `EventTime` is an RFC3339 UTC string
  (`2027-01-15T08:00:00Z`) because that is the spelling `consumer.decodeSnapshot` produces from the
  `timestamp-millis` value, not because that is how it sits on the wire.
- **The event structs keep the Avro JSON union wrappers.** A nullable record arrives nested under
  its own name and a nullable `long` under `"long"`, so `PipelineTimings` is a wrapper holding one
  `StepTimings` field and timestamps are `AvroTime{Long string}` — the timestamps come through as ISO-8601
  strings, not epoch millis, even though the `.avsc` types them `long`/`timestamp-millis`.
  Verified by round-tripping a real book-builder payload: unmarshal + re-marshal is byte-identical.
- **Subjects are derived, not hardcoded.** The subject is the file name with `_`→`-`
  (`aggregated_order_book_event.avsc` → `aggregated-order-book-event`). A new `.avsc` needs no
  code change — but a file whose subject is not the dashed file name would register under the
  wrong subject.
- **`.env` is parsed by hand (~20 lines), no `godotenv`.** Real env vars win over the file;
  a missing `.env` is not an error.
- **Defaults**: `SCHEMA_REGISTRY_URL=http://localhost:8082`, `SCHEMAS_DIR=../schemas`,
  `KAFKA_BROKER=localhost:9092`, `FLINK_API=http://localhost:7070`,
  `NORMALIZER_DIR=../flink/normalizer`, `COMPOSE_FILE=../docker-compose.yml` (paths relative to
  `e2e/`). `e2e/.env` is not in the repo —
  only `.env.example`.
