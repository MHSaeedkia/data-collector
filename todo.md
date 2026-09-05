# TODO

> The nine sections below were removed on `docs/flink-production-hardening` while that branch was
> in flight and restored here when it merged (2026-08-31). They are the state as of `ae08ba4`; the
> branch never touched them. `## flink (production readiness)` at the end is what that branch added.

## adjustment

- [x] **Step 1 — `flink/adjustment/` created, pass-through** (2026-08-24, user request) — standalone single-module project mirroring `flink/merger`'s shape, reads `p{id}-{side}` and writes the identical record to `p{id}-{side}-adjusted`. Output reuses the subject `aggregated-order-book-event`, so no new schema and no registry step to deploy. No transform operator between source and sink — step 1 has no logic to put in one. 6 tests green, 3 mutations confirmed to fail (drop `source_id`, drop `simulation`, write `side` as a String), shaded jar clean with zero flink/avro entries. `warmup.sh` pre-creates the `-adjusted` family; `ALL_JOBS` submits it. **NOT run live.** See [[project_adjustment]]
- [x] **Three empty adjustment stages added, in the user's order** (2026-08-24) — `BuySellCommissionFunction` → `OurProfitFunction` → `SlippageFunction`, chained and `.name()`d in `AdjustmentJob`, each returning its input unchanged. Reverses the first version's "no identity operator" call at the user's explicit request. 11 tests green; mutation-checked (one stage altering a price fails both its own test and the chain test)
- [x] **Step 3 — the three stages now move the price** (2026-08-24) — commission 0.35%, profit 0.1%, slippage 1%. **Each sizes its amount off the price the level ARRIVED with, so they ADD to asks × 1.0145** (corrected by the user the same day: the first version compounded to × 1.014548535). `AdjustedLevel.basePrice` holds the arrival price and no stage writes it; it is not on the wire. Side effect worth knowing: the chain order no longer changes the result, since addition commutes. Each stage writes the rate it applied onto the record, so the event says what was charged and not merely the result. That needed a new schema + subject `adjusted-order-book-event` (+ `_example.json`, + a `register_schema` line in `warmup.sh`) — steps 1–2's reuse of the aggregated subject is dead. Exact BigDecimal, `movePointLeft(2)` not `divide(100)`. 19 tests, 4 mutations verified (invert the sign → 7 fail; double arithmetic → 4; serializer drops a rate → 2; size the amount off the running price instead of the base → 4). **NOT run live**
- [ ] **⚠ CONFIRM THE SIGN CONVENTION.** `Prices.multiplier` moves asks UP and bids DOWN (a charge takes money from the same side of the trade both times). Standard, and it is the one method the whole job's correctness rests on — but it was ASSUMED, never stated by the user. Inverting it publishes a book you could buy from and sell back into at a profit
- [x] **Schema for profit/slippage landed (2026-08-25, user request)** — `exchange_markets` gains `our_profit_percent`/`slippage_percent` (`NUMERIC(6,3) NOT NULL DEFAULT 0.1`/`1`, `CHECK >= 0`), added to `01_schema.sql` + backfilled onto all 351 `02_seed.sql` rows at the defaults. Granularity settled: flat percent per `(exchange_id, market_id)`, NOT per-market-only — the user confirmed slippage genuinely differs by exchange for the same market, which also ruled out a separate joined table (breaks the automatic NOT NULL/default guarantee inline columns give for free). Commission stays out of scope (hardcoded, plausibly per-LEVEL later). See [[project_adjustment]], [[project_db_schema]]. ⚠ **Live server DB needs the `ALTER TABLE` run by hand** — `01_schema.sql` only applies to a fresh volume
- [x] **DB read wired into `job-adjustment`** (2026-08-25) — `OurProfitFunction`/`SlippageFunction` are now `RichMapFunction`s each holding their own `RefreshingLookup<String,AdjustmentFactors>` (new `AdjustmentFactors`/`AdjustmentFactorsLoader`, mirroring `RebaseFactors`/`RebaseFactorsLoader`), keyed `{exchange_id}|{pair_id}`. **Forced a breaking schema change** en route: `our_profit_percent`/`slippage_percent` moved from `adjusted_order_book_event.avsc`'s record level onto the nested `AdjustedLevel` — a book unions levels from multiple exchanges, so a record-wide rate can't represent per-exchange rates (confirmed with the user before implementing). Commission untouched (still a flat constant, out of scope). Fallback to the old hardcoded constant (`DEFAULT_PERCENT`) when a level's `(exchange, pair)` has no exchange_markets row — this job has no dead-letter output to raise a miss into. `mvn -o clean test` 21/21 green. See [[project_adjustment]]
- [x] **LIVE DEPLOY FAILED then fixed same day (2026-08-25)** — first submission crashed on startup, every attempt, `ClassNotFoundException: org.postgresql.Driver` (no retry — `NoRestartBackoffTimeStrategy`). The postgres dependency was wrongly marked `provided` in `flink/adjustment/pom.xml` (copied the OTHER dependencies' scope without checking); `/opt/flink/lib/` does NOT carry the postgres driver on this image, so it must ship in the job's own shaded jar (`compile` scope, no `<scope>` tag — same as job-rebaser/job-pair-extractor). Fixed, confirmed `org/postgresql/Driver.class` present in the rebuilt jar via `unzip -l`, redeployed with `./flink/run-job.sh adjustment`. See [[project_adjustment]]
- [x] **Commission given the exactly-same DB treatment (2026-08-25, user request)** — `exchange_markets.buy_sell_commission_percent` added (`NUMERIC(6,3) NOT NULL DEFAULT 0.35`, `CHECK >= 0`, `01_schema.sql` + all 351 `02_seed.sql` rows), `BuySellCommissionFunction` converted to a `RichMapFunction` with its own `RefreshingLookup<String,AdjustmentFactors>` exactly like `OurProfitFunction`/`SlippageFunction`. `AdjustmentFactors` extended to a 3-arg POJO (one query now selects all three columns). **`buy_sell_commission_percent` moved off the record onto `AdjustedLevel` too** — the record now carries ZERO rate fields, `Prices.applyPercent` (record-wide) was dead code once this landed and was deleted, `applyPerLevelPercent` is the only arithmetic method left. Also caught and fixed a real bug from the previous step: the `02_seed.sql` INSERT header never got `our_profit_percent, slippage_percent` appended (an earlier multi-`-e` sed aborted on a syntax error, silently dropping ALL its clauses) — 8 named columns, 10 values per row, broken SQL that had already been committed. 23/23 tests green (18+5), shaded jar has the postgres driver bundled (compile scope) and zero flink/avro entries. See [[project_adjustment]]
- [x] **VERIFIED LIVE end to end (2026-08-25)** — `make run-all-jobs` against the real stack, `orderbook-adjustment` came up `RUNNING` and stayed healthy with no exceptions, confirming the 3-column DB read works against the actual server `exchange_markets` table, not just tests. Hit and fixed an UNRELATED infra bug along the way: a Flink JobManager slot leak (`NoResourceAvailableException` on all 6 normalizer jobs despite matching slot count) — fixed by restarting `taskmanager` AND `jobmanager` (taskmanager alone was not enough). See [[project_flink_deploy_tooling]]
- [ ] Decide whether prices should be re-truncated to `markets.price_precision` after adjustment. Job 4 applied it upstream; multiplying re-introduces decimals past it (prices now run to 14 significant figures). Needs the per-market precision this job does not read, AND a rounding-direction decision — down favours the buyer on asks and us on bids
- [ ] Nothing consumes `p{id}-{side}-adjusted` — not `web/`, not the staleness exporter, no e2e scenario. Same three gaps `-merged` has, plus this one now has a schema of its own that only this job knows about
- [ ] **⚠ The cluster is now at 8/8 task slots** (6 normalizer + merger + adjustment, parallelism 1 each, `taskmanager.numberOfTaskSlots: 8`). The next job or any parallelism increase needs the taskmanager reconfigured — this is the last slot the merger's note flagged
- [ ] **Decide the lineage rule for the adjusted record.** Step 1 carries job 6's `id` through verbatim, which is what "without any changes" means but conflicts with [[project_record_lineage]]'s re-stamp-on-write convention. The aggregated schema has no record-level `source_ids` to name a parent in, so honouring the convention would need a schema change. Settle it before step 2 writes anything genuinely different
- [ ] `p{id}-{side}-adjusted` is not watched by the staleness exporter, has no e2e coverage, and is not consumed by `web/` — the same three gaps `-merged` has

## market subscriptions

- [x] `markets/` → `market-subscriptions/` + a Go operator console (2026-08-22, user request) — reads `exchange_markets` joined to `exchanges`, drives NiFi's two `/control-plane` endpoints, one binary with the UI `go:embed`ded (user asked for a single service, not split frontend/backend). Table with status badges, filter by exchange/status/search, click-to-sort, checkbox multi-select and "select all shown" for whole-exchange actions, per-market results so one failure never blocks the rest. All 9 settings in `.env`, nothing hardcoded. Mirrors `web/`'s layout, vendored, test-stage Dockerfile, compose service on :8090. **18 tests green, build/vet/gofmt clean; NEVER run against a live postgres or NiFi — docker was down.** See [[project_market_subscriptions]]
- [x] The old CSV CLI kept, not deleted (user decision) — `market-subscriptions/csv-bulk-sync/` with its own README explaining when to use which, and warning that its 364 rows are all `disable` so a plain run sends nothing
- [ ] **Verify the pending→settled handshake against real NiFi** — the whole design rests on the user's statement that NiFi writes `subscribe`/`unsubscribe` back to the row. Nothing in this repo does that, and nothing here can prove it. If NiFi does NOT write the column, every row will stick at `pending-*` and the console needs a different settlement story
- [ ] `internal/postgres` has no tests (needs a live DB) — the `::subscription_status` cast and both queries are unverified against a real schema
- [ ] No auth on the console — anyone who can reach :8090 can unsubscribe every market. Fine on a private network, decide before exposing it
- [ ] A market can be actioned while already pending (the UI allows it as a deliberate override). Confirm that is what you want, or block it

## scripts

- [x] `scripts/purge-topics.sh` (2026-08-19, user request) — empties every pipeline topic with `kafka-delete-records` (immediate low-watermark bump) while leaving topics, partition counts, retention config and registry subjects intact. The counterpart to `warmup.sh`, and a scalpel where `make refresh-normalizer` is a sledgehammer (`down -v` wipes the registry and postgres too). Matches the LIVE topic list rather than re-deriving from postgres, so it also catches unsubscribed markets' leftover topics. `--dry-run` / `--yes` / `--quiet`. **Rewritten the same day after the user hit what looked like a hang** — it was one `docker exec` (one JVM) per topic; now two bulk calls total, with every command echoed and timed. That rewrite surfaced two more real bugs: progress logged to stdout was being captured as topic names, and record counts used `latest` when `kafka-delete-records` raises the START offset (so an empty broker reported 40k records to delete). Verified end-to-end against a stubbed broker: dry-run, full purge, idempotent re-run reporting 0, plus both failure paths (broker command fails, container down). **Never run against a live broker — docker was down here**
- [ ] `purge-topics.sh` holds a THIRD copy of `NORMALIZER_STAGES` (after `warmup.sh` and the exporter) — a stage missing from any copy is silently skipped. Worth one shared source before the next stage is added
- [ ] Consider a `make purge-topics` target, and whether it should chain `run-normalizer-jobs` automatically — purging Kafka leaves Flink's keyed state intact, so a purge alone does NOT give a clean run and it is easy to forget
- [x] `RAW_RETENTION_MS` and `REJECTED_RETENTION_MS` lowered 7 days → 2 days in `scripts/warmup.sh` (2026-08-24, user request, both) — same `--if-not-exists` caveat as always: only lands on topics not yet created

## control plane

- [x] e2e coverage for `control-plane` (2026-08-17, user request) — the topic is now created and deleted per run in `topics.plan()` instead of being left to the broker's auto-create, so a scenario reads its own commands and not the previous one's. `Scenario.WantControlCommands` is asserted on EVERY scenario: **nil means "nothing was requested", not "skip"**, which is what makes a spurious request on a healthy feed fail — 33 scenarios get that for free, and the 8 ending on `no_baseline`/`sequence_gap` declare the one command they produce. Two new feature-grouped scenarios in `data_control.go` (42 ex6 sequenced resync, 43 ex1 null-seq REST resync) each run two episodes to prove one-command-per-episode AND that a resync re-arms the next. The Kafka key is checked structurally then stripped, like the lineage ids; the decoder was the only one with no registered schema behind it, so it used `DisallowUnknownFields` (superseded 2026-08-18 when the topic moved to Avro). Go build/vet/gofmt clean, all Go tests green, `swag init` rerun. See [[project_control_plane]]
- [x] **42/43 verified live** (2026-08-17) — both PASS against the real stack over `-serve`, and the counts confirm the assertion is not vacuous: 42 = 8 snapshots / 4 rejections / **2 commands**, 43 = 5 / 3 / **2 commands**. `30-ex6-snapshot-then-deltas` was re-run as the negative control and read **0 commands**, so the undeclared-means-none assertion the other 33 scenarios rely on is real. The feature works end to end on the normalizer side: a gap does put exactly one `snapshot_request` per episode on `control-plane`, a resync re-arms it, and a healthy feed asks for nothing
- [x] **Resync deadlock fixed (2026-08-19, user-reported)** — "when we have a gap it does not get a snapshot, and sometimes no command is sent". One cause: neither ordering guard cleared `snapshotRequested`/`awaitingSnapshot` on rejection, so a REJECTED resync snapshot wedged the key permanently — `requestSnapshotOnce` is gated only on `snapshotRequested`, and `lastEventTime` only advances inside `emit()`, which a rejected event never reaches, so every later snapshot failed the same stale comparison. Both guards now sit behind `!resyncOutstanding()` (= a request is outstanding ⇒ there is no good book to protect, the `RESET` already emptied it). 3 regression tests — TWO fail with the fix neutralised, the third is a negative control that passes either way (the "each verified to fail" first written here was wrong, corrected 2026-08-22); 34/34 green. **Verified live 2026-08-22** by scenarios 44/45 below. See [[project_control_plane]]
- [x] ~~Retry via `onTimer` (2026-08-19)~~ — **REPLACED 2026-08-22, see the redesign below.** The timer worked but cost `pendingSimulation`/`pendingSourceId`, a key parse, and an uncancelled timer per episode that multiplied the command rate when episodes overlapped
- [x] **Control plane redesigned around one state field (2026-08-22, user request: rethink it from before the feature existed)** — `awaitingSnapshot` + `snapshotRequested` + `pendingSimulation` + `pendingSourceId` collapse into ONE `ValueState<Long> resyncRequestedAt` (value = when we last asked, nullness = is the book untrustworthy); the reject reason is derived from `lastSeq == null` instead of stored. Re-asking is driven by the rejected events themselves — both untrustworthy branches call `askForSnapshot` on every event they turn away, suppressed unless `snapshotRetryMs` has elapsed — so the timer, `onTimer`, `scheduleRetry`, `requestSnapshotOnce` and the key parse are all gone. **The feature now costs ZERO net state**: 4 `ValueState` fields, the same four the function had at `51be8dc` before the control plane existed (7→4 fields, 25→19 methods). Deliberate trade-off: a market that goes silent after a gap gets no retries, because a silent feed cannot be re-synced by anything on the topic and its first update on returning asks anyway. 41 unit tests, mutation-checked three ways (kill the guard exemption → 6 fail; never re-ask → the 4 retry tests fail; no interval → the 4 flood guards fail). **All 45 e2e scenarios PASS live**
- [x] **e2e cover for the deadlock itself (2026-08-22, user request)** — 42/43 both re-sync with a snapshot the ordering guards were always happy to accept, so the suite written for this feature never exercised a REJECTED resync, which is the whole bug. Two scenarios added in `data_control.go`: `44-control-ex6-stale-resync-accepted` (sequenced guard, resync at `u=250` below the pre-gap 301) and `45-control-ex1-lagging-rest-resync` (event-time guard, REST resync stamped 08:00:00 behind a last-accepted delta of 08:00:02, so the emitted snapshot's event time steps BACKWARDS). Each asserts the resync came out as a snapshot AND that a later gap re-armed the request. **Verified live, and mutation-checked against the live stack**: with `resyncOutstanding()` forced false, 44 reads 4 snapshots instead of 7, 45 reads 5 instead of 8, and `control-plane` holds 1 command instead of 2 — the user's "sometimes no command is sent at all", reproduced end to end. All PASS with the fix, alongside 42, 43 and 30 as the negative control. Go build/vet/test/gofmt clean
- [ ] **Re-asking still has no e2e cover** — `SNAPSHOT_RETRY_MS` is read from the JobManager env and set nowhere in `docker-compose.yml`, so every run takes the 5-min default while a scenario finishes in ~20s (which is also why it does not make the suite flaky). Needs the env var on the jobmanager service AND a per-scenario override, since a short global interval would make every scenario ending on an unresolved episode (02, 03, 10, 11, 33, 36, 38 …) emit extra commands mid-verification. The unit tests carry it meanwhile, via `timerService()` which the harness clock drives
- [x] ~~Overlapping episodes stack retry chains~~ — **gone with the timer (2026-08-22)**. One timestamp per key cannot stack; a second episode overwrites the first's clock. Pinned by `overlappingEpisodesDoNotMultiplyTheAskRate`, which asserts the ask rate stays at one per interval rather than one per episode ever opened
- [x] ~~`onTimer` defaults a missing `pendingSourceId` to `""`~~ — **gone with the timer (2026-08-22)**. There is no pending state to be missing: the triggering event is in hand on every ask, so every command names a real dead-lettered record
- [ ] `scripts/diagnose-stuck-markets.sh` declares `WINDOW_MS` and documents it as "how far back to read", but never uses it — the script always reads `--from-beginning`. Misleading in a tool reached for during an incident. It also consumes each dead-letter topic twice (8s timeout each) to get the summary and then the last reason; one pass gives both
- [ ] Tune `SNAPSHOT_RETRY_MS` once the NiFi consumer exists — 5 min is a guess. It interacts with the cold-start burst below (every delta-feed key asks at once after a restart, then re-asks on the same interval), though the event-driven ask is naturally bounded by feed activity in a way the timer was not
- [x] ~~A gap on the FIRST update after a REST snapshot is silently swallowed (ex1/ex2)~~ — **MOOT 2026-09-02**. ex1/ex2 no longer send an `update` type at all (their WS pushes are snapshots — see [[project_pair_extractor]]), so the `baselinePending` branch this item was about is never reached by either exchange any more. The underlying "first update after a resync isn't contiguity-checked" question is still real for exchanges that DO use this bootstrap (ex6), but nobody had opened a todo item for it under that name — worth a fresh item if it matters there
- [ ] **Job 2 trusts `sequence_jump` blindly** — `seq == last + jump` with `jump == 0` means `seq == last`, which also satisfies `seq <= last`; the first branch wins so a DUPLICATE is accepted as valid, and the genuine next update falls into the GAP branch. Nothing validates `jump >= 1` for an update. Depends entirely on the parsers ([[project_pair_extractor]]) stamping it right
- [x] ~~`no_baseline` requests a snapshot but never sets `awaitingSnapshot`~~ — **resolved by the state collapse (2026-08-22)**. There is one flag now, and the two conditions are told apart by `lastSeq == null`, which is what chose the reject reason all along. This asymmetry was the thing that made the deadlock hard to reason about, so it is worth noting it was a design smell that predicted a real bug
- [ ] `scripts/diagnose-stuck-markets.sh` (2026-08-19) reads `control-plane` and every `-rejected-flink` topic together and flags markets whose last rejection is `awaiting_snapshot`/`no_baseline` — the deadlock fingerprint. **Syntax-checked only, never executed** (docker was down); run it against the live stack to confirm whether any market is still wedged from before the fix
- [ ] **Nothing consumes `control-plane`** — `nifi/` is a Dockerfile. Whether NiFi can serve an on-demand snapshot for the DELTA feeds where gaps actually happen (ex6/bybit, ex8/okx — may need a WS resubscribe, not a REST call) is the open question the feature stands on. Needs a wire-shape doc for the NiFi team, the way `sample-raw-data.md` is for the raw payloads
- [x] `ControlCommand` now carries `simulation`, `id` and `source_ids`, and the topic moved from plain JSON to Avro on the registry — subject `control-command`, `schemas/control_command.avsc` (2026-08-18, teammate, commits `8071020` + `96b2bf1`). Reviewed: the serializer is a faithful copy of `AggregatedOrderBookSerializer` (registry-fetched write schema, lazy on the task), the sink wiring is correct, and `mvn test` is green — 29 tests. Almost the whole 206-line diff is IDE reformatting; the real change is ~8 lines. Four defects found in review, all fixed the same day — see the next item and [[project_control_plane]]
- [x] All four review findings fixed (2026-08-18, user request), **re-verified live on a stack rebuilt from scratch**: `scripts/warmup.sh` now registers `control-command` (id 5 on the real run — the e2e harness had been hiding this by registering every `schemas/*.avsc`); the e2e control decoder goes through `textual()` like the other four readers and `events.ControlCommand` gained `Simulation` (declared, compared literally, server rejects anything but 1) plus `ID`/`SourceIDs` (checked structurally by `checkControlLineage`, then stripped); `requestSnapshotOnce` now DERIVES lineage — `Lineage.newId()` + `List.of(event.getId())` — and the schema field is `simulation`, with `doc` on every field like the other four. Counts identical to the pre-Avro run: 42 = 8/4/2, 43 = 5/3/2, 30 = 3/0/0, and the topic was read directly to confirm `simulation: 1`, distinct minted ids and one distinct parent per command are really on the wire rather than defaults the harness agreed with. Java 31 tests, Go tests, `go vet`, `gofmt`, `swag init` all green
- [x] Regression cover for both bugs, mutation-checked: `snapshotRequestHasItsOwnLineage` and `snapshotRequestCarriesSimulationFlag` in `TypeValidateFunctionTest` (the lineage one was confirmed to FAIL when the fix is reverted), and `TestCheckControlLineage`/`TestStripControlLineage` in `e2e/scenario/control_test.go` including the inherited-id case. `e2e/consumer/consumer_test.go` was deleted — it pinned the dead plain-JSON shape, and the replacement decoder needs a live registry, exactly like the other four that have no unit test
- [ ] Cold start asks for every subscribed market at once (no checkpointing ⇒ every delta-feed key hits `no_baseline` on its first update). Confirm NiFi tolerates the burst or rate-limit it
- [ ] Topic name `control-plane` is hardcoded in `TypeValidatorJob` while every other topic is env-derived, and duplicated in `scripts/warmup.sh` and `e2e/topics/topics.go` — same drift trap as `NORMALIZER_STAGES` ([[project_staleness_exporter]]). It also collides with the market-subscription HTTP API of the same name (`markets/README.md`)
- [ ] No unit test for `ControlCommandSerializer`, though the other four serdes each have one — the GenericRecord field names are still pinned only by a live run
- [ ] `TypeValidatorJob.java` was reformatted wholesale on the branch (8-space indent, mangled javadoc diagram) — 145 changed lines for ~20 of real change, and `96b2bf1` did the same to `TypeValidateFunction.java` (184 changed lines, ~8 real; every javadoc paragraph rewrapped to a narrower width). Revert both before merge

## e2e

- [ ] **A single 45-scenario pass in one process is not reliable on this machine** (2026-08-22) — `12-ex2-noise-frames` failed once mid-run with 9 dead-letters where it wants 0 (records leaking from scenario 11 on the same ex2/p1 topics), then from scenario 21 the JobManager crashed and every later scenario failed on `connection reset`. Disk was fine (48Gi free). 12 passed three times in isolation afterwards, and all 45 pass in batches of ~10. Either the cancel→delete→create→submit teardown has a race under sustained load or the JobManager leaks across ~20 submit cycles; worth finding before the suite is trusted as a gate

- [x] Schema registry warmup — load `schemas/*.avsc`, register each in the registry (2026-07-28, verified live: 4 subjects, ids 1–4)
- [x] Split into packages — `e2e/config/` and `e2e/schemaregistry/`, `main.go` is wiring only (2026-07-28, verified live)
- [x] Topic warmup — `e2e/topics/` creates the 9 topics for one exchange/pair via the Kafka admin API (2026-07-28, verified live: ex1/p1, retentions confirmed)
- [x] Flink jobs — `e2e/flink/` cancels running jobs, builds with `mvn`, submits the 6 job jars over the REST API (2026-07-28, verified live: all 6 RUNNING, re-run cancels then resubmits)
- [x] Send payloads to the kafka topics — `e2e/producer/` produces `payload.SourceData` to `ex{exchangeID}-raw` (2026-07-28, verified live: produced and read back off the broker)
- [x] Clean slate — `topics.Delete` wipes the 9 topics before `topics.Create` in `runTest` (2026-07-28, verified live on ex99/p99: delete → create → produce → delete → create leaves the topics empty)
- [x] Setup order — `flink.CancelJobs` exported out of `RunJobs`, so `runTest` is cancel → delete → create → submit (2026-07-28, verified live: `CancelJobs` runs standalone against the Flink API)
- [x] Warmup package — the cancel → delete → create → submit sequence moved out of `runTest` into `warmup.Run(ctx, cfg, exchangeID, pairID)` (2026-07-28, build/vet clean)
- [x] Stack provisioning — `e2e/stack/` recreates the compose stack (`down -v` + `up -d --wait`) as the first step of `runTest`; `RegisterDir` moved after it (2026-07-29, build/vet clean, not run live)
- [x] Verify the final book — `e2e/consumer/` reads the last record on `ex{id}-p{id}-orderbook-snapshot-flink` and `runTest` compares it to `TestPayload.WantedSnapshotData` (2026-07-29, decode verified offline against the real schema, whole run not tried live)
- [x] Multi-payload scenarios — `runTest` takes `[]TestPayload`, produces each `SourceData` in order and asserts snapshot `i` equals payload `i`'s `WantedSnapshotData` (2026-07-29, build/vet clean, not run live)
- [x] Rejections — `runTest` takes a `Scenario{Sources, WantSnapshots, WantRejects}` and checks the snapshot topic *and* `ex{id}-p{id}-rejected-flink` against their own wanted streams, so a run can expect N sources → M snapshots + K rejections + drops (2026-07-29, build/vet clean, not run live)
- [x] `main.go` refactor — ids folded into `Scenario`, scenario data out of `main()` into package-level vars, `runTest` split into `Scenario.produce` / `Scenario.verify` (2026-07-29, build/vet clean, not run live)
- [x] `e2e/scenario/` package — `runTest`/`compare`/`Scenario` moved out of package `main` into `scenario.Run(ctx, cfg, s)` (`scenario.go`) with the cases in `data.go` (`scenario.NobitexSnapshot`); `main.go` is now `main()` and nothing else (2026-07-29, build/vet clean, not run live)
- [x] All 17 scenarios (01–17, ex8/ex3/ex1/ex2) live in `scenario/data_ex1/2/3/8.go` — payloads verbatim, expected books derived by replaying jobs 3/4/5 over them (2026-07-29, build/vet clean, **none run live**)
- [x] `main.go` runs all 17 — a named list in directory order, each scenario warms up its own exchange, failures are collected and reported at the end instead of aborting the run (2026-07-29, build/vet clean, not run live)
- [x] ex3/wallex coverage — 4 scenarios added next to `Ex3WallexHalfBook` (`Ex3EmptySideWipe`, `Ex3PrecisionDust`, `Ex3NoiseFrames`, `Ex3StaleReplay`), all 5 wired into `main.go` as 11–15; none expects a rejection because ex3 cannot reach a dead-letter (2026-08-01, build/vet clean, not run live)
- [x] Job 3 + job 4 coverage — `Ex1PrecisionDust` / `Ex2PrecisionDust` (collision merge + dust delete) and `Ex1RebaseToman` (pair 52, IRR→toman `-1/0`) / `Ex1RebaseScaledUnit` (pair 17, scaled-unit `-6/+6` at precision 10/10); before these, every scenario ran on pair 1 where rebase is the identity, so job 3 was untested (2026-08-01, build/vet clean, not run live)
- [x] Job 6 coverage — `Scenario.WantAggregated` + `consumer.ReadAggregated` assert the final record on `p{id}-asks`/`p{id}-bids`; 4 scenarios opt in (2026-08-01, build/vet clean, not run live)
- [x] Job 6 on every scenario — a nil `WantAggregated` is now derived from the last wanted snapshot with `exchange_id` stamped (`Scenario.wantAggregated`/`stampExchange`) instead of skipping the topics, so all 41 scenarios assert job 5 AND job 6; the 12 explicit literals stay and were confirmed equal to the derivation; `swag init` rerun for the changed field doc (2026-08-02, build/vet/gofmt clean, not run live)
- [x] `WantAggregated` written out on all 41 — the 29 scenarios that leaned on the derivation now carry their own literal (generated from `wantAggregated()`, then re-checked against it by a throwaway test, PASS); the fallback stays for HTTP-posted scenarios, and new scenarios should carry a literal instead of relying on it (2026-08-02, build/vet/gofmt clean, not run live)
- [x] ex4/ex5/ex6 coverage — 16 scenarios in `scenario/data_ex4/5/6.go`, wired into `main.go` as 20–35: ramzinex (descending-sells re-sort, stale offset, noise, rebase toman on pair 2, rebase per-100-unit on pair 17), bitget (snapshots, multi-book record fan-out, stale seq, noise, precision dust), bybit (snapshot+deltas with a qty-"0" delete, one-sided deltas, sequence gap + reset, no_baseline, noise, precision dust) (2026-08-01, build/vet clean, not run live)
- [x] HTTP mode — `go run . -serve` provisions the stack once, then `POST /scenarios/run` takes a `scenario.Scenario` as JSON (snake_case tags added to `Scenario`/`AggregatedBook`) and answers `{status:"ok"|"failed", error, duration_ms}`; chi router in `e2e/server/`, runs serialized with `TryLock` (409 if busy) because each one recreates the topics and jobs (2026-08-02, HTTP layer smoke-verified — decode/400/409/failed-render — but no scenario run live)
- [x] Swagger — swaggo annotations on the handler + general info above `main()`, spec generated into `e2e/docs/` (committed, `server` imports it blank so the build needs it), UI on `GET /swagger/*` with the assets embedded; regenerate with `swag init -g main.go -o docs` after any change to the handler or `scenario.Scenario` (2026-08-02, doc.json/index.html/ui-bundle all verified 200)
- [x] Swagger worked example — `POST /scenarios/run` prefills "Try it out" with the real `scenario.Ex1PrecisionDust` case; swag has no schema-level example annotation, so `server.specJSON` injects it onto the `server.RunRequest` definition and serves it from a static `/swagger/doc.json` route ahead of the UI wildcard (2026-08-02, verified the served example round-trips through `DisallowUnknownFields` + `validate` back to `Ex1PrecisionDust`; UI rendering not checked in a browser)
- [x] `want_aggregated` over HTTP — already accepted and asserted (embedded `Scenario` field, so the json tag + swag definition + `verifyAggregated` covered it already, and the served example carries it since `Ex1PrecisionDust` got a literal); added a fail-fast `validate` rule (400 on an aggregated level with a non-positive `exchange_id` or empty price/quantity — the copy-a-snapshot-side mistake, which otherwise costs a full multi-minute run to discover) and rewrote the endpoint description so job 6 is documented as always-asserted; `swag init` rerun (2026-08-03, build/vet/gofmt clean with `-mod=mod`, throwaway test PASS on decode+validate, not run live)
- [x] `simulation` flag end to end — int on all 4 schemas and all 6 jobs, per-LEVEL on the aggregated topics (`exchange_id, simulation, price, quantity`); NiFi carries it as a root field on the six object-root exchanges and as a **trailing third array element** on ex3/wallex; every e2e example set to `simulation: 1` (177 payloads / 125 snapshots / 215 levels) and plumbed through `web/` to the WebSocket; `swag init` rerun (2026-08-03, 157 Java tests + all Go tests green, build/vet/gofmt clean, **not run live** — needs the 4 subjects re-registered and the jobs resubmitted first)
- [ ] Run the ported scenarios against the live stack — the expected books are derived, not observed
- [ ] Cross-exchange aggregation is still untested — every scenario feeds one exchange, so job 6 only ever unions one book. Needs a multi-exchange scenario shape (two raw topics warmed and fed into one pair), which must also set `WantAggregated` explicitly: the derived fallback assumes a union of one
- [ ] Decide on the `1K_SHIB*` / `1M_BTT*` rebase rows in `postgres/02_seed.sql` — they carry the IRR→toman shift but not the 1000×/1000000× unit shift the `1M_PEPE*` rows do
- [ ] Verify the four upstream steps (raw / type-validated / rebased / applied-precision) have their wanted values too
- [x] ~~No scenarios for ex7~~ — **ex7/ompfinex landed 2026-08-24** (teammate, `feat/add-ompfinex`): `OmpfinexParser` + six scenarios `49-ex7-*` … `54-ex7-*` in `data_ex7.go`, mirroring ex6's block. Reviewed the same day; Java 259 tests and all Go tests green, build/vet/gofmt clean. See [[project_pair_extractor]] § ex7 and [[project_e2e_harness]] § scenarios 49–54. **All six VERIFIED LIVE 2026-08-24 — 6 run, 6 passed, 0 failed** (⚠ the run needs the ex7 `exchange_markets` row present; a stack without the full seed fails all six with a misleading `got 0 records`). No scenarios for ex9 (no raw sample); ex8/okx still has 6 scenarios commented out in `main.go` under stale 01–06 numbering
- [ ] **`sample-raw-data.md` § ex7 holds NO captures** — the section now describes the wire shape, but transcribed from the parser's javadoc, not observed. Four claims ride on captures nobody else has seen: `data.time` is epoch MICROseconds, `U_n == u_{n-1}` (not Binance's `+1`), the first delta after a REST snapshot has `U == lastUpdateId`, and both side keys are always present. Add the real frames before ex7 is trusted
- [ ] **ex7's REST snapshot is SEQUENCED (`data.lastUpdateId`), the opposite of the ex6 call** — safe only if that counter is the WS `u` counter. Evidence is two consecutive samples, which establishes the `+0` convention but NOT the mid-stream resync case. If `U > lastUpdateId` when NiFi fetches a snapshot while deltas flow, ex7 enters the ex5 loop (gap → request → snapshot → gap). No e2e scenario can surface this — they all hand-build `U == prev seq`. **Verify against the live feed**
- [ ] **No `OmpfinexParserTest`, no `ex7-*.json` fixtures, and ex7 is absent from `SimulationFlagTest`/`RecordIdTest`** — the other seven parsers each have a test and fixtures, and those two convention tests enumerate ex1–6+8. The parser would pass them; nothing pins it
- [ ] **The micros÷1000 conversion is asserted nowhere** — every ex7 scenario sets `IgnoreEventTime` (correct: deltas carry no wire timestamp), which also blanks the REST snapshot's event time. A factor-1000 error leaves the suite green. Needs one snapshot-only scenario with the flag off
- [ ] **ex7 requires BOTH side keys**, unlike ex6/ex8 which pass a null side — a missing `a` or `b` drops the whole message via `dropped-unparseable`, i.e. an invisible sequence gap. Two `has()` checks would remove the failure mode; decide whether to match ex6 or keep the strict read
- [x] ~~The 44–48 → 50–54 renumber needs sweeping~~ — **REVERTED 2026-08-24**: ex7 moved to the tail as 49–54, the way scenario 48 deliberately did. 44–48 are back to their original numbers, so every "44/45 verified live" note in [[project_control_plane]] and this file is correct again and nothing needed sweeping. `scenarios.go`'s stale "46 is the sequenced guard, 47 the event-time one" comment — which the renumber itself had created — is correct as originally written. **The diff against main is now purely additive: 11 lines added, no existing entry touched.** Rule going forward: new scenarios go on the tail, grep the Go identifiers not the numbers
- [x] ~~`data_ex7.go`'s rebase justification is false~~ — **header corrected 2026-08-24**: it now records that ompfinex DOES have non-zero-rebase markets (market `"1"` = pair 2, `price_amount_rebase -1`, ~half the ex7 rows), and that the stale/duplicate reject_reason was never unconfirmed either — job 2's update branch rejects `sequence_id <= lastSeq` as `stale_or_duplicate`. Also noted the non-obvious bit: a genuinely REPLAYED ompfinex message cannot reach that branch, since it carries the old `U` and fails the `U == lastSeq` check first
- [ ] **ex7 still lacks the stale/duplicate and rebase scenarios** its peers have (ex8 41, ex1/ex2 05/13; ex1 07/08, ex4 23/24). Both are buildable now — the rebase one needs the ex7 pair-2 row present wherever the suite runs
- [x] ~~Stale "postponed ex7" comments~~ — **all fixed 2026-08-24**: `PairExtractorJob`, `PairExtractFunction` and `PairExtractFunctionTest` now name ex9 lbank as the drop-with-counter example, `Parsers.java`'s dangling javadoc sentence is repaired, and `Centrifugo.java` says four exchanges and flags ex7 as the only one of them with true delta semantics. 259 Java tests green. **Left alone deliberately**: `SimulationFlagTest`/`RecordIdTest`'s "seven parser tests" / "six object-root exchanges" wording is ACCURATE while ex7 has no test — it only goes stale when the ex7 fixtures land, and should be updated in that commit
- [ ] `data_ex1.go:2`, `data_ex2.go:2` and `data_ex8.go:3` still point at `data.go`, which no longer exists — only `data_ex3.go` was fixed
- [x] `simulation` verified live — `06-ex1-precision-dust` run against the local stack after `mvn clean package` passes end to end (2 snapshots, 0 rejections, both aggregated sides matched); the server's all-41 `Simulation:0` failure is stale job jars, not code. See [[project_e2e_harness]] on the missing `clean` (2026-08-03)
- [x] `e2e/flink/flink.go` `build()` now runs `mvn clean package -q -DskipTests` — a stale `target/` can no longer ship a pre-change jar, so nobody has to remember to clean by hand before a run. Left per-scenario rather than hoisted to once-per-process: a full clean reactor build is ~5s against a ~80s scenario (2026-08-03, build/vet/gofmt clean, `06-ex1-precision-dust` re-run live through the changed path and PASSED)

## lpa-staleness-exporter

- [x] Topic names realigned with `scripts/warmup.sh` — DB-derived list was building `ex{id}-p{id}-asks/-bids`, a naming warmup.sh has never created, so every DB-sourced series was a phantom pinned at `stale=1`. Now derives the five `ex{id}-p{id}-{stage}` normalizer stages (threshold from the row) plus `p{pair_id}-{asks,bids}` per distinct pair (new `output_threshold_seconds: 10`); `ex{id}-raw` stays manual. Dead-letter deliberately excluded — inverted semantics (2026-08-10, stage names verified string-equal to warmup.sh, logic exercised with stubbed psycopg2 over fake rows incl. NULL-threshold and DB-down paths, `py_compile` clean; **not run against live postgres or Kafka**)
- [ ] Update the external Grafana dashboards / `KafkaTopicStale` alert rule — the `topic=` label values changed and no Prometheus/Grafana config is committed in this repo, so nothing here will flag the mismatch
- [x] Renamed `kafka-staleness-exporter/` → `lpa-staleness-exporter/` via `git mv` (2026-08-10) — `docker-compose.yml` already pointed at the new path for both the build context and the config mount, so the service could not build until the folder matched
- [x] `topic_source: both|db|config` (2026-08-10, user request) — picks whether the watch list comes from postgres, the manual `topics:` block, or both merged (manual wins on a name collision, unchanged). **`db` ignores the `topics:` block entirely** (user decision), so the hand-listed `ex{id}-raw` topics are NOT monitored in that mode; `config` never opens a postgres connection *for the topic list* (as of 2026-08-11 it does still open one to persist episode history). An unrecognised value logs an error and falls back to `both` rather than killing the process — a monitoring component should not die on a config typo
- [x] Staleness *episode* metrics (2026-08-10, user request) — an episode is one continuous stretch of `stale=1`; exposes `kafka_topic_stale_since_timestamp_seconds` (0 when healthy), `..._last_recovery_timestamp_seconds`, `..._last_stale_duration_seconds` and counters `..._stale_episodes_total` / `..._stale_seconds_total`. In-memory by choice (resets on restart; `rate()`/`increase()` tolerate counter resets and the durable history is Prometheus's job). Every series is zero-initialised on first sight so a never-stale topic is still visible. Verified with a stubbed prometheus_client over a scripted healthy→stale→recover→stale timeline, all three `topic_source` modes incl. a bad value, the phantom-topic path and `drop_topic_metrics`; `py_compile` clean, **not run against live postgres or Kafka**
- [x] Episode history persisted to postgres (2026-08-11, user request) — the five Prometheus series only hold the *current* and *last completed* episode, so "which topic was down from when to when" was unanswerable. Every episode is now a row in `topic_staleness_episodes` (`topic, stale_since, recovered_at, duration_seconds, threshold_seconds`), created by `run_migrations` on every boot; an in-flight episode is recovered from the table at startup instead of being lost. **Written in every `topic_source` mode** — `config` still never queries postgres for the topic *list*, but the committed config *is* `config` mode, so gating persistence on it would have made the feature dead on arrival. Transition logic verified with psycopg2/kafka stubbed (8 assertions: the Decimal crash, drop-while-stale, drop-while-healthy, full healthy→stale→recover cycle), `py_compile` clean; **the SQL itself is NOT yet verified against a live postgres — the docker daemon was down; run `scratchpad/verify_sql.sh`, or just watch the exporter's startup log for `topic_staleness_episodes table ready`**
- [x] Three bugs fixed in that persistence, found in review before it ever ran (2026-08-11) — (1) `extract(epoch FROM stale_since)` returns `numeric` on PG>=14 (compose runs 18) → psycopg2 `Decimal` → `TypeError` when subtracted from a `time.time()` float; swallowed by the poll loop's `try/except`, so a restart-recovered topic would log `failed checking topic` forever and never recover or close its row. Now cast `::float8`. (2) `drop_topic_metrics` left the row open forever when a topic left the watch list — the panel would read it as still down, every restart resurrected it, and a re-added topic's new episode could never be closed past the orphan. It now closes the episode (so "dropped while stale" and "recovered" are indistinguishable in the table — the win is `recovered_at IS NULL` always meaning genuinely stale). (3) `idx_tse_open` was not UNIQUE, so duplicate open rows were possible and unrecoverable (`close_episode` only closes the newest); now `idx_tse_open_unique` + `ON CONFLICT DO NOTHING`, with the old index dropped by name so existing deployments upgrade
- [x] Grafana dashboard committed (2026-08-11, user request) — `lpa-staleness-exporter/grafana-dashboard.json`, the existing "LPA Staleness Monitoring" dashboard (uid `dfsv0halr3im8d`, version 4) plus a third section: **one repeated table per topic** (`repeat: topic` over a query variable) listing every episode as `Went stale at / Recovered at / Duration / Threshold / Status`. Filters by *overlap* rather than `$__timeFilter(stale_since)` so an outage that began before the window but is still running stays visible. JSON structure validated (no gridPos overlaps, unique ids, exactly one repeat); **not opened in a real Grafana**
- [ ] **Create the Grafana postgres datasource and replace `PLACEHOLDER_PG_UID`** (2 occurrences in the panel, 1 in the template variable) — host `postgres:5432`, db `markets`, user `postgres`, TLS/SSL mode `disable`. The dashboard's episode tables show "No data" until this exists
- [ ] `grafana-dashboard.json` is a hand-imported snapshot, not provisioning — it drifts silently the moment anyone edits the dashboard in the UI. Either wire up Grafana file provisioning or accept the copy is a point-in-time reference
- [ ] Episode metrics **and the postgres history** are only accurate to ±1 `poll_interval_seconds`, and a stale/recover cycle shorter than one interval is never recorded at all. **The interval was raised 10 → 30 on 2026-08-11 while `ex{id}-raw` thresholds are 5s and `output_threshold_seconds` is 10s — episode boundaries are now 3–6× coarser than the thresholds they describe.** Flagged to the user, left as-is; see the scaling item below, the two pull in opposite directions
- [ ] Serial poll loop may overrun `poll_interval_seconds` now that it is 5 topics per subscription instead of 2 — the interval was raised 15 → 60 (2026-08-10) which buys headroom; parallelizing instead needs one `kafka-python` consumer per worker (they are not thread-safe)
- [x] `id` / `source_ids` record lineage end to end — a fresh uuid minted by every step that writes to a topic, plus the ids it read. All 4 schemas, all 6 jobs, e2e and web. Four user decisions shaped it (2026-08-03): job 5's sources are every event still holding a resting level (so its MapState value became `RestingLevel{price,quantity,id}`), the aggregated record gets a per-record `id` + a per-LEVEL `source_id`, parents are immediate-only, and **job 1 DROPS a payload with no `id`**. Jobs 3/4 are no longer pass-throughs the way they are for `simulation`. 199 Java tests + all Go tests green; verified live on 5 scenarios covering the object root, ex3's array carrier, fan-out, a delta feed's real multi-source fan-in, and gap→reset. See [[project_record_lineage]]
- [x] `sink_id` renamed to `id` everywhere (2026-08-04, user request) — schemas, all 6 jobs, e2e and web. Pure rename, no behaviour change: `Lineage.newId()`, `get/setId()`, `RestingLevel.id`, the `dropped-no-id` counter, `SinkIdTest` → `RecordIdTest`, Go `ID`. `source_ids` and the per-level `source_id` keep their names. 202 Java tests + all Go tests green (not re-verified live — the previous live runs were against the old field name)
- [x] e2e source payloads carry literal `id`s (2026-08-04, user request) — all 177 now spell `"id": "<uuid>"` out next to `"simulation"`, each unique, in the carrier its parser reads (root field; ex3's index-2 metadata object). `stampID` no longer overwrites an existing id, it only fills one in for scenarios POSTed over HTTP; a present-but-blank id is now a hard error (splicing would duplicate the key and the blank would win). `TestStampIDOnEverySource` additionally asserts suite-wide uniqueness and that every fixture declares its own. Go build/vet/gofmt/tests clean, not run live
- [x] per-level `source_id` on job 5's snapshot (2026-08-08, user request) — each emitted `PriceLevel` now names the job-4 event that last SET that price, out of `RestingLevel` (`priceLevels()` had been dropping it). Fixes the one gap in the trace: the record-level `source_ids` is a deduplicated set with no mapping back to prices, so tracing ONE price dead-ended at "one of these N events" — the chain widened at job 5 where every other hop narrows. Job 6 deliberately unchanged (a level names the snapshot, the snapshot's level of that price names the event — each hop, one step). Only `order_book_snapshot.avsc`'s `PriceLevel` gets the field; `serde/PriceLevels` decides from the SCHEMA, since a `GenericRecordBuilder` throws on a field its schema lacks. 208 Java tests + all Go tests green, build/vet/gofmt clean, **not run live**
- [x] e2e `events` models realigned to the schemas field for field (2026-08-08, user request) — added the missing `last_sequence_id` (mirrored, not decoded: the snapshot stream is DeepEqual'd, so decoding it would put it in all 125 wanted snapshots) and `AggregatedSide.event_time` (decoded; only the levels are compared, so it is free). Replaced the wire-shaped timings types — probed against goavro and they were wrong three ways: union branches are keyed by FULL name (`io.tibobit.orderbook.PipelineTimings`), a logical-type union is keyed `long.timestamp-millis` not `long`, and the value is a number not a string; nothing caught it because nothing decodes into it. `StepTimings`/`AvroTime` deleted. swaggerignore now applied by one rule (never asserted + not scenario-influencable ⇒ out of the contract), so `swag init` dropped 143 lines. All 4 `_example.json` verified against their avsc; only the snapshot example was stale (per-level `source_id`)
- [x] snapshot `source_ids` → `trigger_id` (2026-08-08, user request, follow-on from the per-level `source_id`) — once every level names its owner the record-level array was exactly `{trigger} ∪ {level owners}`, so it added only the trigger and only positionally ("first element by contract"). Replaced by one string; `restingSources()` deleted. **It stays a separate field because it is NOT always among the level ids** — a delete-only event owns no level and a reset empties the book, and those records must still name what caused them. e2e's cross-check got stronger, not weaker: trigger ids must be distinct across the stream (one book per event) and every level's `source_id` must be the trigger of that record or an earlier one (a level cannot predate the event that set it). 207 Java tests + all Go tests green, swagger unchanged (both fields are swaggerignored), **not run live**
- [ ] Consider ASSERTING `last_sequence_id` in e2e — unlike event_time and the lineage it is deterministic from the sources, so it is genuinely checkable; the cost is spelling it out in all 125 wanted snapshots
- [ ] **Re-register `order-book-snapshot` — it is stale AGAIN** after the per-level `source_id` (2026-08-08), even if it was already re-registered for the first lineage change. Job 5's sink throws on the unknown field until it is
- [ ] **NiFi must inject `id` before these jars are deployed** — job 1 drops any payload without one, so a processor that is not updated loses 100% of its data, silently apart from the `dropped-no-id` counter. Root field for ex1/2/4/5/6/8; for ex3/wallex it goes in the SAME trailing object as `simulation` (index 2), never as a fourth element. **`sample-raw-data.md` now shows both fields on all 12 samples and states the drop rule — that is the doc to hand the NiFi team.** Not verified against a real NiFi flow — only against the harness's simulation of one
- [ ] Re-register all 4 schema subjects on the server and resubmit the jobs before the next run there — the same stale-subject trap as `pipeline_timings` and `simulation`
- [ ] `AggregatedOrderBookSerializer` has no unit test for `id`/`source_id` — the job-aggregator pom does not copy `aggregated_order_book_event.avsc` onto its test classpath the way `common`'s does, so those two fields are covered by e2e only
- [x] e2e logging on `log/slog` with levels (2026-08-08, user request) — all 33 `log.Printf` calls replaced; Info = scenario start + PASS/FAIL + once-per-process milestones, Debug = per-scenario internals (topics, schema registration, produce, Flink submit/cancel, consumer reads), Error/Warn = failures. New `-log-level` flag (default `info`). The actual noise was the subprocesses, not the log lines: `mvn` (once per scenario, 41×) and `docker compose` now buffer their output and replay it only on failure, streaming live only under debug; chi's `middleware.Logger` is mounted only under debug. stdlib slog was chosen over zap/zerolog because `e2e/` builds `-mod=mod` against a vendored tree it must never re-vendor. Build/vet/gofmt clean, `scenario` tests pass, **not run live**
- [x] **`reason` on the snapshot_request** (2026-08-22, user request) — the command now says WHY it was raised: `no_baseline` or `sequence_gap`, the same vocabulary as `reject_reason`. Avro field with `default: ""`, so v2 is a BACKWARD-compatible evolution (compat-checked against the live registry before registering; id 7). **No new state** — `resyncReason()` is `lastSeq == null ? NO_BASELINE : SEQUENCE_GAP`, the identical discriminator the reject reasons use, and `lastSeq` is frozen for the whole episode because a pending resync rejects every event; so a RETRY carries the reason that OPENED the episode, not the `awaiting_snapshot` its own trigger was dead-lettered with. e2e declares and compares it literally, and `validate` 400s any reason outside the two. 43 Java tests + all Go tests green, 2 unit mutations (hardcoded reason; retry reports its trigger's reason) + 1 live e2e mutation, verified live on 13 scenarios incl. the negative control, and the topic read directly to confirm the value is on the wire. See [[project_control_plane]]
- [ ] **Deploy order for the `reason` field**: re-register `control-command` and only then resubmit job 2 — `ControlCommandSerializer` fetches the write schema lazily on first use and holds it, so a job that is already running keeps v1 and throws on the unknown field. Same stale-subject trap as `order-book-snapshot`. `scripts/warmup.sh` needs no edit

## web

- [x] Per-exchange order book view (2026-08-10, user request) — one page, pair + exchange dropdowns: "All exchanges (aggregated)" renders job 6's union as before, any specific exchange renders job 5's own book for that pair from `ex{id}-p{id}-orderbook-snapshot-flink`. The two-sides-in-one-record vs one-record-per-side mismatch is absorbed entirely in `internal/schema` (`Decode` → `[]RawBook`, dispatching on the Avro FULL NAME rather than the topic, record-level `exchange_id`/`simulation` pushed down onto the levels), so nothing downstream sees two shapes. Three user decisions: TWO Kafka consumers (aggregated earliest, per-exchange **latest** — a full book per event × every exchange × pair; `ConsumeResetOffset` is client-wide, which is the only reason for two clients), the hub now pushes each client **only its selected pair+exchange** (`select` message, so `ServeWS`'s read loop finally carries meaning), and both dropdowns come from a postgres-derived `catalog` message listing **all** markets and exchanges (server-side filtering broke inferring the pair list from the data). Hub `latest` is keyed by `{pair,exchange,side}`, not by topic. All Go tests green, build/vet/gofmt clean, page + `/ws` upgrade + catalog frame verified locally; **not run against live Kafka**. See [[project_orderbook_web]]
- [x] **Merged order book view** (2026-08-24, user request) — a THIRD entry in the exchange dropdown, `All exchanges (merged)`, rendering [[project_price_merger]]'s `p{id}-{side}-merged` (quantities summed, one level per price). Two user decisions: the option lives in the exchange dropdown rather than as a separate Union/Merged toggle, and the Exchange column shows the **comma-joined contributing exchange names** rather than a count. Merged is a *view*, not an exchange, so it has no `exchange_id` — it takes the free negative slot `-1` (0 was already "aggregated", real ids start at 1), which left `Selection`/`Matches`/`WSSelect`/the hub/the browser's select message **completely unchanged**; the alternative (a `View` field threaded through all of them) costs several times the code and is the right move only if a fourth view appears. `Book.Merged` — not the nil `Exchange`, which is nil for the aggregated union too — is what `ExchangeID()` and therefore the `hub.latest` key branch on. **One consumer, not a third**: `aggregatedPattern` widened to `^p[0-9]+-(asks|bids)(-merged)?$` (same record rate, same `earliest` offset), the deliberate mirror image of `MergerJob`'s regex, which must *exclude* `-merged` or it eats its own output. The level shape difference is absorbed in `internal/schema` as before (third Avro full name `MergedOrderBookEvent`; `exchange_ids[]`/`source_ids[]` kept as LISTS, never flattened to a first exchange), and `registry.Enrich` resolves them into `Level.Exchanges` while leaving the scalar `Level.Exchange` at zero — resolving the absent id 0 would stamp every merged row `unknown`. All Go tests green (5 new: 2 decoder, 2 registry, 2 hub), build/vet/gofmt clean, page verified served locally with the new option; **not run against live Kafka — docker was down**. **`exchange_id` is now one ubiquitous language** (user follow-up, same day): `1+` = a real exchange, `domain.AggregatedExchangeID` = 0, `domain.MergedExchangeID` = -1, declared as one const block with one doc comment in `internal/domain/book.go` and mirrored as `AGGREGATED`/`MERGED` in `public/index.html`; no bare 0/-1 survives anywhere, including the tests and the `showExchange` check (now `=== AGGREGATED || === MERGED`, not the `<= 0` numeric trick). See [[project_orderbook_web]]
- [ ] **The Go constants and the JS ones are coupled by convention only** — `public/index.html` can't import from `internal/domain`, so a change to either side has to be made twice. Nothing enforces it; the alternative is serving the vocabulary in the `catalog` message, which is more machinery than three numbers deserve today
- [ ] **No merged record has ever been decoded by this app.** The merger job itself has also never run live ([[project_price_merger]]), so the whole `-merged` path — producer and consumer — is unit-verified only. First real test is a stack with the merger submitted (`make run-all-jobs`); the things to watch are the Schema Registry fetch of `merged-order-book-event` and whether the widened consumer regex actually picks the topics up (they are matched at subscribe time, so they must exist before the web app starts)
- [ ] Run it against the live stack — the per-exchange path has never seen a real `order-book-snapshot` record, only synthesised ones in `decoder_test.go`
- [ ] An idle exchange renders nothing until its next event (consequence of reading the per-exchange topics from `latest`). If that turns out to be a problem in practice, the fix is a bounded backfill on that consumer, not a switch back to `earliest`
- [ ] The pair dropdown lists every market in postgres, including ones with no `subscribe` row and therefore no topics — chosen deliberately; narrowing it is a `WHERE EXISTS` on the markets query if it becomes noise

## flink/normalizer (job 1 parsers)

- [x] **ex9/lbank LANDED — the last seeded-but-unparsed exchange is closed** (2026-08-26, user request, on four wire samples the user supplied). A teammate had committed `LBankParser` + one `Parsers` map entry on 2026-08-25 (`977e770`) and nothing else; this pass reviewed that parser and built everything around it. **The parser's LOGIC was not changed** — the review raised three questions and the user's answers confirmed all three of its existing choices: `TS` is UTC (keep `ZoneOffset.UTC`), do NOT whitelist the `type` field (shape check is enough), and equal timestamps stay ACCEPTED (job 2's guard remains `<`, not `<=`). **Regime: SNAPSHOT ONLY** — every frame is a whole book under `depth`, the platform's second such feed after ex3/wallex and the first that is snapshot-only *and* event-time-ordered for real (ex3's guard can never fire, since its clock is job-1 processing time). **`sequence_id = null`, jump 0 by user decision**: no counter exists anywhere on the wire, and `TS`-as-sequence was rejected to avoid the cadence trap that cost ex5 a live resync loop. Consequences: ex9 reaches exactly ONE reject reason (`out_of_order`) and can emit NO control command ever, `baselinePending` is set and never consumed (ex3's situation), and **NO job-2 change was needed** — the null-seq guard built for ex1 was inherited whole. Touched: `LBankParser` javadoc (behaviour untouched), `Parsers`/`PairExtractorJob`/`PairExtractFunction`/`PairExtractFunctionTest` stale "ex9 = the drop-with-counter example" comments, `TypeValidateFunction` javadoc (two null-seq exchanges → three), new `LBankParserTest` (6 cases) + fixture `ex9-snapshot.json`, ex9 rows in `SimulationFlagTest`/`RecordIdTest` (+ their stale "six object-root" / "seven parser tests" counts), `sample-raw-data.md` § ex9 + table row + carrier list, new `e2e/scenario/data_ex9.go` (5 scenarios) appended to `scenarios.go` as **55–59, no renumbering**. **269 Java tests green (was 259)**; e2e build/vet/test/gofmt clean. See [[project_pair_extractor]], [[project_e2e_harness]]
- [x] **ex9 scenarios 55–59 VERIFIED LIVE — 5 run, 5 passed, 0 failed** (2026-08-26, first attempt, 29–39 s each) against the local docker stack, run **without `stack.Provision`** because it does `docker compose down -v` and the stack held 17 h of live NiFi state. Unlike the ex7 first run this did NOT hit the trimmed-seed trap: postgres held exactly the ex9 slice (one exchange `9 lbank`, one row `9|btc_usdt → 1` rebase 0/0, `markets.id 1` precision 2/8), the `ex9-p1-*` topics existed and no jobs were running — cost of the run was the 2 records on `ex9-raw`. Proves end to end: the zone-less ISO-8601 `TS` → UTC conversion through all six jobs (no ex9 scenario blanks event time), snapshot-replaces-book with no qty-`"0"` anywhere, `out_of_order` firing on a replayed older book AND the key surviving it, equal-`TS` acceptance walking the book backwards, all eight drop paths incl. the wrong-case `BTC_USDT`, job 4's truncate → group → sum-raw → truncate-sum order, and ex9 asking the control plane for nothing. See [[project_e2e_harness]]
- [ ] **ex9 has no live MUTATION check** — the suite is proven to pass, not proven to bite. Scenario 31 got one; do the same here: shift a single `TS` and confirm `56` fails
- [ ] **`e2e/main.go` has no scenario filter**, so a single-exchange run means editing `main.go`/`scenarios.go` or (as on 2026-08-26) dropping a throwaway `package main` into the module. Every single-exchange run since ex5 has needed some variant of this — a `-only` flag is a few lines
- [ ] **⚠ `TS` being UTC is user-confirmed but unverifiable from the payload.** lbank documents `TS` as *server* time and the wire string carries no zone marker, so if it is really UTC+8 every ex9 `event_time` is 8 h in the future — invisible in the levels, and it would surface only as a staleness alarm. One `ZoneOffset` in `LBankParser` is the whole change. Settle it by comparing a live `TS` against the wall clock at capture time
- [ ] **⚠ ex9 frames are selected by SHAPE, not by `type`** (user decision). All four captures say `"type":"fdepth"` (futures depth) and the parser ignores the field, so lbank's spot `depth` channel would parse with no change — but so would any future channel that happens to carry `pair`, `TS` and a `depth` object. Cheap to close later with a two-value whitelist if lbank adds one
- [ ] **ex9 has no rebase scenario and no market with nonzero rebase factors seeded.** Every ex9 row in `02_seed.sql` is `0/0`, so the gap is in the seed, not the suite — unlike ex7, where the rebase scenario was buildable all along. Confirm against the server whether any lbank market is quoted in a rebased unit
- [ ] **`count` in the ex9 frame is unexplained.** All four captures say `"count":200` while `limit` is 50 and each side carries exactly 50 levels. Ignored by the parser either way; worth one line in `sample-raw-data.md` if anyone learns what it counts

- [x] **ex4/ramzinex data-flow double check** (2026-08-22, user request) — re-ran `RamzinexParser` against a second live capture (`orderbook:11`, offset 10298388, 50+50 levels). Everything matches `sample-raw-data.md § ex4`: numeric channel id → market key `"11"`, `buys`→bids / `sells`→asks, 7-element JSON-number levels with only `[price, qty]` read, exact decimal literals (`1398.2677`, `450`, no sci-notation), `seq = pub.offset` jump 0, type `snapshot`, event time = processing time. **BOTH sides price-descending re-confirmed** (best bid first `1910670`, best ask LAST `1910700`) — job 5 sorts asks ascending, so the descending wire order is handled. No code change needed. See [[project_pair_extractor]]
- [ ] **ramzinex market `11` is not in the local seed** (`postgres/02_seed.sql` has `12/2/13/3/643/…`, no `11`) — the sampled frame drops on `dropped-unknown-market` locally. Confirm the server row exists, and that `price_amount_rebase = -1` if pair 11 is IRT-quoted: the sampled prices (~1,910,670) are rial-scale, and a missing `-1` publishes prices 10× too high. Same local-seed-vs-server drift already flagged in [[project_db_schema]]
- [ ] `sample-raw-data.md § ex4` says level element 6 is "a small int (10–74 in this sample) — possibly order count". The second capture ranges 5–221 and tracks quantity magnitude (`0.23`→5, `1398.2677`→111, `10043.5`→221), so it is a depth-bar scale, not an order count. Field is ignored either way — this is a doc correction only
- [x] **ex5/bitget wire format changed** (2026-08-22, user-supplied capture) — the feed moved off the snapshot-only `books50` channel onto the price-GROUPED `depth` channel (`arg.params.scale: "0.01"`, `instType` `SPOT` → `sp`). Three changes at once: `action` gained `"update"` (a true delta feed, qty `"0"` = delete, confirmed on the wire), **`seq` and `pseq` vanished**, and a `checksum` appeared — a CRC book-integrity value, not monotonic, unusable as a sequence. Sequence id is now the inner string `ts` (also the event time), and since that is a millisecond CLOCK rather than a counter it needed a new schema field **`sequence_jump_tolerance`** (`default: 0`, BACKWARD-compatible): job 2's contiguity check is now the window `last + jump ± tol`, with **ex5 at 600 ± 10** and every other exchange at 0 (= the exact check, unchanged). Touched: both schemas + their `_example.json`, `RawOrderBookEvent` + raw serde (the rejected serializer delegates, so no change), `BitgetParser` rewritten, `TypeValidateFunction`, both ex5 fixtures, `BitgetParserTest`, the ex5 rows of `SimulationFlagTest`/`RecordIdTest`, 5 new job-2 tests, `data_ex5.go` rewritten, `scenarios.go` renumbered, `sample-raw-data.md § ex5`. All 6 job modules green; 2 mutations confirm the window tests bite. See [[project_pair_extractor]], [[project_type_validator]], [[project_avro_schema]]
- [ ] **⚠ Deploy order for the ex5 change**: re-register `raw-order-book-event` AND `rejected-order-book-event`, THEN resubmit the jobs — the serializer caches the write schema on first use. Same trap as `pipeline_timings` / `simulation` / `reason`. Note `scripts/warmup.sh` registers subjects by name while e2e registers every `schemas/*.avsc`, so a green e2e run proves nothing about `make warmup`
- [x] **RESOLVED 2026-08-23 by live measurement — the ±10 window applied to snapshot→update** (explicit user decision 2026-08-22, taken over a flagged objection). bitget's own capture has the first update **22 ms** behind the snapshot, nowhere near 600, so on the live feed that burst is dead-lettered `sequence_gap`, empties the book via the reset and asks the control plane for a snapshot — and if the resync snapshot is also followed by a close-behind update, ex5 can loop reset → request → snapshot → gap and stay dark. Pinned as an EXPECTED result in `TypeValidateFunctionTest.capturedBurstAfterSnapshotIsAGap`. **Settle against the live feed**: measure the real update→update cadence, and if the 22 ms burst is normal, exempt the snapshot→update hop by reusing the `baselinePending` bootstrap ex1/ex2 already use (the option offered and declined)
- [x] **ex5/bitget REST snapshot aligned** (2026-08-23, user-supplied capture `bitget-snapshot-apicall.txt`) — `ex5-raw` carries a SECOND stream, the same REST+WS split ex1/ex2 have. The two WS samples supplied alongside it matched the committed fixtures exactly, so only the REST body was new. It differs on every axis: market key = injected root `pair` (not `arg.instId`), `data` is a single OBJECT (not an array), sides are `a`/`b` (not `asks`/`bids`), and levels are JSON NUMBERS (not string pairs, so it reuses ex4's `Levels.fromNumericArrays`). **`action` is `"snapshot"` on BOTH streams so it cannot discriminate** — the parser branches on the shape of `data`, same trap as ex1/ex2. `requestTime` ignored, `code`/`msg` not inspected (an error body has no `data.a`/`data.b` and already fails the shape whitelist). **NO job-2 change was needed** — the REST body produces the same `RawOrderBookEvent` a WS snapshot does. Touched: `BitgetParser` (split into `parseRestSnapshot`/`parseWsFrames`), new fixture `ex5-rest-snapshot.json`, 2 new `BitgetParserTest` cases, ex5 rows of `SimulationFlagTest`/`RecordIdTest`, `Levels` javadoc, e2e `Ex5RestSnapshotResync` + `scenarios.go` renumbered again, `sample-raw-data.md § ex5`. All 231 normalizer tests green, e2e build/vet/test/gofmt clean, 2 parser mutations bite. **Scenario 31 verified LIVE against the running stack (PASS ~21 s) and live-mutation-checked**: shifting only the REST `data.ts` by −100 ms fails the run with an empty book at record 4. See [[project_pair_extractor]], [[project_e2e_harness]], [[project_control_plane]]
- [x] **RESOLVED 2026-08-23 by live measurement — the risk EXTENDED to the resync path** (user decision 2026-08-23, taken over the flagged alternative) — the REST snapshot is sequenced by its own `data.ts` at 600 ± 10, i.e. exactly like a WS snapshot, **not** by the null-seq `baselinePending` bootstrap ex1/ex2 give their REST bodies. So after every ex5 resync the next WS update must land 590–610 ms after the REST book's timestamp or it is dead-lettered `sequence_gap` immediately, re-emptying the book. Note the REST `ts` comes from a different endpoint and arrives at an arbitrary moment relative to the WS cadence, which is what makes this narrower than it looks. The deadlock fix means this degrades into a reset → request → snapshot → gap **request loop** rather than a silently dark market. **Settle it with the same live measurement as the item above**; the fix, if needed, is one line in `BitgetParser.parseRestSnapshot` (pass `null` as the sequence id and `0L` as the jump), and `31-ex5-rest-snapshot-resync` is the scenario that will fail first
- [x] **Audit: "a snapshot is ORDERED, never jump-checked", every exchange** (2026-08-23, user-stated invariant) — the user restated the rule platform-wide: an arriving snapshot (REST or WS) is validated on ordering alone (`sequence_id` newer than the last accepted), and `sequence_jump` must never be applied to it; gap detection has exactly TWO legal sites, snapshot → next update and update → update. **The code already conformed — no production behaviour changed.** Job 2 is the platform's only sequence validator (jobs 3–6 never validate `sequence_id`) and `getSequenceJump()` appears in ONE expression, inside the update branch; per-exchange, every snapshot lands in a branch that cannot reach it (ex1/2/3 null-seq → event-time order; ex4/5/6/8 → `seq > lastSeq` only). **What was missing was proof**: every pre-existing snapshot test used jump 0 — where `last + 0` is satisfied by nothing, so a leaked jump check is invisible — or hit `lastSeq == null` / `resyncPending()`. Added 4 tests on NONZERO jumps (48 → 52): `snapshotAfterUpdatesIgnoresTheJump`, `snapshotShortOfTheJumpIsStillAccepted`, `bitgetResyncSnapshotIsNotWindowChecked`, `snapshotOrderingBoundaryIgnoresTheJump`; 2 mutations bite (enforce the jump on snapshots → 6 failures; stop re-anchoring `lastSeq` → 7). Invariant now stated in the class javadoc and on the branch itself. ⚠ **This does NOT narrow the two ex5 open risks above** — the user's rule keeps snapshot → next update as a legal gap site, which is precisely the +22 ms transition they are about. See [[project_type_validator]]
- [x] **ex5 LIVE BUG FIXED: the resync loop** (2026-08-23, diagnosed on `tibobit-data-collector-afra` at the user's request) — `control-plane` held nothing but `snapshot_request/sequence_gap/ex5/p1` and `ex5-p1-rejected-flink` alternated `sequence_gap`/`awaiting_snapshot`. Measured 4569 `ex5-raw` frames over 36 min (BTCUSDT, the only ex5 market with `status=subscribe`): **(a)** the WS `depth` channel sends **NO snapshots** (3538 updates, 0), so the REST body is ex5's ONLY baseline — which contradicts the 2026-08-23 answer 'both exist, keep as-is'; **(b)** the REST `data.ts` is the endpoint's own clock — −706..+662 ms against the WS update before it, **behind 57%** of the time — and the update after it fell inside `600 ± 10` only **9.9%** of the time, so ~90% of resyncs gapped instantly and looped; **(c)** update→update is bimodal, a 575–625 mass plus a real **725–775 cluster**, only 93.2% inside `600 ± 10`. Fix (user chose widen over ordering-only): `BitgetParser` stamps the REST snapshot **null-seq / jump 0** (the ex1/ex2 `baselinePending` bootstrap) and the WS window is now **650 ± 110** = [540,760] = 99.83% of 3537 live transitions, with a missed tick (~1200 ms) still outside so gap detection survives. **No job-2 change** — both causes were job-1 stamping. Replaying the capture: **1030 resets + 1031 requests (28.6/min) → 4 + 5 (0.1/min)**, accepted 2641→3962. Touched `BitgetParser`, `BitgetParserTest`, the ex5 block of `TypeValidateFunctionTest` (incl. a new `bitgetRestResyncDoesNotSeedTheWindow` reproducing the loop and `bitgetStillDetectsAMissedTick`), `data_ex5.go` (`Ex5JumpTolerance` edges 540/760/761, `Ex5RestSnapshotResync` rebuilt around the live shape), `sample-raw-data.md § ex5`. All 236 normalizer tests + e2e build/vet/test green. See [[project_type_validator]], [[project_pair_extractor]]
- [ ] **⚠ NOT YET DEPLOYED OR VERIFIED LIVE.** Only `job-pair-extractor` changed behaviourally, so the deploy is: rebuild the jobs image and resubmit job 1 (no schema change, so no re-registration needed this time). Then re-check `control-plane` and `ex5-p1-rejected-flink` for a few minutes — expect ~0.1 requests/min instead of ~28. Job 2 keeps its `lastSeq` state across a job-1 restart (no checkpointing), but the first null-seq REST body self-corrects it within a couple of events
- [ ] **ex5's 650 ± 110 band is fitted to 36 minutes of one market.** Re-measure over a longer window and during a volatile session before trusting it. 4 transitions in the sample landed at 875–1149 ms and are indistinguishable from a genuinely missed tick; the `checksum` item below is the real answer
- [ ] ex5's `checksum` is bitget's intended divergence detector — a CRC32 over the top of the book, which is exactly what would catch a lost update on a feed that now has no contiguous counter. Not implemented (it spans job 5, which holds the book, and job 2, which asks for snapshots). Worth considering, since without it the only gap detection ex5 has is the timestamp window above
- [x] **ex6/bybit REST snapshot aligned** (2026-08-24, user-supplied captures `bybit-snapshot.txt`, `bybit-update.txt`, `bybit-snapshot-apicall.txt`) — `ex6-raw` carries a SECOND stream, the same REST+WS split ex1/ex2/ex5 have. **The two WS captures needed no change** (they matched `BybitParser` as written: `u` 210920912 → 210920913 jump 1, `seq` jump 10 and still unusable, event time `cts`), so only the REST body was new. Unlike every previous REST+WS exchange the **discriminator is easy**: the book is under `result` vs `data` and the WS frame has no `action` at all, so one `result`-is-an-object check suffices — do NOT copy ex5's shape-trap reasoning here. Market key = `result.s` (bybit's own symbol, so both ex6 streams derive the key identically; the injected root `pair` agrees and is redundant), both sides required, **string levels on BOTH streams** (no numeric hazard, unlike ex5), event time = `result.cts` (the same matching-engine field the WS branch reads, so one event-time clock across both streams). `result.ts` and top-level `time` ignored; `retCode`/`retMsg` not inspected (an error body answers `"result": {}`, no `a`/`b`, already fails the shape whitelist). **⚠ The REST snapshot is null-seq / jump 0, and this time the proof is ARITHMETIC rather than statistical: `result.u` is NOT on the WS counter** — the REST capture is **24.3 h LATER** than the WS pair yet its `u` is **171,928,550 LOWER** (38992362 vs 210920912), and a monotonic counter cannot run backwards. (Most plausibly the REST `updateId` is scoped per request depth rather than to the `orderbook.50` topic — **that explanation is unconfirmed; the incomparability is not.**) Adopting it re-creates the ex5 resync loop, here as an instant `stale_or_duplicate` since the REST value is the *smaller* one. **NO job-2 change** — the `baselinePending` bootstrap is exchange-agnostic and already existed. The capture also settled an open shape question: an unchanged side on a live delta is a present-but-**EMPTY** array (`"b": []`), harmless only because job 5 clears a side just when `type == snapshot`, so null and empty differ only on snapshots — the `data_ex6.go` header previously said "clears it" without that qualifier and was corrected. Touched: `BybitParser` (split into `parseRestSnapshot`/`parseWsFrame`), new fixture `ex6-rest-snapshot.json`, `BybitParserTest` 3 → 8 cases, ex6 rows of `SimulationFlagTest`/`RecordIdTest`, e2e `Ex6RestSnapshotResync` + `scenarios.go` (**appended as 48, no renumbering**), `data_ex6.go` header, `sample-raw-data.md § ex6`. All 126 job-1 + common tests green; e2e build/vet/gofmt clean. See [[project_pair_extractor]], [[project_e2e_harness]]
- [ ] **⚠ ex6 scenario 48 NOT RUN LIVE, and the parser change is NOT DEPLOYED.** The docker stack was down and the harness does a destructive `down -v`, so `48-ex6-rest-snapshot-resync` has never executed — the snapshot COUNT and the reset record's event time are reasoned from `Ex6SequenceGap`/`Ex5RestSnapshotResync`, not observed. Deploy is job-1 only (rebuild the jobs image, resubmit `job-pair-extractor`; no schema change, so no re-registration). Also worth a live mutation check, the way scenario 31 got one: stamp `result.u` as the sequence id and confirm 48 fails
- [ ] **Is NiFi actually publishing the ex6 REST snapshot yet?** The capture proves the shape exists but not that the flow is live for bybit. Unlike ex5 — whose WS `depth` channel turned out to send **no** snapshots, making REST its only baseline — ex6's WS feed does send real snapshots, so ex6 is not one-legged if the REST stream is absent. Worth confirming rather than assuming, since it decides whether scenario 48 is covering a live path or a latent one
- [ ] **ex6 `u` may not be per-`(market, depth)` the way the parser assumes.** The null-seq decision makes the REST counter moot, but the WS side still keys gap detection on `data.u` with jump 1 — and the only evidence that the `orderbook.50` counter is scoped per market is that consecutive frames for one market increment by 1. If bybit ever changes the subscribed depth (`orderbook.50` → another), the counter changes with it and every market gaps once. Cheap to survive, not currently detected
- [ ] `sample-raw-data.md § ex5` says the snapshot's `checksum` is `0` — that is what the captured frame carried, but a real bitget snapshot normally carries a real CRC. Re-verify against a second capture before relying on it

## flink/merger (price-merged order book)

- [x] Cross-exchange **price merging** (2026-08-11, user request) — new standalone Flink project `flink/merger/`, deliberately **outside `flink/normalizer/`**: it is not a stage of the raw pipeline, it reads that pipeline's finished output. Consumes job 6's `p{id}-{side}` and sums equal prices across exchanges onto `p{id}-{side}-merged` (new subject `merged-order-book-event`), each level carrying `exchange_ids[]` + `source_ids[]` — the ids that created it. **A parallel view, not a replacement**: job 6's union-never-sum is a pinned business decision and is untouched, both topics are live and consumers pick. Four user decisions: input is **job 6's output, not job 5's per-exchange books** (one Kafka hop of latency buys a *stateless* job — one aggregated record already IS the whole book for that pair+side, so no MapState, no splitter, no reset handling, versus ~300 duplicated lines of `job-aggregator`); grouping is by **(price, simulation)** so simulated depth is never summed into a live number; the level keeps `exchange_ids` positionally aligned with `source_ids`; naming is the `-merged` suffix. Single Maven module, self-contained — duplicates `AvroSchemaLoader` + `Decimals.canonicalize` rather than depending on `normalizer-common`, which is never installed to a repository and would impose a build-order coupling. Runs on the **same image and cluster** (slots 6 → 7 of 8). 16 tests green (12 merge + 4 serde incl. a real Avro binary round-trip), jar packages with the right Main-Class, `warmup.sh` registers the subject and creates the topics; **NOT run live — no stack was up.** See [[project_price_merger]]
- [ ] **Never run live.** No smoke test and no e2e scenario — the merge and both serdes are unit-verified only, and nothing has exercised the Schema Registry fetch, the anchored topic-pattern subscription, or the sink against real Kafka
- [x] **Wired into deployment** (2026-08-11) — `make run-all-jobs` cancels and submits `ALL_JOBS` = merger + the 6 normalizer jobs, merger first because it is downstream of job 6 and every source reads `latest`. `refresh-normalizer` / `run-normalizer-jobs` were left normalizer-only, so **a refresh leaves the merger down** — decide whether that is right
- [ ] **One `flink/run-job.sh` for every project** (2026-08-11, user request) — replaced the two near-identical per-project scripts and both per-project Makefiles (now one `flink/Makefile`, arg is `JOB=` not `MODULE=`). Jobs are discovered by scanning poms for a shade `<mainClass>`, which is also exactly what makes a jar submittable — so `common` is excluded for free and a new job needs no script edit. Verified: discovery lists all 7, `common` and unknown names are rejected, both build paths produce the expected jar. **Submission itself is unverified — no cluster was up.** See [[project_flink_deploy_tooling]]

- [x] **`flink/run-local.sh <job>`** (2026-08-18, user request) — runs one job as a plain JVM process on an in-process MiniCluster: no Flink image, no cluster, no jar upload, with Kafka and postgres still on docker. Job discovery moved to `flink/job-discovery.sh` and is sourced by both scripts rather than copied. The work was not the script but two pom holes that only surface off-cluster, because `provided` deps are neither shaded nor transitive: `flink-connector-base` (bundled in flink-dist on the cluster, declared by nobody) and per-module resolution of `/opt/flink/lib` — `job-rebaser` reaches avro only through `normalizer-common` and died on `GenericRecord`, so the normalizer parent now declares the image's lib set for every module (**keep it in step with `flink/normalizer/Dockerfile`**). `flink-clients` added test-scope for the `LocalExecutorFactory`. Verified: `merger` and `job-rebaser` both reach a running MiniCluster against a deliberately dead broker (nothing written to the live stack), 181 tests green, shaded jars still carry zero flink/avro classes. **Not run locally against live Kafka.** See [[project_flink_deploy_tooling]]
- [x] **Java debug mode for local jobs** (2026-08-19, user request) — `DEBUG=1 ./run-local.sh <job>` adds a JDWP agent (`suspend=y`, port 5005, `DEBUG_PORT` to override) to the final `java` call only; `.vscode/launch.json` gained an "Attach to run-local.sh" config. The flag lives on the script rather than `JAVA_TOOL_OPTIONS` because the latter is inherited by the `mvn` build in the same script and hangs it. Verified the JVM suspends and holds 5005 in LISTEN. **Superseded the same day** by native F5 debugging: `flink-clients` moved `test` → `provided` in both poms (what the comments already claimed — the cluster supplies it — and invisible to shade either way, but only `provided` is on the classpath the IDE hands a `src/main` class), and `.vscode/launch.json` now has one `flink: <job>` config per job with env from `.vscode/flink-local.env`. Verified on a compile+provided classpath (a subset of the IDE's): MiniCluster starts, only the deliberate dead-endpoint failure remains; jar still has zero flink classes; 111 + 16 tests green. **F5 itself was not pressed — untested inside VS Code.** Also flipped `.vscode/settings.json`'s `java.configuration.updateBuildConfiguration` to `automatic`: at `interactive` JDT never re-read the changed pom, so the first retry failed identically on an unchanged classpath. See [[project_flink_deploy_tooling]]
- [ ] `kafka-staleness-exporter` does not watch the `-merged` topics; adding them is a third family in the DB-derived list, and the `NORMALIZER_STAGES`-style duplication between it and `warmup.sh` has nothing enforcing it
- [ ] No e2e coverage — `e2e/` asserts jobs 5 and 6 only. A merged assertion would be the natural place to finally prove the multi-exchange case, which [[project_e2e_harness]] already flags as untested (every scenario feeds one exchange, so job 6 only ever unions one book — and a merger fed a one-exchange union sums nothing)
- [x] **The web UI consumes `-merged`** (2026-08-24) — see the `## web` section
- [ ] Output `price` is canonicalized (`10.00` → `10`) because merging must compare numerically — so a merged level's price *string* can differ from the same price on `p{id}-{side}`. Harmless for numeric consumers, worth knowing for anyone diffing the two topics by string

## flink (production readiness)

Reviewed 2026-08-26, scope agreed with the user 2026-08-29. **The agreed list is COMPLETE — M1, M7,
M2a, M2b, S5, M4, M3, S4, M9 and S7 all applied; M8 dropped. Nothing deployed or observed running.**
**2026-08-31: the hardening was then moved out of `docker-compose.yml` into a separate
`docker-compose.prod.yml` and the dev file restored — see "Dev/prod compose split" below.** Full report in
[[project_flink_production]] — every item below has its config block there under the same ref.

**Two standing decisions (2026-08-29):**

- **Checkpointing POSTPONED** (not rejected). Flink has no cross-job checkpoint coordination, so a
  crashed upstream job replays into a downstream job that never restarted and **silently reverts
  price levels** — verified: `BookBuildFunction` reads `sequence_id` only to stamp output (line 93),
  and job 2, the only validator, is four stages upstream. Preference is to restart from `latest` and
  re-baseline from a snapshot rather than resume from a possibly-wrong state. Parked with it:
  **M5** (`.uid()`), **S2** (`max-parallelism`), **M6** (checkpoint volumes), **S6** (savepoint
  deploys), **L4**. Revisit routes are in the report's section 03
  - **⚠ UN-POSTPONED 2026-08-31 on `feat/checkpointing`.** Checkpointing landed (all 6 normalizer
    jobs, `EXACTLY_ONCE`) and the user's call was to accept the cross-job replay risk above for now
    rather than block on one of the three routes. See [[project_flink_production]]'s dated section for
    what landed and what this review fixed (`isolation.level=read_committed`, a shared checkpoint
    volume, and reverting a restart-strategy override back to M1's cluster policy). **M5/S2/M6/S6/L4
    are still NOT done** — they were not part of this pass and remain open below.
  - **⚠ Live symptom 2026-08-31: all 6 jobs stuck `RESTARTING` after this landed.** Root cause found —
    the new checkpoint volume came up root-owned (Dockerfile never created `/opt/flink/checkpoints`
    before `USER flink`), so every checkpoint write failed and Flink's default 0-tolerance failed the
    job outright. Dockerfile fixed to pre-create it with `chown flink:flink`; the user's *existing*
    volume still needs a one-off `chown -R flink:flink` (command given, not yet confirmed run/fixed —
    verify before treating this as closed). Details in [[project_flink_production]]
- **NiFi out of scope** — not managed from our side, development-only in this compose file. Do not
  change it and do not propose changes to it
- **S1 CLOSED** — keep `OffsetsInitializer.latest()`. Downstream-first submission ordering in the
  root Makefile therefore stays load-bearing

**⚠ Job count is 8, not 7** (corrected 2026-08-29 after merging `origin/main`): `ALL_JOBS :=
adjustment merger $(NORMALIZER_JOBS)`. The cluster runs **8/8 task slots, completely full**, at
parallelism 1 each. The report was written against 7 and its slot arithmetic and job-count alert are
corrected below — **[[project_flink_production]] still says 7 in places and needs the same pass.**

### Apply in this order, one at a time

- [x] **M1 — explicit restart strategy** (jobmanager `FLINK_PROPERTIES`, 5 lines). `disable` is the
      default *only because* checkpointing is off; setting the key overrides it. **Biggest single
      win and it does not wait on the checkpointing decision** — today the first exception in any job
      kills it permanently. Backoff deliberately longer than Flink's defaults (10 s → 5 min) because
      with no checkpoints every restart costs a full resync. **APPLIED 2026-08-29.**
      ⚠ **Verification still owed: kill a job and watch it come back.** Not observed
- [x] **M7 — `restart: unless-stopped` + a `logging:` block** on `jobmanager` and every
      `taskmanager` (they have none today; logs grow unbounded). Trivial, no behaviour change.
      **APPLIED 2026-08-29** — json-file, 100m x 5, on the JM and all four TMs only
- [x] **M2a — set TaskManager memory on the existing single TM.** Image default is `1728m` ⇒ roughly
      **384 MB of task heap shared by all 8 jobs**. Set `process.size`, `managed.fraction: 0.1`, and
      `env.java.opts.taskmanager` for a heap dump on OOM. ⚠ **never set `env.java.opts.all`** — the
      image uses that key for the Java 21 `--add-opens` list. Verify: read the TM startup log's own
      memory breakdown and the JVM options line. **APPLIED 2026-08-29.** ⚠ **Verification still
      owed: read the TM startup log and confirm both the `--add-opens` list AND the heap-dump flags
      appear in the JVM options line** — if `--add-opens` is missing, the key was set wrong
- [x] **M2b — split into 4 TaskManagers × 3 slots** (from 1 × 8). ⚠ **revised from 4 × 2**: with 8
      jobs, 4 × 2 = 8 slots is exactly full and leaves no room for a 9th job or any parallelism
      increase — the situation the adjustment note already flagged. 4 × 3 = 12 keeps ~2 jobs per JVM
      (the blast-radius point) with 4 slots spare. ⚠ container `deploy.resources.limits.memory` must
      **exceed** `process.size` or the kernel OOM-killer masks the failure. Size to the real box —
      host RAM was never inspected. **APPLIED 2026-08-29** as `taskmanager-1..4`, 9g container
      limits, host metrics ports 9250-9253; the commented-out `taskmanager-2` block is gone.
      ⚠ **BLOCKER BEFORE DEPLOY: 4 × 8g + the JM is ~34g of Flink alone and host RAM is still
      uninspected.** Check free RAM on the box; if it does not fit, scale `process.size` and the
      container limit together. **2026-08-31: this now applies to the prod box only** — the compose
      split put this sizing in `docker-compose.prod.yml` and gave dev back one unbounded TaskManager,
      which removes the "must run on 5+ dev/test envs" half of the problem but not the prod half. ⚠ **Verification owed: 12 free slots, and the 8 jobs spread across
      four TMs rather than piling onto one**
- [ ] **Fallout from M2b, already partly handled.** `flink/run-job.sh` hard-coded
      `docker logs ... taskmanager` in 3 places — **fixed in the same change** with a
      `TASKMANAGERS=(...)` array and a `tm_logs` helper that reads all four, then **replaced
      2026-08-31 by runtime discovery** when the compose split gave dev back its single
      `taskmanager`. Still open: [[flink-deploy-tooling]]'s slot-leak procedure
      (`docker compose restart taskmanager`, `freeSlots: 8`) is stale — now flagged in that file,
      but not re-tested across four TMs, and the container name it names is now dev-only
- [x] ~~**Loose end — `jobmanager.memory.process.size: 2g`**~~ — decided with M3, see below
- [x] **S5 — TaskManager healthcheck** (`curl :9250/metrics`). They have none, so a hung JVM stays
      "up" forever. **APPLIED 2026-08-29** to all four TM blocks (20s/5s/3, 40s start period).
      ⚠ **Visibility, not recovery** — Docker does not restart a container for going unhealthy, so
      a hung JVM now *reports* unhealthy and keeps running. M9 is what makes this actionable
- [x] **M4 — stop silent sink loss via producer config.** ⚠ `AT_LEAST_ONCE` is **inert** without
      checkpointing (it flushes *on checkpoint* — the docs' own wording), so the fix is
      `acks=all` + `enable.idempotence=true` + `retries` + `delivery.timeout.ms` via
      `setProperty` on every `KafkaSink`, job 2's dead-letter sink included. Idempotence is the
      important one: plain retries can **reorder** writes, which is the same class of bug as the
      replay problem above. Verified `setKafkaProducerConfig`/`setProperty` exist on
      `KafkaSinkBuilder` (connector 5.0.0-2.2). **Now 8 jobs to touch, including `adjustment`**.
      **APPLIED 2026-08-29** to **11 sink sites across 8 files** — not 8: RebaserJob has a
      `rejected` sink and TypeValidatorJob has `rejected` **and** `controlCommands` too. Applied
      inline rather than via a shared helper because `merger`/`adjustment` do not depend on
      `normalizer/common` and centralising would couple three independently-built projects.
      All three projects compile; **144 tests pass** (105 + 16 + 23).
      ⚠ **Does NOT close everything**: `DeliveryGuarantee` is still `NONE`, so records buffered in
      the producer when a TM JVM dies are still lost, and idempotence is per-producer-session so a
      restart can still duplicate at the seam.
      ⚠ **STALE AS OF 2026-08-31 — re-open this.** M4 was chosen *because* `AT_LEAST_ONCE` is inert
      without checkpointing (it flushes on checkpoint). Checkpointing now exists, so that reasoning
      no longer holds: `AT_LEAST_ONCE` would now actually flush, and it is the piece that would
      close the buffered-record loss above. Not changed here — it is a code change across 11 sink
      sites and was not part of resolving this merge. **Decide deliberately.** See **S8** — single-broker RF=1 means `acks=all` is
      "the one replica that exists".
      ⚠ **Verification owed: no test in the repo exercises sink configuration.** The 144 passing
      tests prove nothing broke, not that M4 works — read `ProducerConfig` in the TM startup log
- [x] ~~**M8 — bind 7070 to the private interface**~~ — **DROPPED 2026-08-31 (user).**
      `docker-compose.yml` runs on **5+ dev and test environments** and must stay host-agnostic. A
      literal `192.168.150.104` does not just fail to be portable — the container **fails to start**
      on any host that does not own that address (`bind: cannot assign requested address`), and
      `127.0.0.1` breaks reaching the UI from a laptop against a shared test box. Applied and
      reverted the same day; the port is back to `"7070:8081"`.
      ⚠ **The risk is unmitigated, not resolved.** Flink's REST API still has **no auth at all** —
      anyone who can reach 7070 uploads a jar and runs code next to Kafka/Postgres.
      `web.submit.enable: false` is NOT the fix (`run-job.sh` deploys through that endpoint). The
      control has to live outside this file: a host firewall, or a `docker-compose.override.yml` on
      the box that actually needs a restricted bind.
      ⚠ **Binds M3 and M9 too** — no hard-coded ZooKeeper address, scrape targets or root URL
- [x] **M3 — JobManager HA via ZooKeeper** + new `zookeeper` service + `data-collector-flink-ha`
      volume. Today a JM restart returns an **empty cluster**. Consistent with the postponement: no
      checkpoint to restore, so recovered jobs start at `latest` and re-baseline.
      **APPLIED 2026-08-31.** `zookeeper:3.9`, four `high-availability.*` keys, 3 new volumes
      (`flink-ha`, `zk-data`, `zk-datalog`). Pure insertion, 125 lines. Host-agnostic, so it clears
      the 5+ env constraint. Two deviations from the report, both reasoned not observed:
      **HA keys are on the four TaskManagers too** (leader fencing rejects a TM left on `NONE`) and
      **`flink-ha` is mounted on the TMs too** (it doubles as the blob store). Healthcheck is
      `zkServer.sh status`, not `echo ruok | nc` — `nc` presence in the image is unverified.
      ⚠ **verify by killing the JobManager** — that HA recovery works cleanly with no checkpoints is
      the entire value of this item and has not been observed. First check on `up`: **12 registered
      slots**. If TaskManagers fail to register, the TM-side HA keys are suspect #1
- [ ] **New hazard created by M3 — resubmission now duplicates.** Once HA recovers the 8 jobs
      unattended, a human running `make run-all-jobs` out of habit gets **16 running jobs**, all
      consuming and producing. `run-job.sh` does not check for an already-running job of the same
      name. Either make it check `/jobs` first, or document the check in the runbook
- [ ] **New gap created by M3 — recovery ignores submission order.** HA restores all 8 job graphs
      at once in no particular order, but sources are `OffsetsInitializer.latest()` and
      downstream-first ordering is load-bearing (S1). A recovered upstream job can emit before its
      downstream consumer is running and those records are gone. Inside the accepted re-baseline
      semantics, but this is a **new, automatic, unattended** place that semantic fires
- [x] **Loose end CLOSED 2026-08-31 — `jobmanager.memory.process.size: 2g` NOT applied.**
      Unattributed in the report, belongs to no M-item, and nothing shows the image's 1600m default
      is short; M3 adds only a ZK client and the job-graph store to the JM. With the file running on
      5+ envs, raising a memory floor without measurement is the wrong direction
- [x] **S4 — HistoryServer** (`jobmanager.archive.fs.dir` + a history-server service). Without it a
      JM restart takes every failed job's exception with it. Works without checkpointing.
      **APPLIED 2026-08-31.** New `historyserver` service on the same `./flink/normalizer` build,
      `data-collector-flink-archive` volume shared with the JM, 57 lines, pure insertion.
      Deviations: **host port 7071** (8082 is schema-registry, 8081 is NiFi; container stays 8082),
      **no `depends_on: jobmanager`** (the failure it exists for *is* a dead JM) and therefore
      **none of the M3 HA keys**, and `historyserver.web.address: 0.0.0.0` pinned rather than
      trusted to default.
      ⚠ **Only *terminal* jobs are archived.** A job stuck in M1's restart loop is running, not
      terminal, so it never shows up here however often it has failed — that is M9's `numRestarts`,
      not S4
- [x] **BUG found and fixed 2026-08-31 while applying S4 — the HA and archive volumes were
      unwritable, which silently broke M3.** Docker creates a missing mount destination as
      `root:root`; the Flink image runs as **uid 9999**. Neither `/opt/flink/ha` nor
      `/opt/flink/archive` exists in `flink:2.2.0-scala_2.12-java21`, so both named volumes came up
      root-owned and the JobManager could not write either. **Verified by probe, not reasoned** —
      `touch` into a fresh volume gave `Permission denied`; the same probe against an image that
      pre-creates and chowns them gave `WRITE OK`. Fix is 4 lines appended to
      `flink/normalizer/Dockerfile`. `/opt/flink/log` was never affected because it already exists
      in the image, which is why M7's log volumes worked
- [x] **Deploy note created by that fix — the next deploy MUST rebuild the Flink image.**
      `docker compose up -d` on its own reuses the old image and M3's HA has nowhere to write, with
      no obvious error at `up` time. **CLOSED 2026-08-31** — `make prod-up` and `make prod-deploy`
      both pass `--build` unconditionally, and both compose headers say why
- [x] **M9 — Prometheus + Alertmanager + Grafana.** `monitoring/` holds the config; three services
      added to compose (Prometheus **9090**, Alertmanager **9093**, Grafana **3001** — 3000 is
      `web`). Ten rules in `monitoring/prometheus/rules/flink.yml`. Every scrape target is a compose
      service name, so it stays host-agnostic. **Metric names came from a live scrape**, not from
      docs: a throwaway `flink:2.2.0` JM+TM running the bundled `TopSpeedWindowing` example, then a
      real Prometheus pointed at it — all ten rules evaluated `health: "ok"`, and `FlinkJobCountWrong`
      / `FlinkTaskManagersMissing` were confirmed *firing* with correctly rendered annotations.
      Deviations and gaps, all four deliberate:
      - **Added `FlinkJobManagerDown` (`up == 0`)**, not in the report's table. Without it a dead
        JobManager is the *quietest* failure of the set: `numRunningJobs != 8` cannot fire because
        the series is gone. It inhibits the derived alerts in Alertmanager
      - **Split "numRestarts rising" into two** — any restart in 15 min (warning) and >5 in an hour
        (restart loop, critical). M1 turned a poison record from a dead job into a permanently
        restarting one, which stays `RUNNING`, never reaches the HistoryServer, and is invisible to
        every other rule here
      - **Back-pressure uses `backPressuredTimeMsPerSecond`, not the `isBackPressured` gauge** the
        report names. The gauge is a 0/1 instant sample that a 15 s scrape mostly misses; the other
        is time accumulated between scrapes
      - **⚠ One unverified metric name: `records_lag_max`.** The probe cluster had no Kafka source,
        so `flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max` is
        from the documented convention. If it is wrong the rule is *silently dead* — it never errors,
        it just never fires. See the deploy check below
      - **Alertmanager delivers nowhere**: no Slack webhook or SMTP host has been agreed, so the
        default receiver is empty on purpose rather than pointed at a placeholder. Alerts are still
        visible on its UI and Prometheus's `/alerts`. Grouping + two inhibit rules are in place, so
        adding a receiver is a one-block edit
      - **The two parked checkpoint alerts were un-parked 2026-08-31**, when checkpointing landed,
        and a third added. `flink-checkpoints` group: `FlinkCheckpointsFailing` (critical — Flink's
        tolerable-failed-checkpoints is 0, so one failure fails the job), `FlinkCheckpointsNotCompleting`
        (a *stuck* checkpoint increments no failure counter, so the first rule stays silent forever)
        and `FlinkCheckpointDurationHigh` (half the configured 120s timeout). Alertmanager gained a
        third inhibit rule so the cause, not the restart symptom, is what pages. **This is the exact
        failure the first live checkpointing run hit** — 6 jobs looping on a root-owned volume, job
        count still 8, every existing rule silent. 13 rules now; `promtool check rules` passes
      - **Grafana ships the Prometheus datasource provisioned but no dashboards** — none have been
        authored, and an empty dashboards provider is just another directory to keep in sync
      - **Not validated:** `alertmanager.yml` parses as YAML and matches Alertmanager's schema by
        inspection, but no `prom/alertmanager` image is available locally, so `amtool check-config`
        has **not** been run. Prometheus's own config and rules *were* validated with `promtool`
- [ ] **Deploy check for M9 — confirm the Kafka lag metric name on the first real deploy.**
      `curl -s localhost:9250/metrics | grep -i records_lag_max`. If the name differs, fix
      `FlinkKafkaSourceLag` in `monitoring/prometheus/rules/flink.yml`; a wrong name is silent
- [ ] **Deploy check for M9 — `prom/alertmanager:v0.27.0` and `grafana/grafana:11.1.0` are not
      cached locally** and need a pull. `prom/prometheus:v2.53.0` is already present
- [ ] **Deploy check for the checkpoint alerts — confirm the three metric names.**
      `curl -s localhost:9249/metrics | grep -i checkpoint`. They are the only names in
      `monitoring/prometheus/rules/flink.yml` NOT taken from a live scrape — the probe cluster
      predated checkpointing — so `numberOfFailedCheckpoints`, `numberOfCompletedCheckpoints` and
      `lastCheckpointDuration` are from the documented convention. A wrong name is **silent**
- [ ] **Deploy check for S7 — `docker pull provectuslabs/kafka-ui:v0.7.2`** before the first prod
      deploy. It is the one pin that could not be read off a cached image; a wrong tag stops `up`
      with `manifest unknown`
- [x] **S7 — pin `kafka-ui`/`redis_exporter`/`kafka-exporter` off `:latest`**, and stamp the git SHA
      into the job jars — every jar is `1.0-SNAPSHOT`, so nothing on a running cluster says which
      commit it is. **APPLIED 2026-08-31.**
      **Pins** (in `docker-compose.prod.yml` only — the dev file is allowed to float):
      `redis_exporter:v1.86.0` and `kafka-exporter:v1.9.0` were **read off the locally cached
      images**, not guessed — the OCI `org.opencontainers.image.version` label and the binary's own
      `--version`. ⚠ **`kafka-ui:v0.7.2` is the one unverified pin**: that image carries no version
      label and no build-info, so the tag is from provectuslabs' last release before the project
      moved to kafbat. `docker pull` it before the first deploy. A wrong tag fails **loudly** at
      `up` (manifest unknown), so unlike the M9 metric name it cannot be silently wrong
      **SHA stamping**, two places, because one alone was not enough:
      - `Git-Commit` in each shaded jar's manifest, via a `${git.sha}` property (defaulting to
        `unknown`) declared in the three build roots and a `<manifestEntries>` block in all 8
        module shade transformers. Verified by building: `-Dgit.sha=testsha123` produced
        `Git-Commit: testsha123`, a plain `mvn package` produced `Git-Commit: unknown`
      - the **filename the jar is uploaded under** — `curl -F 'jarfile=@x.jar;filename=y.jar'` in
        `run-job.sh`, so Flink's `/jars` listing and the UI's Uploaded Jars both read
        `job-aggregator-1.0-SNAPSHOT-<sha>.jar`. The manifest alone would have meant fetching the
        jar off the JobManager to answer "what commit is this"
      `run-job.sh` resolves the SHA once (`rev-parse --short HEAD`, suffixed `-dirty` when the tree
      is not clean) and passes it to both maven invocations. All three projects still build and
      **144 tests pass**
      ⚠ **Residual gap: the uploaded-jar list does not survive a JobManager restart** (Flink keeps
      uploads in a temp dir), so after an HA recovery the running jobs no longer say which commit
      they came from. The manifest still does, if you have the jar. Closing that properly means
      logging the SHA from each job's `main()` — 8 files, and not done

### Dev/prod compose split — 2026-08-31

The hardening above was applied in place to `docker-compose.yml`, which is the file developers run.
User's call: put production in its own file and give the developers theirs back.

- [x] **`docker-compose.prod.yml` created; `docker-compose.yml` restored byte-for-byte to `f023a47`**
      (the last commit before M1). Verified with `diff` against `git show f023a47:docker-compose.yml`
      — identical apart from a 3-line header pointing at the prod file. Both parse
      (`docker compose config --quiet`). Prod-only services: `zookeeper`, `taskmanager-1..4`,
      `historyserver`, `prometheus`, `alertmanager`, `grafana`. Dev-only: `taskmanager`
- [x] **Checkpointing is wired into BOTH files** (merge of `feat/checkpointing`, 2026-08-31). The
      shared `data-collector-flink-checkpoints` volume is mounted on the JobManager and every
      TaskManager in each — one TM in dev, four in prod — because `filesystem` storage has the JM
      writing metadata and the TMs writing state files, so it has to be one directory. This is the
      first real instance of the "edit both files by hand" cost below, and it arrived within a day
      ⚠ A local volume shares across containers on **one host only** — same limitation as the HA
      store. Neither survives losing the box; that needs a distributed FS
- [x] **Two independent files, NOT a base + override.** An override file cannot *remove* a service
      or a volume, and prod has to drop dev's single `taskmanager` — so `-f a.yml -f b.yml` could
      never have produced the prod stack. The cost is real and is written into both headers:
      **a service added to one must be added to the other by hand**
- [x] **NiFi is in the prod file, byte-identical to dev's** (user's call 2026-08-31). It stays out
      of scope for *modification* — nothing in the hardening touches it — but it is not removed
- [x] **`monitoring/` is prod-only.** Dev has no Prometheus, so nothing scrapes it; the dev
      TaskManager still runs the Prometheus reporter on 9250, it just has no collector
- [x] **`flink/normalizer/Dockerfile` stays shared and unchanged.** The HA/archive `mkdir`+`chown`
      is inert in dev (two empty dirs, no volumes mounted on them) and load-bearing in prod
- [x] **`run-job.sh` no longer hard-codes the TaskManager names.** M2b had baked in
      `taskmanager-1..4`; dev is back to a single `taskmanager`, so log lookups now **discover**
      the containers (`docker ps` + `^taskmanager(-[0-9]+)?$`). Guarded for the empty case —
      `"${arr[@]}"` on an empty array aborts under `set -u`. Exercised both branches
- [x] **Makefile gained `prod-up` / `prod-deploy` / `prod-verify` / `prod-logs`;
      the dev targets are untouched** (user's call). The prod targets deliberately differ:
      no `down -v` anywhere, no `git pull`, and `--build` is not optional
- [ ] **Not verified: neither file has been brought up.** The split is a text-level change validated
      by `config --quiet` and by diffing service sets — no container has been started from either

### Deploy-path hazards — separate from the cluster config

- [x] **H1 — `make refresh-normalizer` runs `docker compose down -v`**, deleting every named volume
      including Kafka's log dir and the Postgres data dir. **CLOSED 2026-08-31** by the compose
      split: `refresh-normalizer` is now explicitly a **dev** target pointed at `docker-compose.yml`,
      and the prod path is `make prod-deploy` — **no prod target removes a volume**. Note the
      guarantee is by convention, not enforcement: nothing stops someone typing
      `make refresh-normalizer` on a prod box, it just no longer targets the prod stack's file
- [ ] **H2 — every deploy target opens with `git pull origin`** and builds on the box, so prod runs
      whatever is on the branch with no rollback. Build and tag images in CI.
      **Half-addressed 2026-08-31**: the new prod targets do **not** `git pull` — you check out the
      ref you intend to run. The other half stands: prod still *builds on the box*, and the real fix
      (build + tag in CI, deploy an image reference) needs a registry decision and is not done.
      S7's SHA stamping at least makes "what is actually running" answerable now
- [x] **H3 — `run-job.sh` exits at `RUNNING`** and nothing re-checks. **CLOSED 2026-08-31.** M9's
      `FlinkJobCountWrong` closed half; `make prod-verify` closes the other — it counts `RUNNING`
      jobs on `/jobs` against `ALL_JOBS` and fails the deploy if they disagree, instead of leaving
      it to a 5-minute alert. `prod-deploy` runs it as its last step.
      ⚠ Never executed against a live cluster — the jq expression is unverified against a real
      `/jobs` response

### Carried over — open items from other sections that this work depends on

- [ ] **Cold start asks for every subscribed market at once** (no checkpointing ⇒ every delta-feed
      key hits `no_baseline` on its first update). Confirm the burst is tolerated or rate-limit it.
      **The postponement makes this load-bearing rather than incidental**: it now fires on every
      restart, and M1 makes restarts automatic. Verify it before or alongside M1. *(from `## control
      plane` on main)*
- [ ] **`SNAPSHOT_RETRY_MS` is read from the JobManager env and set nowhere in
      `docker-compose.yml`**, so every run takes the 5-min default. It interacts directly with the
      cold-start burst above, and the JobManager env is something this work is already editing.
      *(from `## control plane` on main)*

### Deferred, with a trigger rather than a date

- [ ] **S3 — RocksDB** if M2's sizing proves insufficient as markets grow (trigger: heap utilisation,
      not a schedule). `managed.fraction` back to 0.4, plus disk for `io.tmp.dirs`
- [ ] **S8 — Kafka is single-broker RF=1**, so `acks=all` means "the one replica that exists". Caps
      what M4 can promise. Its own piece of work
- [ ] **L1 — application mode / one cluster per job**; **L2 — standby JM + 3-node ZooKeeper**;
      **L3 — parallelism > 1** (repartition topics first, or it gains nothing — and note there are
      no spare slots today)
- [x] **Checkpointing landed 2026-08-31** on `feat/checkpointing`, risk accepted rather than
      mitigated — see the standing-decision note above. The three routes below are now about closing
      the *residual* cross-job risk, not a precondition for checkpointing itself
- [ ] **Close the cross-job replay risk** via one of the report's three routes: merge the normalizer
      chain into one JobGraph (only option giving both state survival and consistency), add a
      downstream monotonicity guard (⚠ conflicts with the user-stated "job 2 is the only sequence
      validator" invariant — reopen deliberately, see [[project_type_validator]]), or checkpoint only
      terminal jobs
- [ ] **M5 — `.uid()` on every operator.** Needed before a checkpoint can be restored across a
      topology-changing redeploy; not added as part of the 2026-08-31 checkpointing landing
- [ ] **S2 — `pipeline.max-parallelism`**, **S6 — stop-with-savepoint deploys**, **M6 — checkpoint
      volumes were added ad hoc (`data-collector-flink-checkpoints`), revisit against the report's
      proposed values (num-retained, tolerable-failed-checkpoints) in section 03

### From the PR #11 review (2026-09-02)

- [ ] **⚠ DEV STACK IS NOW UNDER-PROVISIONED FOR CHECKPOINTING — found live 2026-09-02.** The e2e
      harness ran 21 PASS / 38 FAIL; 37 of the 38 are a cascade from a starved TaskManager (checkpoint
      RPC timeouts -> JobManager down -> `connection refused` for every later scenario), only 1 was
      data-shaped and it fell inside the same window. Dev's single TaskManager has
      `Total Process Memory: 1.688gb` (the image default) on a 4.1 GB Docker VM. **M2a gave the TMs
      real memory in `docker-compose.prod.yml` ONLY** — the compose split handed dev back an
      image-default TM, survivable until checkpointing landed. Pick one: raise the Docker Desktop
      allocation to ~8 GB, give dev's TM explicit memory (M2a's dev half), or raise
      `CHECKPOINT_INTERVAL_MS` in dev. **Until then the e2e suite cannot validate this branch.**
- [ ] **Make the e2e harness rebuild images, or document that it does not.** `stack.Provision` runs
      `up -d --wait` with no `--build` ("Images are not rebuilt: a missing one is built by `up`"), so
      it builds the job JARs from source but runs them on a stale image. On this PR that produced
      **59/59 failures** whose stack trace (`Failed to create directory for shared state:
      file:/opt/flink/checkpoints/<jobid>/shared`) points at checkpoint storage and never mentions the
      image. One `--build` fixes it; the diagnostic dead-end is the expensive part
- [ ] **Two checkpoint volumes exist on the dev box; only the project-prefixed one is mounted.**
      `data-collector-flink-checkpoints` (unprefixed, healthy) vs
      `data-collector_data-collector-flink-checkpoints` (what compose actually uses). A `chown`
      remediation aimed at the wrong one silently does nothing — **verify against
      `docker inspect <container>` mounts before trusting it, including on prod**
- [ ] **`FlinkCheckpointsNotCompleting` cannot fire when no job is running.** The
      `flink_jobmanager_job_*` series are job-scope and disappear entirely with the jobs, so
      `increase(...) == 0` has no series to evaluate — same blind spot as `numRunningJobs != 8` when
      the JobManager is down. Consider an `absent()` companion rule

- [x] **Blocker — `read_committed` on every consumer outside `flink/normalizer`.** The earlier round
      stopped at the module boundary. Added `isolation.level=read_committed` to `flink/merger` and
      `flink/adjustment` (both consume `^p[0-9]+-(asks|bids)$`, which job 6 now writes
      transactionally), and `kgo.FetchIsolationLevel(kgo.ReadCommitted())` to
      `web/internal/kafka/consumer.go` and `e2e/consumer/consumer.go`. All four build clean.
      **Rule going forward: making a sink transactional is a change to every consumer of that topic,
      in every language.**
- [ ] **⚠ ESCALATED 2026-09-02 by the user: "30 sec delay for an event in an ETL system is
      catastrophic."** Full analysis in [[project_flink_production]] §11 and the published report.
      `added_latency ≈ hops × interval ÷ 2` — 6 transactional hops × the 10 s default = ~30 s avg /
      ~60 s worst, and it is NOT tunable (1 s avg would need a ~330 ms interval; `minPause` is
      `interval/5` and `maxConcurrentCheckpoints` is 1). **Recommended: `AT_LEAST_ONCE` on the six
      intermediate sinks, keep checkpointing — zero added latency, consumers need no change, and
      duplicates are near-harmless because job 2 rejects them as `stale_or_duplicate` (not a resync
      trigger). NOT yet decided.** Do this in order:
    - [ ] **Measure first with `pipeline_timings`** (`book_build_out − event_time`) under
          `EXACTLY_ONCE`. Nothing above is measured — it is arithmetic
    - [ ] **Put a FLOOR under `staleness_threshold_seconds`.** Arrivals now step at the commit
          cadence, so any threshold near the interval fires on healthy markets. The 2026-09-01 live
          run used **20 s** — two commit cycles. No market below ~3 × the interval; a `CHECK`
          constraint would make it hard to get wrong
    - [ ] **A checkpoint-failure storm now looks exactly like a market-wide outage.** Tolerable 3 ×
          timeout 120 s ⇒ minutes with no commits, every market downstream silent, and job 2 asks for
          a snapshot for EVERY one. The cold-start burst, triggerable by a slow disk
    - [ ] **Pin `CHECKPOINT_INTERVAL_MS` in compose** — set nowhere today, same gap as
          `SNAPSHOT_RETRY_MS` and `REFRESH_INTERVAL_MS`
    - [ ] **Raise the exporter's `output_threshold_seconds`** from 10 s, which is below ONE hop
- [ ] **OPEN PRODUCT DECISION — EXACTLY_ONCE latency.** Records are invisible downstream until the
      producing job checkpoints, so 6 transactional hops add ~30s average / 60s worst case, against
      sub-second before. `lpa-staleness-exporter`'s `output_threshold_seconds: 10` is now below the
      last hop's added latency alone. Options: accept, drop the interval to ~2s, or keep the middle
      hops `AT_LEAST_ONCE` and reserve `EXACTLY_ONCE` for the terminal sinks. **Nothing decided.**
- [x] **`tolerableCheckpointFailureNumber` raised from Flink's default 0 to 3** (2026-09-02). One
      failed checkpoint used to fail the job, which is how the 2026-08-31 root-owned-volume incident
      became a permanent 6-job restart loop. A transient failure is now an alert, not an outage; a
      persistent one still ends the job, just after 4 rather than 1. `FlinkCheckpointsFailing` fires
      on the FIRST failure regardless, so the budget hides nothing. The comments in `Dockerfile`,
      `flink.yml` and `alertmanager.yml` that asserted "tolerable is 0" were updated with it
- [x] **`RETAIN_ON_CANCELLATION` → `DELETE_ON_CANCELLATION`** (2026-09-02). Retention only leaked:
      `run-job.sh` submits with no `savepointPath`, so a resubmitted job never restores from a retained
      checkpoint — and `run-all-jobs`/`prod-deploy` cancel all 8 first, stranding 8 directories per
      deploy, forever (`num-retained` prunes only a *running* job's history). It also contradicted S1
      (restart from `latest` and re-baseline). ⚠ This governs CANCELLATION only — a job that FAILS
      still keeps its checkpoint and automatic restarts restore from it, which is the whole feature.
      **Still open: no disk-usage alert on `data-collector-flink-checkpoints`.** The leak is closed so
      it is no longer urgent, but a full volume is still a checkpoint failure and nothing watches it
- [x] **`CheckpointingOptions.CHECKPOINT_STORAGE` made load-bearing instead of dead** (2026-09-02).
      `CheckpointConfig.configure()` reads `execution.checkpointing.dir` but ignores
      `execution.checkpointing.storage` outright, so the key was silently dropped and filesystem
      storage was in effect only via `CheckpointStorageLoader`'s undocumented directory fallback (which
      logs a warning asking for the explicit value). Fixed by routing the storage `Configuration`
      through `env.configure()` rather than `env.getCheckpointConfig().configure()` — `env.configure()`
      does `configuration.addAll(...)` of every key AND delegates to `CheckpointConfig.configure()`,
      so both the storage type and the directory land where the loader reads them. Kept job-side
      rather than moved to compose `FLINK_PROPERTIES` on purpose: dev compose sets no checkpoint
      properties and `run-local.sh` has no cluster at all. ⚠ **In Flink 2.x the keys are
      `execution.checkpointing.storage` / `execution.checkpointing.dir`** — the 1.x names
      (`state.checkpoint-storage`, `state.checkpoints.dir`) are gone; do not reach for them
- [ ] **Nits from the review, none blocking:** package `io.tibobit.normalizer.checkpointingConfigurer`
      is camelCase while every sibling is lowercase; trailing whitespace on 7 lines; the
      `control-plane` sink in `TypeValidatorJob` still carries the now-false "Without checkpointing,
      DeliveryGuarantee is NONE" comment the PR rewrote on all 9 other sinks; `CHECKPOINT_INTERVAL_MS`
      and `CHECKPOINT_DIR` are read from env but set nowhere (and under REST submission `main()` runs
      on the **JobManager**, so they would go on that service); no test for `CheckpointingConfigurer`
- [ ] **⚠ Do not upgrade `kafka-python` past 2.0.2 without fixing `lpa-staleness-exporter` first.**
      The exporter survives transactional topics only because 2.0.2 never filters control batches
      (`is_control_batch` is defined and referenced nowhere). A conformant client returns nothing for
      its `seek(end - 1)` and every Flink topic goes silently unmeasured. See
      [[project_flink_production]]
- [x] **ex1/ex2 REVISED AGAIN 2026-09-02 — WS pushes are full snapshots, not deltas** (user
      request, on fresh live captures `nobitex-snapshots.txt`/`bitpin-snapshots.txt`). Reverses the
      2026-07-21/07-25 "WS = delta" classification: consecutive WS pushes carry the FULL book each
      time, and a level absent from a later push with no `qty=0` entry marking it proves it cannot
      be a delta feed. Confirmed with the user via two clarifying questions (treat WS as
      `type=snapshot` matching ex4/ex5's ordered-but-never-jump-checked shape; trust the new
      captures over the July finding) before touching any code. Fix confined to
      `NobitexParser`/`BitpinParser` (`type` "update"→"snapshot", `sequence_jump` 1→0, kept
      `sequence_id=pub.offset`) — **job 2 and job 5 needed ZERO code changes**, since both were
      already exchange-agnostic and already supported a sequenced-but-unchecked snapshot (ex4/ex5's
      shape). `baselinePending` now dead for ex1/ex2 (joins ex3/ex9); the control plane can no
      longer engage for either exchange (`no_baseline`/`sequence_gap` live only in job 2's `update`
      branch). Touched: both parsers + their unit tests + javadoc, `TypeValidateFunction`'s class
      javadoc (removed ex1/ex2 from "delta feeds", relabeled the null-seq-REST-resync example from
      ex1 to ex6), `TypeValidateFunctionTest` (relabeled the same block from "ex1 nobitex" to "ex6"
      — logic unchanged, ex6's REST snapshot is the current live example of that shape),
      `e2e/scenario/data_ex1.go`/`data_ex2.go` (3 of 6/8 scenarios renamed for a changed premise,
      every WS-driven `WantSnapshots` entry recomputed for clear-then-replace instead of merge,
      `PrecisionDust`/`RebaseToman`/`RebaseScaledUnit` each lost a previously-resting level from
      their post-WS expectation as a worked example of the bug), `scenarios.go` (renamed 01–14, did
      NOT renumber), `data_control.go` (`ControlEx1NoBaselineThenGap`/`ControlEx1LaggingRestResync`
      DELETED — their premise is now unreachable; 45/47 retired, not reused), `sample-raw-data.md`
      §§ ex1/ex2, and memory (`project_pair_extractor`, `project_type_validator`,
      `project_raw_pipeline_decision`, `project_e2e_harness`, `project_control_plane`, `MEMORY.md`).
      **Verified**: 189 Java tests green across `common`+`job-pair-extractor`+`job-type-validator`,
      `job-book-builder`'s 28 tests green untouched (confirms it needed no change), `go build`/
      `go vet` clean on `e2e/`. **Not run live** — no docker stack available this session, and no
      CI exists in this repo to run instead. **Disclosed, not silently dropped**: e2e-level coverage
      of the null-seq/event-time resync-accepted path (previously `ControlEx1LaggingRestResync`)
      has no replacement this session — only the unit-level `TypeValidateFunctionTest` case covers
      it now; recreating it needs ex6's real REST wire shape built out as a new scenario. See
      [[project_pair_extractor]], [[project_type_validator]], [[project_e2e_harness]],
      [[project_control_plane]]
- [x] **Review pass on PR #14 (`fix/nobitex-and-bitpin`), 2026-09-05** — reviewed the ex1/ex2
      snapshot reclassification above and closed the three findings it left. **Finding 1 (raised,
      then CLOSED BY THE USER, no code change)**: because the WS branch no longer consults
      `baselinePending`, a Centrifugo offset RESET leaves `lastSeq` permanently ahead of every
      incoming offset — each push is dropped `stale_or_duplicate` forever, and no
      `snapshot_request` is emitted, since that machinery lives only in the `update` branch.
      Confirmed empirically against the real function (old shape recovers via `baselinePending`,
      new shape does not; 0 commands). Checkpointing being enabled now also removes the
      restart-clears-state escape hatch. **The user's decision: keep `seq <= last` → drop as is.**
      `event_time` cannot serve as a tiebreaker because it is COLLECTOR-stamped on several feeds
      (ex3/ex4 `System.currentTimeMillis()`, ex7 WS processing time), and a second cross-clock
      comparison risks re-creating the 2026-08-19 deadlock that `resyncPending()` exists to fix.
      Recorded as accepted risk, not a defect. **Finding 2 (FIXED)**: the e2e gap the entry above
      disclosed — verified real first (scenario 46's event times march strictly forward, so nothing
      left in the suite emitted a book whose event time REGRESSES). Added
      `ControlEx6LaggingRestResync` as **60**, appended not renumbered, 47 stays retired. Ported to
      ex6 because bybit's REST snapshot is now the only live null-seq resync feeding a real delta
      stream. Job-2 half verified offline by replaying the exact sequence through the function: 8
      main-stream records (6 books + 2 resets), rejects `sequence_gap`/`awaiting_snapshot`/
      `sequence_gap`, exactly 2 commands. **⚠ Its eight `WantSnapshots` books are HAND-COMPUTED
      from the replace/merge rules and NOT observed live** — same caveat as 48; jobs 1/3/4/5/6 are
      unverified for this scenario. **Finding 3 (FIXED)**: `e2e/docs/` was never regenerated after
      the `server.go` annotation changed, so the committed spec still said "WS delta". Ran
      `swag init -g main.go -o docs`; verified surgical — exactly one word per file
      (`delta`→`snapshot`), JSON still valid, 1 path / 7 definitions intact, no version drift.
      **Finding 4 (NEW, FIXED)**: the PR introduced gofmt violations (`main` clean, branch not) —
      3 continuation comments in `data_ex1.go`/`data_ex2.go` indented with spaces not tabs, despite
      the entry above claiming gofmt clean. `gofmt -w`. Verified: `go build`/`go vet`/`gofmt -l`
      clean on `e2e/`, 189 Java tests still green. See [[project_control_plane]],
      [[project_e2e_harness]]


## flink job 2 — staleness-triggered resync (branch `feat/flink-check-staleness`)

Built 2026-08-31 (`d78d84d`), then **cut back**: the `no_data_received` half was removed and the
`stale` half re-implemented on a per-key timer instead of a tick stream. Design, the reasoning for
the cut, and the two vacuous-test lessons are in [[project_control_plane]] §"Silence closes the last
gap". 66 job-2 tests, 282 across the normalizer. **Verified LIVE 2026-09-01 on the local docker
stack** — see the same memory section for the run and what it did not cover.

- [x] ~~Run it live~~ — done 2026-09-01. ex1/BTCUSDT, threshold 20s: snapshot accepted → 21s silence
      → `RESET` (`source_ids: []`, `simulation: 1`, null book) + `snapshot_request` `reason: "stale"`.
      Held at exactly 1 reset / 1 command through 4+ deadlines; recovery snapshot with an OLDER
      `event_time` was ACCEPTED (0 rejects), then a NEW episode opened 21s later
- [x] ~~Verify the postgres driver really loads on the cluster~~ — done: the job reached RUNNING with
      both vertices up, which only happens if `RefreshingLookup.open()` read the watch list.
      Confirmed `flink-connector-datagen` is NOT on the cluster classpath — bundling it would have
      been necessary, which is one more reason the tick-stream design was the wrong one
- [x] ~~An unwatched market is never judged silent~~ — done: pair 2 (`status='unsubscribe'`) held a
      live key in job 2 state, went silent 2.5× the threshold, and produced nothing
- [ ] **Test the threshold-edit / unsubscribe path live.** `RefreshingLookup` was only ever read at
      `open()` in this run — the 60 s refresh was never exercised, so "edit the threshold in
      Postgres and it lands without a resubmit" is still UNPROVEN live (it is unit-tested via
      `resubscribedMarketIsWatchedAgain`)
- [ ] **Confirm never-heard-from markets really are covered by the exporter.** The cut assumed a
      subscribed market that has produced nothing already reads `stale=1` in
      `lpa-staleness-exporter`. That was reasoned from [[project_staleness_exporter]], NOT verified
      against `exporter.py` or a live dashboard. **If it turns out not to be covered, that is a gap
      to close in the exporter — not by putting the roster back into job 2**
- [ ] **Watch a cold start with a REAL watch list.** The live run had exactly ONE subscribed market.
      Nothing has yet exercised many markets going stale at once, which is where the shared
      suppression window and the per-key timers actually matter
- [ ] **e2e coverage.** Silence is time-based, so it inherits the same "no e2e" gap as re-asking.
      Decide whether a scenario with a tiny threshold is worth it, or whether the 16 unit tests plus
      the live check are enough
- [x] **Unsubscribe left a phantom book on every downstream topic** — found by the user in live
      testing 2026-09-01, fixed the same day. `onTimer`'s unwatched branch was a bare `return`, so a
      dropped market's book stood in job 5's MapState and job 6's union forever with nothing ever
      watching that key again. It now emits a `RESET` and deliberately **no** `snapshot_request`.
      ⚠ **this revises the "unwatched market judged nothing" result of the 2026-09-01 live run —
      that was verified as correct and was in fact the bug.** A web-side age guard was considered and
      **rejected by the user**: each stage goes to a different team, so a book with no feed behind it
      must not be on the topic at all. Full note in [[project_control_plane]].
      ⚠ **Verification owed — run it live: unsubscribe a fed market, confirm exactly ONE reset, ZERO
      control commands, and the exchange leaving `p{id}-{side}`**
- [ ] **NiFi must ignore `snapshot_request` for an unsubscribed market.** The user's fix, on the NiFi
      side, not in this repo. Until it lands, losing the (now much narrower) refresh race still lets
      the control plane reopen a feed the operator just closed
- [ ] **`REFRESH_INTERVAL_MS` is set nowhere in `docker-compose.yml`** and takes its in-code default,
      exactly like `SNAPSHOT_RETRY_MS` above. Same decision, same place to make it. ⚠ the default is
      now **15 s**, and the constraint is that it stays well below the SMALLEST
      `staleness_threshold_seconds` in `exchange_markets` — whatever is set in compose must respect
      that, or unsubscribes start racing the silence timer again
- [ ] Optional: re-register `control-command` so the registry carries the updated `reason` doc.
      Doc-only, not a compatibility change — `stale` works without it (the live run proves it:
      `reason: "stale"` serialised fine against the registered schema)
- [x] **Kafka broker OOM'd roughly once a day (4x, 2026-09-02..04)** — root cause found and fixed on
      `fix/kafka-broker-oom-producer-churn`, **NOT deployed, NOT observed live**. `EXACTLY_ONCE` +
      10s checkpointing (PR #12, merged the day the OOMs started) + `KafkaSinkBuilder`'s
      **INCREMENTING** default = a new transactional.id, and so a new producer id, every checkpoint;
      the broker held each for **7 days** on the **1 GB default heap**. Fixed by `POOLING` on all 8
      sinks, 1h transactional-id/producer-id expiration, and a heap floor in both compose files.
      Full note + traps in [[project_kafka_broker_memory]]
- [x] **PR #15 merged to `main` 2026-09-05** as `7dc3a50`, +252/-0 (identical to the PR's stat).
      Two conflicts, both mechanical: `TypeValidatorJob.java` (the branch predates PR #13, which
      re-indented that file — resolved by taking `main`'s copy and re-applying the import + two
      `setTransactionNamingStrategy` calls, NOT by hand-merging the hunk) and a parallel append in
      this file. `mvn -o clean test` on `flink/normalizer` green, both compose files
      `docker compose config -q` clean. **Still not deployed.** See [[project_kafka_broker_memory]]
- [ ] **Consider `-XX:+ExitOnOutOfMemoryError` on the broker's `KAFKA_OPTS`.** PR #15 makes the OOM
      unlikely but leaves the recovery hole it documented: a heap OOM exits **0**, so
      `restart: on-failure` never fires and a human is still needed if it ever happens again. One
      flag turns that into an automatic restart. Left out of #15 on scope
- [ ] **Deploy and verify the broker fix.** Nothing below has been seen working:
      - [ ] `free -g` on the box first — `docker-compose.prod.yml` assumes 4G of broker heap is
            affordable, and the dev file's 2G is a guess sized for a small box, not a measurement
      - [ ] Bring the broker up (it has been DOWN since 2026-09-04 17:17) and **rebuild + resubmit
            the 6 normalizer jobs** — the POOLING change is in the jars, not in config
      - [ ] After an hour up, confirm the churn stopped: producer ids per partition should sit in
            the single digits, like NiFi's `ex{id}-raw` (5-8), not ~1700. Read them off a broker
            restart's `Wrote producer snapshot ... with N producer ids` lines, or
            `docker logs taskmanager | grep -c 'ProducerId set to'` — **with the broker UP**, since
            that check returns 0 for the trivial reason when nothing is producing
      - [ ] Watch the first job restart after the switch: POOLING recovers by LISTING transactions
            to abort, a different path from INCREMENTING, and it is a **one-way** switch
- [ ] **No alert can catch the next broker OOM.** `kafka-exporter` exposes no JVM heap metric and
      M9's rules are Flink-only — three days of heap growth were invisible. Needs a JMX exporter on
      the broker and a heap rule, and the `kafka-topics --list` healthcheck (a JVM start plus ~3000
      topics' metadata every 15s, with a 10s timeout) replaced with something cheap
- [ ] **Decide on the 10s checkpoint interval.** Deliberately left untouched by the OOM fix. With
      `EXACTLY_ONCE` + `read_committed` each of the 6 chained jobs only publishes on commit, so the
      interval is a **per-hop latency floor**: ~6 x 10s end to end. That is a product decision about
      how fresh the book must be, not a memory one

## kafka broker OOM — POOLING regression (2026-09-05) — RESOLVED BY REVERT

- [x] **P0 — pipeline was down on the dev server. Resolved 2026-09-05 by reverting the whole
      checkpointing/EXACTLY_ONCE/POOLING round (user's call), not by fixing POOLING.**
      All 6 normalizer jobs restart-loop every 60s on
      `IllegalStateException: The record serializer does not expose a static list of target topics`.
      `POOLING` -> `LISTING` abort strategy -> must enumerate target topics, and
      `ExactlyOnceKafkaWriter.initialize()` aborts lingering transactions **unconditionally**, so it
      fails on a clean submit. All 8 EXACTLY_ONCE sinks use a `setTopicSelector` lambda, which cannot
      supply the identifier. Not a config problem — POOLING and dynamic topic routing are
      incompatible as currently written. See [[project_kafka_broker_memory]]
- [x] **Pick the fix — neither (a) nor (b): the feature was removed instead.** Kept here because
      both remain the options if EXACTLY_ONCE ever comes back. (a) Named static class implementing `TopicSelector<T>` +
      `KafkaDatasetIdentifierProvider` returning `DefaultKafkaDatasetIdentifier.ofPattern(...)` —
      keeps POOLING and dynamic routing, 8 sinks to change, needs one narrow pattern per stage.
      (b) Revert the 8 sinks to `INCREMENTING` — one-line each, and the broker-side 1h
      `transactional.id`/`producer.id` expirations (the half that actually fixed the OOM) stay.
      (b) restores service fastest; (a) is where it should end up.
- [ ] **If EXACTLY_ONCE returns and (a) is chosen: measure it before trusting it.** `AdminUtils.getTopicsByPattern` does a full
      `listTopics()` on **every writer `initialize()`** — per subtask, per restart — and LISTING then
      lists transactions per matched topic. Never measured at this cluster's ~3000 partitions.
- [ ] **Give the dev deploy targets a verify step.** `make prod-verify` counts RUNNING jobs and would
      have failed this immediately, but the box runs `docker-compose.yml` and the deploy was
      `make run-all-jobs`, which submits and stops. Consider making the check hold over an interval
      rather than sampling once, so a restart-looping job cannot pass through a RUNNING window.
- [ ] **No alert fired for a total pipeline stall.** M9's rules are Flink-side, yet ~50 min of every
      normalizer job restart-looping and zero output on `p{id}-{side}` surfaced only on manual
      inspection. Whatever the fix, this gap is the reason it went unnoticed.

### Follow-ups left open by the revert (2026-09-05)

- [ ] **⚠ P1 — `docker-compose.yml` has no `restart-strategy`, and checkpointing is now off.** Flink
      defaults `restart-strategy.type` to `disable` when checkpointing is off, so a job that throws
      once goes to FAILED and stays there. `docker-compose.prod.yml` is covered by M1's explicit
      `exponential-delay` block; the dev file is not — **and the dev file is what the dev server
      runs**. Copying M1's five keys across is ~5 lines. Raised with the user, deliberately NOT
      applied as part of the revert. This is the single most likely way the pipeline dies quietly.
- [ ] **Rebuild and redeploy `web` and `e2e`, not just the Flink jars.** Removing
      `kgo.FetchIsolationLevel(kgo.ReadCommitted())` touched two separate Go deployables.
- [ ] **First deploy after the revert may stall `read_committed` readers that still exist elsewhere.**
      Any transaction left dangling by the failed POOLING deploy holds the LSO until the broker
      aborts it — bounded by `transaction.timeout.ms` (10m), so it self-clears; worth knowing rather
      than debugging live.
- [ ] **The dev TaskManager's `2g` / `managed.fraction: 0.1` sizing comment is now stale** — it is
      justified in `docker-compose.yml` by "6 jobs checkpointing every 10s". The sizing is still fine
      and was left alone; the rationale no longer holds.
- [ ] **`e2e` may have assertions that assume exactly-once record counts.** Not audited as part of
      the revert. With `DeliveryGuarantee.NONE` duplicates are possible again, so any exact-count
      scenario is now a potential flake.
- [ ] **Local `mvn test` cannot run JaCoCo on this laptop** (JDK 26; "Unsupported class file major
      version 70"). Tests were run with `-Djacoco.skip=true`. Pre-existing, unrelated to the revert,
      but it means coverage gates do not run locally.
