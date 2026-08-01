# TODO

## e2e

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
- [x] ex4/ex5/ex6 coverage — 16 scenarios in `scenario/data_ex4/5/6.go`, wired into `main.go` as 20–35: ramzinex (descending-sells re-sort, stale offset, noise, rebase toman on pair 2, rebase per-100-unit on pair 17), bitget (snapshots, multi-book record fan-out, stale seq, noise, precision dust), bybit (snapshot+deltas with a qty-"0" delete, one-sided deltas, sequence gap + reset, no_baseline, noise, precision dust) (2026-08-01, build/vet clean, not run live)
- [ ] Run the ported scenarios against the live stack — the expected books are derived, not observed
- [ ] Cross-exchange aggregation is still untested — every scenario feeds one exchange, so job 6 only ever unions one book. Needs a multi-exchange scenario shape (two raw topics warmed and fed into one pair)
- [ ] Decide on the `1K_SHIB*` / `1M_BTT*` rebase rows in `postgres/02_seed.sql` — they carry the IRR→toman shift but not the 1000×/1000000× unit shift the `1M_PEPE*` rows do
- [ ] Verify the four upstream steps (raw / type-validated / rebased / applied-precision) have their wanted values too
- [ ] No scenarios for ex7 (ompfinex, postponed — no parser) or ex9 (no raw sample). ex8/okx still has 6 scenarios commented out in `main.go` under stale 01–06 numbering
- [ ] `data_ex1.go:2`, `data_ex2.go:2` and `data_ex8.go:3` still point at `data.go`, which no longer exists — only `data_ex3.go` was fixed
