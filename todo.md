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
- [ ] Verify the four upstream steps (raw / type-validated / rebased / applied-precision) have their wanted values too
