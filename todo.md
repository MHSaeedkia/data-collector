# TODO

> **Scope note.** This branch (`docs/flink-production-hardening`) carries only the Flink
> production-readiness worklist. The project's other sections — adjustment, market subscriptions,
> scripts, control plane, e2e, lpa-staleness-exporter, web, flink/normalizer parsers, flink/merger —
> were removed here at the user's request, along with every completed task. **They are not done and
> they are not abandoned**: recover them with `git show origin/main:todo.md`, and do not merge this
> file back to main without restoring them first.

## flink (production readiness)

Reviewed 2026-08-26, scope agreed with the user 2026-08-29. **Nothing applied yet.** Full report in
[[project_flink_production]] — every item below has its config block there under the same ref.

**Two standing decisions (2026-08-29):**

- **Checkpointing POSTPONED** (not rejected). Flink has no cross-job checkpoint coordination, so a
  crashed upstream job replays into a downstream job that never restarted and **silently reverts
  price levels** — verified: `BookBuildFunction` reads `sequence_id` only to stamp output (line 93),
  and job 2, the only validator, is four stages upstream. Preference is to restart from `latest` and
  re-baseline from a snapshot rather than resume from a possibly-wrong state. Parked with it:
  **M5** (`.uid()`), **S2** (`max-parallelism`), **M6** (checkpoint volumes), **S6** (savepoint
  deploys), **L4**. Revisit routes are in the report's section 03
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
      container limit together. ⚠ **Verification owed: 12 free slots, and the 8 jobs spread across
      four TMs rather than piling onto one**
- [ ] **Fallout from M2b, already partly handled.** `flink/run-job.sh` hard-coded
      `docker logs ... taskmanager` in 3 places — **fixed in the same change** with a
      `TASKMANAGERS=(...)` array and a `tm_logs` helper that reads all four. Still open:
      [[flink-deploy-tooling]]'s slot-leak procedure (`docker compose restart taskmanager`,
      `freeSlots: 8`) is stale — now flagged in that file, but not re-tested across four TMs
- [ ] **Loose end — `jobmanager.memory.process.size: 2g`.** It sits in the report's section 07
      jobmanager block but belongs to no M-item, so it was deliberately NOT applied. Decide with M3
- [ ] **S5 — TaskManager healthcheck** (`curl :9250/metrics`). They have none, so a hung JVM stays
      "up" forever. **Now four blocks to edit, not one** — the TM blocks are already open
- [ ] **M4 — stop silent sink loss via producer config.** ⚠ `AT_LEAST_ONCE` is **inert** without
      checkpointing (it flushes *on checkpoint* — the docs' own wording), so the fix is
      `acks=all` + `enable.idempotence=true` + `retries` + `delivery.timeout.ms` via
      `setProperty` on every `KafkaSink`, job 2's dead-letter sink included. Idempotence is the
      important one: plain retries can **reorder** writes, which is the same class of bug as the
      replay problem above. Verified `setKafkaProducerConfig`/`setProperty` exist on
      `KafkaSinkBuilder` (connector 5.0.0-2.2). **Now 8 jobs to touch, including `adjustment`**
- [ ] **M8 — bind 7070 to the private interface**, not `0.0.0.0`. Flink's REST API has **no auth at
      all**; anyone who can reach it uploads a jar and runs code next to Kafka/Postgres.
      `web.submit.enable: false` is NOT the fix — `run-job.sh` deploys through that endpoint.
      `make run-remote` keeps working with `192.168.150.104:7070:8081`
- [ ] **M3 — JobManager HA via ZooKeeper** + new `zookeeper` service + `data-collector-flink-ha`
      volume. Today a JM restart returns an **empty cluster**. Consistent with the postponement: no
      checkpoint to restore, so recovered jobs start at `latest` and re-baseline. ⚠ **verify by
      killing the JobManager** — that HA recovery works cleanly with no checkpoints is the entire
      value of this item and has not been observed
- [ ] **S4 — HistoryServer** (`jobmanager.archive.fs.dir` + a history-server service). Without it a
      JM restart takes every failed job's exception with it. Works without checkpointing
- [ ] **M9 — Prometheus + Alertmanager + Grafana.** Nothing scrapes the metrics we already export.
      Alerts: **running jobs != 8** (not 7 — corrected), registered TMs < 4, `numRestarts` rising,
      source `records_lag_max`, sustained `isBackPressured`, heap/GC. Build the rules from a live
      `curl :9249/metrics` — metric names vary by version. (The two checkpoint alerts are parked
      with the postponement)
- [ ] **S7 — pin `kafka-ui`/`redis_exporter`/`kafka-exporter` off `:latest`**, and stamp the git SHA
      into the job jars — every jar is `1.0-SNAPSHOT`, so nothing on a running cluster says which
      commit it is

### Deploy-path hazards — separate from the cluster config

- [ ] **H1 — `make refresh-normalizer` runs `docker compose down -v`**, deleting every named volume
      including Kafka's log dir and the Postgres data dir. **This target must not exist on a
      production box.** Split into a dev target and a prod `up -d --build` with no `down -v`
- [ ] **H2 — every deploy target opens with `git pull origin`** and builds on the box, so prod runs
      whatever is on the branch with no rollback. Build and tag images in CI
- [ ] **H3 — `run-job.sh` exits at `RUNNING`** and nothing re-checks. M9's "job count wrong" alert is
      what closes this loop — part of the deploy, not monitoring polish

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
- [ ] **Revisit checkpointing** once one of the report's three routes is chosen: merge the normalizer
      chain into one JobGraph (only option giving both state survival and consistency), add a
      downstream monotonicity guard (⚠ conflicts with the user-stated "job 2 is the only sequence
      validator" invariant — reopen deliberately, see [[project_type_validator]]), or checkpoint only
      terminal jobs
