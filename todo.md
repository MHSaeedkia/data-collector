# TODO

> **Scope note.** This branch (`docs/flink-production-hardening`) carries only the Flink
> production-readiness worklist. The project's other sections — adjustment, market subscriptions,
> scripts, control plane, e2e, lpa-staleness-exporter, web, flink/normalizer parsers, flink/merger —
> were removed here at the user's request, along with every completed task. **They are not done and
> they are not abandoned**: recover them with `git show origin/main:todo.md`, and do not merge this
> file back to main without restoring them first.

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
      restart can still duplicate at the seam. See **S8** — single-broker RF=1 means `acks=all` is
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
- [ ] **Revisit checkpointing** once one of the report's three routes is chosen: merge the normalizer
      chain into one JobGraph (only option giving both state survival and consistency), add a
      downstream monotonicity guard (⚠ conflicts with the user-stated "job 2 is the only sequence
      validator" invariant — reopen deliberately, see [[project_type_validator]]), or checkpoint only
      terminal jobs
