---
name: flink-production-hardening
description: Production-readiness report for the Flink cluster in docker-compose.yml — what breaks today, the changes agreed for now (restart strategy, memory/TM split, HA, producer acks, exposure, monitoring), and why checkpointing was POSTPONED then UN-postponed 2026-08-31 on feat/checkpointing (cross-job replay risk accepted, not mitigated). NiFi is out of scope.
metadata:
    type: project
---

# Taking the Flink cluster to production

Report of 2026-08-26, revised 2026-08-29 after review with the user. **M1, M7, M2a, M2b, S5, M4, M3, S4 and M9
are APPLIED** (2026-08-29 / 2026-08-31, on `docs/flink-production-hardening`); **M8 is DROPPED** — see "What has landed"
below; everything else is still review-only. A formatted copy is published at
<https://claude.ai/code/artifact/0bab2c11-7c5a-4928-b2cc-78a1aaacdc15>; nothing here depends on that
link. The ordered apply-one-by-one list is in `todo.md` under `## flink (production readiness)`,
keyed by the same refs.

**Scope** `docker-compose.yml` jobmanager + taskmanager, `flink/` · **Jobs** 8 (6 normalizer + merger + adjustment) · **Image** flink:2.2.0-scala_2.12-java21

> **Job count corrected 2026-08-29.** The review was written against 7 jobs; merging `origin/main` brought in `flink/adjustment` (PR #10), making it **8**, and `ALL_JOBS := adjustment merger $(NORMALIZER_JOBS)` confirms it. The cluster runs **8/8 task slots, completely full**. M2's slot arithmetic and M9's job-count alert are corrected throughout; `flink/adjustment` was **not read** for this review, so whether it has the same sink/state characteristics as the other seven is unverified.

## What has landed (2026-08-29 … 2026-08-31)

**⚠ Read this first: as of 2026-08-31 none of the hardening below is in `docker-compose.yml`
any more.** It all lives in **`docker-compose.prod.yml`**, a separate file. `docker-compose.yml`
was restored byte-for-byte to `f023a47`, the last commit before M1, and is the **development**
stack. Where an item below says "applied to `docker-compose.yml`", read `docker-compose.prod.yml`.
See "The dev/prod compose split" at the end of this section for why, and for what it costs.

The agreed round is **complete**: M1, M7, M2a, M2b, S5, M4, M3, S4, M9, S7. M8 dropped.

Applied in apply-order, verified only by `docker compose config --quiet` (exit 0) — **nothing has
been deployed or observed running**:

- **M1** — the five `restart-strategy.exponential-delay.*` keys on `jobmanager`.
- **M7** — `restart: unless-stopped` and a `json-file` 100m x 5 `logging:` block on the JobManager
  and every TaskManager. Deliberately *not* applied to any other service.
- **M2a** — `process.size: 8g`, `managed.fraction: 0.1`, `env.java.opts.taskmanager` on the single
  TaskManager, slots left at 8.
- **M2b** — replaced the single `taskmanager` with **`taskmanager-1..4`, 3 slots each (12 total)**,
  each with `deploy.resources.limits.memory: 9g`, host metrics ports 9250-9253. The commented-out
  `taskmanager-2` block was removed, having become the real `taskmanager-2`. Note the container
  metrics port stays 9250 in all four and only the *host* port varies — the report's section 07
  said "each with its own metrics port", which turned out to be unnecessary.

- **S5** — a `healthcheck:` on all four TaskManagers (`curl -f localhost:9250/metrics`, 20s/5s/3,
  40s start period). ⚠ **This is visibility, not recovery.** Docker does not restart a container
  because it went unhealthy, and `restart: unless-stopped` does not change that — a hung JVM now
  *reports* unhealthy and keeps running. M9's alerting is what makes it actionable.
- **M4** — `acks=all`, `enable.idempotence=true`, `retries=2147483647`,
  `delivery.timeout.ms=120000` via `.setProperty(...)` on **all 11 `KafkaSink` builder sites across
  8 files** — one per job, plus RebaserJob's `rejected` dead-letter sink and TypeValidatorJob's
  `rejected` **and** `controlCommands` sinks (the todo said "job 2's dead-letter sink"; there are
  three extra sinks, not one). Verified by compiling all three Maven projects and running the
  suites: **105 + 16 + 23 = 144 tests, all passing**.

**M8 was applied on 2026-08-31 and reverted the same day** — the port binding is back to a bare
`"7070:8081"`. See the standing decisions below for why; section 07's `192.168.150.104:7070:8081`
line is superseded and should not be applied.

- **M3** (2026-08-31) — a `zookeeper:3.9` service plus `high-availability.{type,zookeeper.quorum,
  storageDir,cluster-id}` and a shared `data-collector-flink-ha` volume. Host-agnostic by
  construction, so it satisfies the 2026-08-31 constraint: `zookeeper:2181` is a compose service
  name on the internal network and the storage dir is a container path. New volumes:
  `data-collector-flink-ha`, `-zk-data`, `-zk-datalog`. Pure insertion, 125 lines, nothing removed.

  **Two deliberate deviations from section 07, both believed necessary:**
  1. **The HA keys are on all four TaskManagers, not just the JobManager.** With HA on, the leader
     publishes a random session id to ZooKeeper and fences RPC with it; a TaskManager left on the
     default `NONE` uses the standalone leader id and should be rejected at registration. In an
     ordinary standalone deployment this falls out of every node sharing one `flink-conf.yaml` —
     compose is what splits config per service, which is why section 07 reads as if only the JM
     needs it. **Reasoned from Flink's fencing model, not observed.** If the cluster comes up with
     0 registered TaskManagers, this is the first thing to check — but the failure mode of the
     *other* choice is the same symptom, so trying it is cheap either way.
  2. **The `flink-ha` volume is mounted on the TaskManagers too**, not only the JobManager as
     section 07 says. The HA storage dir doubles as the blob store, which is how a TaskManager
     fetches job jars when the JobManager is unreachable; in a real cluster it is one shared
     filesystem visible to every node.

  **M3 as first applied was broken and was fixed on 2026-08-31 while doing S4** — see the S4 entry:
  `/opt/flink/ha` does not exist in the image, so the named volume mounted there came up `root:root`
  and the JobManager, which runs as uid 9999, could not write it. The fix is in
  `flink/normalizer/Dockerfile`, not in compose.

  **The healthcheck is `zkServer.sh status`, not section 07's `echo ruok | nc`.** `nc` is not
  guaranteed present in `zookeeper:3.9` and there was no internet to check; `zkServer.sh` ships with
  ZooKeeper and the image's own entrypoint uses it. Costs a JVM per probe, hence `timeout: 10s`.

  **Two operational consequences of M3 that did not exist before, neither yet handled:**
  - **Resubmission now duplicates.** Once HA recovers the 8 jobs on its own, a human running
    `make run-all-jobs` out of habit gets **16 running jobs**, all consuming and producing. Check
    `/jobs` before submitting anything.
  - **Recovery ignores submission order.** HA restores all 8 job graphs at once in no particular
    order, but sources use `OffsetsInitializer.latest()` and downstream-first submission ordering is
    load-bearing (S1). A recovered upstream job can emit before its downstream consumer is running,
    and those records are gone. This is inside the re-baseline semantics the user already accepted,
    but it is a *new* place that semantic gets exercised — automatically, unattended.

- **S4** (2026-08-31) — `jobmanager.archive.fs.dir: file:///opt/flink/archive` on the JobManager,
  a new `historyserver` service on the same `./flink/normalizer` build (`command: history-server`),
  and a shared `data-collector-flink-archive` volume. 57 lines in compose, pure insertion.

  **The volume-ownership bug, found here and fixed for both S4 and M3.** Docker creates a missing
  mount destination as `root:root`, and this image runs as **uid 9999 (`flink`)**. Neither
  `/opt/flink/ha` (M3) nor `/opt/flink/archive` (S4) exists in `flink:2.2.0-scala_2.12-java21`, so
  both named volumes came up unwritable by the process that has to write them. **Verified by probe,
  not reasoned**: `touch` into a fresh volume at `/opt/flink/archive` returned `Permission denied`,
  and the same probe against an image that pre-creates and `chown`s the directories returned
  `WRITE OK`. The fix is four lines appended to `flink/normalizer/Dockerfile`
  (`mkdir -p` + `chown flink:flink` for both paths, as root, then back to `USER flink`).
  **Consequence: the next deploy must rebuild the Flink image** — `up -d` alone reuses the old one
  and M3's HA silently has nowhere to write. `/opt/flink/log` was never affected because it exists
  in the image already, which is why M7's log volumes worked.

  **Deviations from section 05/07, all deliberate:**
  1. **Published on host port 7071, not 8082.** 8082 — the HistoryServer's own default — is
     schema-registry on this host and 8081 is NiFi. Container side stays 8082. A port choice, not an
     address, so the host-agnostic decision is unaffected.
  2. **No `depends_on: jobmanager`.** The failure this exists for *is* a dead or unhealthy
     JobManager, which is exactly when the archive has to stay readable. It only reads a directory
     and never joins the cluster, so it also gets **none** of the M3 HA keys.
  3. **`historyserver.web.address: 0.0.0.0` and `.web.port: 8082` set explicitly.** In-container
     binds, not host binds; the published port is unreachable if the address ever defaults to
     loopback. Unverified which way the default goes in 2.2.0, so it is pinned rather than assumed.
  4. **Same build as the cluster** rather than the bare `flink` image — already built locally, so it
     adds no pull. The connector jars it inherits are dead weight and harmless.

  **What S4 does not cover: only jobs that reach a *terminal* state are archived.** A job caught in
  M1's exponential-delay restart loop is running, not terminal, so it never appears in the
  HistoryServer no matter how many times it has failed. Repeated-restart diagnosis is M9's
  `numRestarts` alert plus the container log, not this. S4 is for jobs that died and stayed dead.

**`jobmanager.memory.process.size: 2g`: decided 2026-08-31, NOT applied — loose end closed.** It
appears in section 07's jobmanager block, belongs to no M-item, and carries no attribution or
measurement. The image default is 1600m and nothing shows it is short; M3 adds only a ZK client and
the job-graph store to the JobManager. With the file now committed to running on 5+ dev and test
environments, raising a memory floor without evidence is the wrong direction. Revisit only with a
measured JobManager heap number.

**What M4 does and does not buy.** It closes: transient broker-side failures (infinite `retries`
bounded by `delivery.timeout.ms`), acceptance of an under-replicated write (`acks=all`), and
**reordering across retries** (`enable.idempotence`) — that last one is the load-bearing part,
because a reordered write corrupts the book in exactly the way the checkpoint-replay problem does.
It does **not** close: `DeliveryGuarantee` is still `NONE`, so records sitting in the producer's
buffer when a TaskManager JVM dies are still lost, and idempotence is per-producer-session, so a job
restart starts a new producer id and can still duplicate at the seam. And per **S8**, this is a
single-broker RF=1 Kafka — `acks=all` means "the one replica that exists".

**M4 was applied inline at all 11 sites rather than through a shared helper.** `flink/merger` and
`flink/adjustment` are standalone Maven projects that do **not** depend on `normalizer/common`
(checked `merger/pom.xml`), so centralising would have meant introducing a cross-project dependency
between three independently-built artifacts — far outside M4's scope. Inline duplication was the
smaller cost.

**Deviations from section 07, deliberate:** section 07's jobmanager block also carries
`jobmanager.memory.process.size: 2g`, which belongs to no M-item and was **not** applied — it is a
loose end, decide it with M3. The `-1..4` blocks are written out explicitly rather than sharing a
YAML anchor, matching this file's existing fully-explicit style.

**Blast radius that M2b created and that was fixed with it:** `flink/run-job.sh` hard-coded
`docker logs ... taskmanager` in three places for its failure diagnostics. Renaming the container
would have made every one of them print "No such container" at exactly the moment someone is
debugging a failed submit. It now has a `TASKMANAGERS=(taskmanager-1 .. -4)` array and a `tm_logs`
helper that reads all four and prefixes each line with its source. **[[flink-deploy-tooling]]'s
slot-leak procedure is now stale** — `docker compose restart taskmanager` names a container that no
longer exists; the sequence is four restarts then the JobManager.

**Unverified, and it needs a human before deploy:** four TaskManagers at 8 g plus the JobManager is
~34 g of Flink alone, before Kafka/Postgres/NiFi/the rest. **Host RAM was never inspected** — this
session had no access to the target box. A comment above the TaskManager blocks says so. If it does
not fit, scale `process.size` and the container `memory:` limit *together*, keeping the limit above
`process.size`.

**M9 — Prometheus, Alertmanager and Grafana *(applied 2026-08-31)*.** `monitoring/` holds all the
config, bind-mounted read-only; compose gains `prometheus` (host **9090**), `alertmanager` (**9093**)
and `grafana` (**3001**, because 3000 is the `web` service) plus three named volumes. Six scrape
targets — the JobManager on 9249, the four TaskManagers on 9250, and the three existing exporters —
all named as compose services, so this satisfies the host-agnostic rule without exception.

*The metric names are the point of this entry.* They were not taken from documentation. A throwaway
`flink:2.2.0-scala_2.12-java21` JobManager and TaskManager were booted on a scratch network, the
bundled `TopSpeedWindowing` example submitted to force job- and task-scope metrics into existence,
and both endpoints scraped. Then a real `prom/prometheus:v2.53.0` was pointed at that pair with the
actual rules file. Result: all ten rules reported `health: "ok"` with no `lastError`, and
`FlinkJobCountWrong` and `FlinkTaskManagersMissing` were observed *firing* with correctly rendered
annotations ("Flink is running 1 jobs, expected 8"). Three things that scrape revealed and that no
reference would have:

- **Flink exposes everything as a Prometheus `gauge`**, counters included — `numRestarts` among them.
  `increase()` still works, because Prometheus ignores TYPE metadata at query time, but it emits a
  "metric might not be a counter" warning that is expected and not a fault
- **TaskManager series carry `host` as the container IP with dots turned into underscores**
  (`host="172_20_0_3"`), which changes on every recreate. Anything that groups TaskManagers must use
  Prometheus's own `instance` label instead
- **`numRestarts` is JobManager-scope, not TaskManager-scope** (`flink_jobmanager_job_numRestarts`,
  labelled with `job_name`), so the restart rules read from 9249

Four deliberate deviations from the table in section 02, each because the table would not have
worked as written:

1. **`FlinkJobManagerDown` (`up == 0`) was added**, and it is listed first for a reason: a dead
   JobManager is the *quietest* failure in the whole set, because `numRunningJobs != 8` cannot fire
   when the series does not exist. It inhibits the derived alerts in Alertmanager so a failover pages
   once instead of six times
2. **"numRestarts rising" became two rules** — any restart in 15 min (warning), more than five in an
   hour (critical). M1 converted a poison record from a job that dies into a job that restarts
   forever; that job stays `RUNNING`, so the job-count alert stays clear, and it never reaches a
   terminal state, so S4's HistoryServer never sees it either. These two rules are the only thing
   that does
3. **Back-pressure reads `backPressuredTimeMsPerSecond`, not the `isBackPressured` gauge** the report
   named. The gauge is a 0/1 instantaneous sample and a 15 s scrape interval will mostly miss it; the
   other is milliseconds accumulated between scrapes and is what a threshold can be set against
4. **Nothing was written for the three exporters.** They are scraped and queryable, but their
   thresholds were never part of this review and inventing them ships noise

**Two honest gaps.** First, `records_lag_max` is the **one metric name in the file that is not
verified** — the probe cluster had no Kafka source, so
`flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max` comes from the
documented Flink 2.x convention. If it is wrong the rule is *silently dead*: a rule whose selector
matches nothing never errors and never fires. Confirm on the first deploy with `curl -s
localhost:9250/metrics | grep -i records_lag_max`. Second, **Alertmanager delivers nowhere.** The
project has no agreed notification channel, so its default receiver is deliberately empty (the
upstream `- name: 'null'` idiom) rather than pointed at a placeholder URL that would fail on every
alert. Grouping and two inhibit rules are in place, so adding a receiver is a one-block edit. Alerts
remain visible on Alertmanager's UI and Prometheus's `/alerts` meanwhile. Grafana ships with the
Prometheus datasource provisioned and **no dashboards** — none have been authored, and an empty
dashboards provider is just another directory to keep in sync.

**What was and was not validated.** `promtool check config` passed on `prometheus.yml` and found the
rules file (10 rules), and the live run above exercised them end to end. `alertmanager.yml` parses as
YAML and matches the schema by inspection, but **`amtool check-config` was never run** — no
`prom/alertmanager` image is cached locally. Related: `prom/prometheus:v2.53.0` is already local,
while `prom/alertmanager:v0.27.0` and `grafana/grafana:11.1.0` **both need a pull** before the first
`up`.

**S7 (2026-08-31)** — the last item of the round. Pins: `redis_exporter:v1.86.0` and
`kafka-exporter:v1.9.0` were **read off the locally cached images** (the OCI
`org.opencontainers.image.version` label, and the binary's own `--version` respectively) rather than
recalled; `kafka-ui:v0.7.2` could not be — that image carries neither a version label nor
Spring's `build-info.properties` — so it is the one pin taken from memory of provectuslabs' last
release, annotated as such in the file. It fails **loudly** if wrong (`manifest unknown` at `up`),
which is why it was acceptable to ship unverified where M9's `records_lag_max` was not.
SHA stamping went two places because either alone is insufficient: `Git-Commit` in each shaded
jar's manifest (a `${git.sha}` property defaulting to `unknown`, plus `<manifestEntries>` in all 8
module shade transformers — verified by building with and without `-Dgit.sha`), and the **filename
the jar is uploaded under** (`curl -F 'jarfile=@x.jar;filename=y.jar'`), which is what Flink's
`/jars` listing and UI actually display. The manifest is durable but needs the jar in hand; the
upload name is visible from the cluster but is lost on a JobManager restart.

---

### The dev/prod compose split (2026-08-31)

**Why.** Every item above was applied in place to `docker-compose.yml` — the file developers run on
a laptop. That was the wrong home for it: production hardening (four 8 GB TaskManagers, ZooKeeper,
a monitoring stack) makes the dev stack heavy and slow for no dev benefit. User's call: production
gets its own file, developers get theirs back exactly as it was.

**Shape.** `docker-compose.prod.yml` is a **full standalone copy**, not an override layered with
`-f a.yml -f b.yml`. That was forced, not preferred: **a compose override can add and modify but
cannot remove**, and prod has to drop dev's single `taskmanager` service in favour of
`taskmanager-1..4`. An override would have left a fifth, unwanted TaskManager registering with the
cluster. The price is duplication — **a service added to one file must be added to the other by
hand** — and that warning is written into both headers and the README rather than left implicit.

**Decisions inside the split, each with a reason that is not obvious from the diff:**

- **`docker-compose.yml` was restored from `f023a47`, not hand-reverted.** `origin/main` had
  already merged the branch at `e130d6f`, so main's copy *contains* M1/M7/M2a/M2b/S5 — it is not a
  clean baseline. `f023a47` is the last commit that touched the file before M1. Verified with
  `diff`; identical apart from a 3-line header.
- **NiFi is in the prod file, byte-identical** (user's call). "Out of scope" has always meant *do
  not modify it*, which is not the same as *remove it*; removing it would have left the prod stack
  with no ingestion if prod does in fact run it from here.
- **Volume names and the compose project name are unchanged across both files.** Deliberate: a
  distinct project name would re-prefix every volume, and an existing box switching to the prod file
  would come up with empty Kafka and Postgres volumes and no error. The two stacks are never meant
  to run side by side on one host.
- **S7's pins live only in the prod file.** Dev is allowed to float; "restore it as it was" wins.
- **The shared `flink/normalizer/Dockerfile` was left alone.** Its HA/archive `mkdir`+`chown` is
  inert in dev (the dirs exist, nothing is mounted on them) and load-bearing in prod, so one image
  still serves both.
- **`run-job.sh` now *discovers* TaskManager containers** (`docker ps` filtered on
  `^taskmanager(-[0-9]+)?$`) instead of the `taskmanager-1..4` list M2b baked in. This is fallout
  the split created — dev is back to one `taskmanager` — and it needs the empty-array guard,
  because `"${arr[@]}"` on an empty array aborts under `set -u`.
- **Makefile: prod targets added, dev targets untouched** (user's call). They differ on purpose in
  three ways — no `down -v` anywhere in the prod path, no `git pull` (check out the ref you mean to
  deploy), and `--build` unconditional (the Dockerfile ownership fix). `prod-verify` counts
  `RUNNING` jobs against `ALL_JOBS` and fails the deploy on a mismatch.

**What this closed:** H1 (the `down -v` target is now unambiguously dev-only), H3 (the deploy
asserts its own job count), and the `--build` deploy note. **Half of H2** — no more `git pull` in
the deploy path; prod still builds on the box. It also defuses the standing concern that 4 × 8 GB of
TaskManager could not coexist with "runnable on 5+ dev/test envs": that sizing is now prod-only.
**Still unverified: neither file has been brought up.** The split is a text-level change validated
by `config --quiet` and by diffing the two service sets.

---

**Verification still owed on all four**, none of it possible from here: kill a job and watch it come
back (M1); read the TaskManager startup log's own memory breakdown and confirm both the `--add-opens`
list and the heap-dump flags appear in the JVM options line (M2a); confirm 12 free slots and that the
8 jobs spread across the four TaskManagers rather than piling onto one (M2b); confirm the
TaskManagers report healthy and that a deliberately hung one flips to unhealthy (S5); confirm the
producer config actually took by reading `ProducerConfig` values in the TaskManager startup log —
**no test in the repo exercises sink configuration**, so the 144 passing tests prove nothing broke,
not that M4 works (M4).

---

## Three standing decisions (user, 2026-08-29 / 2026-08-31)

- **Checkpointing is POSTPONED, not rejected.** Leave it off and the pipeline as it is for now.
  Section 03 records the reason and what would have to be true to revisit it. Everything that only
  pays off with checkpointing is parked with it.
- **NiFi is out of scope entirely.** It is not managed from our side and its compose entry is
  development-only. Nothing in this document proposes a NiFi change, and NiFi is not part of any
  sizing, hazard or configuration item here.
- **`docker-compose.yml` must stay host-agnostic — no host IPs, no host-specific bindings**
  (user, 2026-08-31). This one file runs on **five-plus dev and test environments**, so a literal
  like `192.168.150.104` is not merely unportable: the container **fails to start** on any host
  that does not own that address (`bind: cannot assign requested address`). `127.0.0.1` is no
  better — it makes the UI unreachable from anywhere but the box itself, which breaks the normal
  dev workflow of hitting a shared test box from a laptop. **This is why M8 was dropped**, and it
  binds every later item too: **M3** must not hard-code a ZooKeeper host address and **M9** must
  not hard-code scrape targets or a Grafana root URL. If a per-host binding is ever wanted, it
  belongs in a `docker-compose.override.yml` on that host or in an env var with a permissive
  default — not in this file.

---

## 2026-08-31 — Checkpointing UN-postponed on `feat/checkpointing`

The postponement above held for two days. `feat/checkpointing` (commit `ec8d35c`, reviewed and fixed
2026-08-31) enables per-job `EXACTLY_ONCE` checkpointing on all 6 normalizer jobs via a new
`CheckpointingConfigurer` (`flink/normalizer/common/.../checkpointingConfigurer/`), called at the top
of each `main()`. **User's call, 2026-08-31: proceed and accept the cross-job replay risk described in
03 for now** — it has NOT been mitigated (none of the three "What would have to change" routes below
were taken; the 6 jobs still checkpoint independently with no shared cut). This is a live, accepted
risk, not a resolved one — re-read 03's "Why" section before assuming checkpointing makes restarts
safe.

What landed with it, and what this review fixed on top:
- **FIX — did not compile.** `CheckpointingConfigurer` called `CheckpointConfig.setCheckpointStorage
  (FileSystemCheckpointStorage)`, which does not exist in Flink 2.2.0 — that fluent
  `StateBackend`/`CheckpointStorage`-object API was removed; checkpoint storage is config-key driven
  now (`CheckpointingOptions.CHECKPOINT_STORAGE` = `"filesystem"` +
  `CheckpointingOptions.CHECKPOINTS_DIRECTORY`, applied via `CheckpointConfig.configure(Configuration)`
  — the same pattern already used for the other options). Confirmed by decompiling the actual
  `flink-runtime`/`flink-core` 2.2.0 jars from `~/.m2` (no such method exists on `CheckpointConfig`),
  then by compiling all 7 touched modules — **fails without this fix, builds clean with it, and all
  269 existing tests in those modules still pass** (`mvn test -pl common,job-pair-extractor,
  job-type-validator,job-rebaser,job-precision,job-book-builder,job-aggregator -am`, JDK 25 targeting
  the pom's `java.version=21`, JDK 21 itself not installed on this machine). **The branch as pushed to
  `origin/feat/checkpointing` would not have built.**
- **`EXACTLY_ONCE` `DeliveryGuarantee` + a `TransactionalIdPrefix`** on every main/dead-letter
  `KafkaSink` across all 6 jobs (not `job-type-validator`'s `control-plane` sink — left on M4's plain
  idempotent-producer config, since duplicate resend requests are harmless).
- **FIX — missing consumer `isolation.level=read_committed`.** The branch added transactional
  producers but never set this on any downstream `KafkaSource`; without it, a `read_uncommitted`
  consumer (Kafka's default) sees records from transactions that later abort — silent corruption of
  exactly the kind 03 describes, just one hop earlier. Added to the 5 sources that read another job's
  now-transactional output (`job-type-validator`, `job-rebaser`, `job-precision`, `job-book-builder`,
  `job-aggregator`); `job-pair-extractor`'s source reads NiFi's raw topic, never written
  transactionally, so it was left alone.
- **FIX — restart strategy left alone.** `CheckpointingConfigurer` also set a job-level `fixed-delay`
  restart strategy (5 attempts, 10s) via `env.getConfig().configure(...)`, overriding M1's cluster-wide
  `exponential-delay` (infinite retries) in `docker-compose.yml`. Removed — jobs still inherit M1's
  policy. (Verified against the flink-runtime/flink-core 2.2.0 jars locally: `ExecutionConfig
  .configure()` does own restart-strategy parsing, so the call would have worked as written — reverted
  for policy reasons, not because it was broken.)
- **FIX — no shared checkpoint storage.** `CHECKPOINT_DIR` defaults to `file:///opt/flink/checkpoints`,
  but no volume backed that path — each of the 5 containers (jobmanager + 4 taskmanagers) had its own
  ephemeral local copy. A task rescheduled to a different TaskManager after a restart would not find
  its own checkpoint. Added one shared named volume, `data-collector-flink-checkpoints`, mounted at
  `/opt/flink/checkpoints` on all 5.
  **Re-applied across BOTH compose files when this branch merged into main (2026-08-31)**, since by
  then the hardening had moved to `docker-compose.prod.yml` and `docker-compose.yml` was back to the
  plain dev stack. Dev gets it too and is not optional: `CheckpointingConfigurer` runs in the jobs,
  not the cluster config, so a dev run checkpoints whether or not the volume is there — and without
  it the JobManager's metadata and the TaskManager's state files land in two different container
  filesystems that each look fine and cannot restore each other. Dev has one TM, which reduces the
  blast radius to two containers, not to zero.
  ⚠ A local volume is shared across containers on **one host**. Same single-host ceiling as the HA
  store: neither survives losing the box, and both would need a distributed FS to.
- Stale comments ("Without checkpointing, DeliveryGuarantee is NONE...") left over from before
  `CheckpointingConfigurer` existed, on every sink this branch upgraded, were rewritten to describe the
  current EXACTLY_ONCE/transactional behavior.

**ADDED when this branch merged to main (2026-08-31) — checkpointing is now monitored.** M9's two
checkpoint alerts had been parked with the postponement; un-parking them was the whole point of the
postponement ending. A third was added on top. New `flink-checkpoints` group in
`monitoring/prometheus/rules/flink.yml`: `FlinkCheckpointsFailing` (critical, because
`TOLERABLE_FAILURE_NUMBER` is 0 — one failure fails the job), `FlinkCheckpointsNotCompleting` (a
*stuck* checkpoint increments no failure counter, so the first rule would stay silent forever — this
is the one that separates "failing" from "not happening") and `FlinkCheckpointDurationHigh` at half
the configured 120s timeout. Alertmanager gained a third inhibit rule so a checkpoint failure
suppresses that job's restart alerts — cause, not symptom. `promtool check rules` passes, 13 rules.
**The live failure below is precisely what this closes**: 6 jobs looping, job count still 8, every
existing rule silent. ⚠ These three metric names are the **only** ones in the file not taken from a
live scrape — the M9 probe cluster predated checkpointing. Confirm with
`curl -s localhost:9249/metrics | grep -i checkpoint`; a wrong name is silent.

**FIX (2026-08-31, found live) — the new checkpoint volume came up root-owned, so every job
restarted forever.** After the shared-volume fix above, the user ran the stack and all 6 jobs sat in
`RESTARTING`. Root cause, confirmed by decompiling `CheckpointingOptions` in the actual 2.2.0 jar:
`TOLERABLE_FAILURE_NUMBER` defaults to `0` (a single failed checkpoint fails the job), and
`flink/normalizer/Dockerfile` ends `USER flink` (the official image's non-root user) but never created
`/opt/flink/checkpoints` — so when Docker initialized the brand-new named volume by copying the
image's directory at that path, there was nothing there and it came up **root-owned**. The `flink`
user couldn't write into it, the first checkpoint (10s after start) failed, the job failed, backoff,
repeat — indefinitely, on all 6 jobs equally since they share the one volume. Fixed by pre-creating
`/opt/flink/checkpoints` with `chown flink:flink` in the Dockerfile *before* `USER flink`, so Docker
copies correct ownership into the volume on first mount. ⚠ the volume as already created on the user's
box was still root-owned and needed a one-off `chown -R flink:flink` (via a root-user container against
the same volume) — the Dockerfile fix only prevents this on a *fresh* volume, it does not repair one
that already exists. NOT yet confirmed fixed live (user was given the remediation command, result
unseen from this session).

**PR #11 review (2026-09-02) — three consumers outside the normalizer module were still
read_uncommitted; fixed.** The earlier round added `isolation.level=read_committed` to the five
`KafkaSource`s *inside* `flink/normalizer`, and stopped at the module boundary. `ALL_JOBS` is 8 jobs:
`flink/merger` and `flink/adjustment` both consume `^p[0-9]+-(asks|bids)$`, which `AggregatorJob`
(job 6) now writes transactionally, and neither had the property. Same defect one directory over.
Also fixed the two Go consumers, which franz-go defaults to read-uncommitted:
`web/internal/kafka/consumer.go` (reads both the aggregated family and job 5's snapshots) and
`e2e/consumer/consumer.go` (where aborted records surface as a flaky record-count assertion, not as a
visible bug) — both now pass `kgo.FetchIsolationLevel(kgo.ReadCommitted())`. **The rule this
establishes: a sink turning transactional is a change to every consumer of that topic, in every
language, not just to the Flink job next in the chain.** `merger`/`adjustment` still have no
checkpointing and no transactional sink of their own — deliberately left alone, that is a separate
decision with the same latency cost as below, not a correctness fix.

**CORRECTION — `lpa-staleness-exporter` was NOT broken by transactional writes, contrary to the first
read of PR #11.** The concern was that `latest_offset_and_ts` seeks to `end - 1`, which on a
transactional topic is a commit marker, and that Kafka never delivers control records — so the poll
would return nothing and every Flink topic would go permanently unmeasured. That is true of a
*conformant* client. It is **not** true of `kafka-python==2.0.2`, which is what the exporter pins:
`is_control_batch` is defined in `kafka/record/default_records.py:168` and **referenced nowhere else
in the package** — the fetcher never filters control batches, so the marker comes back as an ordinary
record. The exporter only reads `records[-1].timestamp`, never key or value, so it keeps working.
Two consequences worth knowing: the timestamp it now reads on Flink topics is the transaction
*commit* time rather than the produce time (a systematic optimistic bias of up to one checkpoint
interval, which matters against `output_threshold_seconds: 10`), and an idle topic still correctly
goes stale because **Flink does not commit empty transactions** — `ExactlyOnceKafkaWriter.prepareCommit`
guards on `hasRecordsInTransaction()` and recycles the producer otherwise (verified in
`flink-connector-kafka-5.0.0-2.2.jar`). ⚠ This safety is an accident of a client bug: **upgrading
kafka-python past 2.0.2 will silently break the exporter** on exactly the mechanism described above.

**Verified during the PR #11 review, so stop re-deriving these:**
- **The three checkpoint metric names are real.** `numberOfFailedCheckpoints`,
  `numberOfCompletedCheckpoints` and `lastCheckpointDuration` all exist as job-scope metrics in
  `flink-runtime-2.2.0.jar`, so the `flink_jobmanager_job_*` prefix in the rules is right. The ⚠ "only
  names not from a live scrape" caveat in `monitoring/prometheus/rules/flink.yml` can be softened to
  "verified against the jar, not yet scraped" — it is no longer an unknown.
- **`CheckpointingOptions.CHECKPOINT_STORAGE` is a no-op where it is set.** `CheckpointConfig.configure()`
  in 2.2.0 reads 14 `CheckpointingOptions` keys and that is **not** one of them (`CHECKPOINTS_DIRECTORY`
  is, via the private `setCheckpointDirectory`). Filesystem storage is in effect only because
  `CheckpointStorageLoader` falls back to it when a directory is set — and logs "Users are strongly
  encouraged to explicitly set this configuration" while doing so. The line in `CheckpointingConfigurer`
  and the comment above it are therefore both misleading about *why* it works. Not fixed; see todo.
- **The broker is already correct for transactions**: `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1`
  + `_MIN_ISR: 1` are set, and `transaction.timeout.ms=600000` sits under the 900000 broker default.
  This partly answers the S8 question left open below.
- **Transactional ID prefixes are unique** across all 9 transactional sinks, and the job number in
  every prefix and every `isolation.level` comment matches the real pipeline order (job1 pair-extractor
  → job6 aggregator). Checked individually; do not re-check.
- Leaving `job-type-validator`'s `control-plane` sink non-transactional is right — NiFi consumes it and
  would otherwise need `read_committed` too.

**⚠ The unpriced consequence of EXACTLY_ONCE: end-to-end latency.** A record is invisible downstream
until the producing job's checkpoint commits, so each of the 6 transactional hops adds up to one
checkpoint interval — roughly **30s average, 60s worst case**, against sub-second before. Nothing in
the branch or in this document acknowledged that, and `lpa-staleness-exporter`'s
`output_threshold_seconds: 10` on the aggregated family is now below the added latency of the last hop
alone. Three ways out, none chosen yet: accept it, drop the interval to ~2s, or keep the middle hops
on `AT_LEAST_ONCE` and reserve `EXACTLY_ONCE` for the terminal sinks. **This is a product decision and
is still open.**

**Still not done** (out of scope for this pass — flag before relying on them): **M5** (`.uid()` on
every operator — needed before a checkpoint can be restored across a topology-changing redeploy, and
nobody added it here), **S2** (`pipeline.max-parallelism`), **S6** (stop-with-savepoint deploys), and
**S8's interaction with transactional writes** (single-broker RF=1 —
`TRANSACTION_STATE_LOG_REPLICATION_FACTOR`/`_MIN_ISR` are both 1, unexamined for what that means for
transaction durability). None of this was verified live — same caveat as the rest of this document,
"nothing has been deployed or observed running."

---

## 01 · Baseline — What happens today when something fails

Each row is the current, verified behaviour of the stack as committed — not a hypothetical.

| Failure | What actually happens now | Why | Addressed by |
|---|---|---|---|
| **A job throws** | The job goes to `FAILED` and stays there forever. The other six keep running, so the pipeline silently loses one stage. Nothing alerts. | Default `restart-strategy.type` is `disable` when checkpointing is not enabled. | **M1** |
| **TaskManager dies** | Docker restarts the container, but all eight jobs had tasks on it, so all eight fail — and per the row above, none come back. Cluster healthy, pipeline dead. | One TaskManager with 8 slots hosts every job. | **M1 + M2** |
| **JobManager dies** | Docker restarts it and it comes up as an *empty* cluster. Every submission is gone. Recovery is a human running `make run-all-jobs`. | `high-availability.type` defaults to `NONE` — no persistent job-graph store. | **M3** |
| **Any restart at all** | All keyed state is gone (books, `lastSeq`), sources resume at `latest`, and every delta market hits `no_baseline` and resyncs. | No checkpoints; all eight jobs use `OffsetsInitializer.latest()`. | **Accepted** — see 03 |
| **Broker hiccup on write** | Records are dropped. No exception, no metric, no dead letter — the book downstream is wrong until the next snapshot touches that level. | `KafkaSink`'s default is `DeliveryGuarantee.NONE`; no job overrides it or sets producer acks. | **M4** |
| **State outgrows heap** | The TaskManager JVM OOMs, which is the "TaskManager dies" row — all eight jobs go down together. | Image default `taskmanager.memory.process.size: 1728m`; `hashmap` backend keeps every book on heap. | **M2** |
| **Host reboot** | Containers come back, the cluster is empty, jobs need resubmitting by hand. | Same as JobManager death. | **M3 + M7** |
| **Logs accumulate** | JobManager and TaskManager JSON logs grow without bound on the docker host. | The Flink services set no `logging:` block. | **M7** |

**The compounding failure.** Rows 1–3 stack. A single bad record in `job-type-validator` kills that
job; six jobs keep publishing into topics nobody is reading; nothing alerts, so it is found by a
human noticing stale data hours later. **M1 and M9 together collapse this chain**, and neither needs
checkpointing — which is why they lead the list.

---

## 02 · Now — the changes agreed for this round

Ordered for application. Each is independently verifiable and independently revertible.

### M1 — An explicit restart strategy *(one line, largest single win)*

The default is `disable` **only because checkpointing is off**; setting the key explicitly overrides
that. So automatic restarts do not wait on the checkpointing decision.

```yaml
# jobmanager FLINK_PROPERTIES
restart-strategy.type: exponential-delay
restart-strategy.exponential-delay.initial-backoff: 10 s
restart-strategy.exponential-delay.max-backoff: 5 min
restart-strategy.exponential-delay.backoff-multiplier: 2.0
restart-strategy.exponential-delay.reset-backoff-threshold: 15 min
```

**Backoff is deliberately longer than Flink's defaults** (1 s / 1 min). With no checkpointing a
restart means an empty book and a full resync, so each restart has a real downstream cost — a job
flapping on a poison record must not turn into a resync loop. Pair with M9's restart-loop alert: the
backoff buys time, the alert is what actually gets it fixed.

A restarted job starts with empty state and its source at `latest`, then re-baselines through the
normal `no_baseline` → snapshot path. That is the intended semantic, not a defect.

### M2 — Real memory, and stop running eight jobs in one JVM

The image ships `taskmanager.memory.process.size: 1728m` and compose does not override it. Working
the documented split backwards from 1728 MB — metaspace 256 MB, JVM overhead ~192 MB, managed 0.4 of
the remainder, network 0.1, framework heap 128 MB — leaves on the order of **384 MB of task heap
shared by all eight jobs**. That is arithmetic; the real number is printed in the TaskManager startup
log and is worth reading before sizing.

```yaml
# taskmanager FLINK_PROPERTIES
taskmanager.numberOfTaskSlots: 3
taskmanager.memory.process.size: 8g
taskmanager.memory.managed.fraction: 0.1   # hashmap backend barely uses managed memory
env.java.opts.taskmanager: -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/opt/flink/log
```

Then **four TaskManagers with 3 slots each** instead of one with 8. **Eight** jobs at parallelism 1
need eight slots, so the cluster is at 8/8 today and 4 x 2 would still be exactly full — 4 x 3 = 12
leaves four spare for a ninth job or a parallelism increase. Spreading them over four JVMs means one
OOM takes down roughly two jobs rather than the entire pipeline. The commented-out `taskmanager-2` block is the right shape — copy to `-3` and `-4`,
each with its own metrics port.

Worth splitting into two steps when applying: **M2a** set memory on the existing single TaskManager
(low risk, immediately measurable), **M2b** split into four.

> **DO NOT SET `env.java.opts.all`.** The base image uses that exact key for the long
> `--add-opens` / `--add-exports` list Flink needs on Java 21; setting it in `FLINK_PROPERTIES`
> *replaces* the list. Use the per-process `env.java.opts.taskmanager`, and confirm both lists appear
> in the JVM options line at the top of the TaskManager log.

> **Container limit must exceed the Flink limit.** If you add `deploy.resources.limits.memory`, set
> it *above* `process.size` (8 g Flink → 9 g container). Lower, and the kernel OOM-killer kills the
> container before Flink's memory accounting can report anything — a silent restart with no
> diagnosis.

### M3 — JobManager HA, so a restart recovers the submissions

Without an HA store nothing remembers that eight jobs were submitted. With ZooKeeper the JobManager
re-reads its job graphs on boot and resubmits them unattended.

**HA does not require checkpointing, and stays consistent with the postponement:** with no checkpoint
to restore from, recovered jobs start with empty state at `latest` and re-baseline — exactly the
restart semantic chosen in 03.

```yaml
# jobmanager FLINK_PROPERTIES
high-availability.type: zookeeper
high-availability.zookeeper.quorum: zookeeper:2181
high-availability.storageDir: file:///opt/flink/ha
high-availability.cluster-id: /data-collector
```

```yaml
zookeeper:
    image: zookeeper:3.9
    container_name: zookeeper
    hostname: zookeeper
    restart: unless-stopped
    environment:
        ZOO_MY_ID: 1
        ZOO_4LW_COMMANDS_WHITELIST: srvr,ruok
    volumes:
        - data-collector-zk-data:/data
        - data-collector-zk-datalog:/datalog
    networks: [ data-collector-net ]
    healthcheck:
        test: [ "CMD-SHELL", "echo ruok | nc -w 2 localhost 2181 | grep -q imok" ]
        interval: 15s
        retries: 5
```

Needs one volume, `data-collector-flink-ha:/opt/flink/ha`, on the JobManager. **No checkpoint or
savepoint volumes** — those are parked with M1-CP.

A single ZooKeeper node is itself a SPOF; accepted, because the missing capability is job recovery
across a JobManager restart and one node delivers it. Ensemble + standby JM is L2.

**Rejected alternative:** an external supervisor polling `/jobs` and resubmitting. Less machinery,
but it would have to reimplement what the HA store already does.

### M4 — Stop silent record loss at the sink *(producer config, not delivery guarantee)*

`KafkaSink` defaults to `DeliveryGuarantee.NONE` — "messages may be lost in case of issues on the
Kafka broker." **`AT_LEAST_ONCE` is not the fix while checkpointing is off**: the docs are explicit
that it "will wait for all outstanding records in the Kafka buffers to be acknowledged by the Kafka
producer *on a checkpoint*", so with no checkpoints there is no flush barrier and it is inert.

The protection that works without checkpointing is producer-side. `KafkaSinkBuilder` exposes both
`setKafkaProducerConfig(Properties)` and `setProperty(String, String)` (verified in the
flink-connector-kafka 5.0.0-2.2 javadoc):

```java
.sinkTo(KafkaSink.<OrderBookSnapshot>builder()
        .setBootstrapServers(bootstrapServers)
        .setProperty("acks", "all")
        .setProperty("enable.idempotence", "true")
        .setProperty("retries", "2147483647")
        .setProperty("delivery.timeout.ms", "120000")
        .setRecordSerializer(...)
        .build())
```

`enable.idempotence=true` is the important one and is the reason not to just set `retries`: it makes
the broker de-duplicate retried sends **and preserves per-partition ordering across retries**. Plain
retries without it can reorder writes, which for an order-book stream is the same class of bug as the
cross-job replay in 03 — an older level state landing after a newer one.

Applies to **every** `KafkaSink`, including `job-type-validator`'s dead-letter sink.

> `acks=all` is only as strong as the broker's replication. With the single-broker RF=1 setup (S8) it
> means "acknowledged by the one replica that exists" — it closes the in-flight-buffer loss, not the
> broker-loss case.

### M7 — `restart: unless-stopped`, and log rotation on the Flink services

`on-failure` does not bring a container back after an intentional stop or in every host-reboot path;
`unless-stopped` does. And the Flink services have no `logging:` block, so their JSON logs grow until
the disk fills.

```yaml
restart: unless-stopped
logging:
    driver: json-file
    options:
        max-size: "100m"
        max-file: "5"
```

Apply to `jobmanager` and every `taskmanager`. Leave every other service alone.

### M8 — Close the REST port to the world

Port `7070` is published on `0.0.0.0` and Flink's REST API has **no authentication whatsoever**.
Anyone who can reach it can upload a JAR and run arbitrary code as the `flink` user, inside a
container with network access to the rest of the stack. This is the sharpest security issue here.

```yaml
ports:
    - "192.168.150.104:7070:8081"
```

`web.submit.enable: false` is *not* the fix — `run-job.sh` deploys through exactly that endpoint. The
control is network-layer: private-interface binding plus a host firewall, or a reverse proxy with
auth. `make run-remote` pointing at `192.168.150.104:7070` keeps working with the binding above.

The same reasoning is worth a separate pass over the other published ports (`kafka-ui` 8080,
`market-subscriptions` 8090 — already flagged unauthenticated in `todo.md`, Postgres 5432 on
`postgres/postgres`, Redis 6379 with no password). Out of scope for this round.

### M9 — Actually collect the metrics already exported *(APPLIED 2026-08-31 — see "What has landed")*

Both Flink processes run a Prometheus reporter, and there are `kafka-exporter`, `redis-exporter` and
`lpa-staleness-exporter` alongside. Nothing scrapes any of them. Every failure in section 01 is
currently detected by a human noticing stale data.

Add Prometheus, Alertmanager and Grafana, then start with:

| Alert | Signal | Catches |
|---|---|---|
| Job count wrong | running jobs `!= 8` for 2 min | Baseline rows 1, 2, 3 — the whole class |
| TaskManagers missing | registered TMs `< 4` | A TaskManager died and did not come back |
| Restart loop | `numRestarts` rising | A poison record cycling — and, here, resyncing |
| Consumer lag | source `records_lag_max` | A stage falling behind the feed |
| Backpressure | `isBackPressured` sustained | Which stage is the bottleneck |
| Heap pressure | heap used / GC time | The OOM in baseline row 6, in advance |

The two checkpoint alerts from the original report (`numberOfFailedCheckpoints`,
`lastCheckpointDuration`) are parked with M1-CP.

Exact metric names differ between Flink versions and depend on the reporter's scope format. Build the
rules against a live `curl localhost:9249/metrics`, not from a reference.

**As applied**, this became `monitoring/` plus three compose services, and the table above changed in
four places: a `FlinkJobManagerDown` rule was added ahead of everything else, "restart loop" became
two rules, back-pressure uses `backPressuredTimeMsPerSecond` rather than the `isBackPressured` gauge,
and one metric name — the Kafka one — is still unverified. The reasons are in the landed entry.

---

## 03 · Postponed — checkpointing and everything that depends on it

**Status: postponed by the user, 2026-08-29. Not rejected — revisit deliberately.**

Parked together: **M1-CP** (checkpointing config), **M5** (`.uid()` on every operator), **S2**
(`pipeline.max-parallelism`, baked into the first checkpoint), **M6** (checkpoint/savepoint volumes),
**S6** (stop-with-savepoint deploys). None of them pays off without the others.

### Why — the finding that caused the postponement

Flink has **no cross-job checkpoint coordination**. Each JobGraph checkpoints on its own timer, so
seven independently submitted jobs never share a consistent cut. That is fine within a job and
dangerous between them:

- **Within a job: safe.** If job 5 crashes, its books *and* its offsets rewind to the same checkpoint
  atomically. It re-reads and re-applies exactly the events it had already applied, in the same
  order, and lands in the identical state. No corruption.
- **Across jobs: corrupting.** If job 4 crashes and rewinds, it re-emits into
  `applied-precision-flink`. Job 5 never restarted — its books are at the head. It applies those old
  updates over newer ones and **silently reverts price levels**, and the wrong book persists until a
  snapshot happens to touch each affected level.

**Nothing is lost in this scenario — records are re-ordered.** A checkpoint is never ahead of what was
processed; the records are still in Kafka and get re-read. Re-ordering is what corrupts an order
book, which is why the fix is not a shorter interval.

**Verified, and it corrects an earlier claim in this document.** The original M4 argued the book
builder is "idempotent by construction". That conflates *idempotent* with *commutative*:
`BookBuildFunction` reads `event.getSequenceId()` only to stamp it onto the emitted snapshot
(`BookBuildFunction.java:93`) and never compares it. Job 2 is the platform's only sequence validator
and sits four stages upstream, so replayed records reaching job 5 have already passed validation once
and meet no second guard. See [[project_type_validator]], [[project_book_builder]].

### The user's position, and why it is sound

> Prefer restarting from the latest event and requesting a snapshot if needed, over resuming from a
> possibly-wrong place and building order books on invalid state.

For a live order book a fresh exchange snapshot is ground truth and cheap. Replayed state can be
subtly wrong in a way nothing detects. Correctness argues for re-baselining, so the current
no-checkpoint behaviour is the safe default — the real gap was that **nothing restarts**, which M1
fixes on its own.

### What would have to change to adopt checkpointing later

Three routes, in increasing order of cost:

1. **Merge the six normalizer jobs into one JobGraph** — operators connected directly instead of
   through Kafka topics, so a single checkpoint covers the whole chain atomically. The only option
   that gives both state survival and consistency. Costs the per-stage topics, the dead-letter
   visibility and the e2e harness's inspection points.
2. **Add a monotonicity guard downstream** (reject `sequence_id` ≤ last applied, per key, in job 5).
   Small and targeted, but it **conflicts with the user-stated invariant that job 2 is the only
   sequence validator** ([[project_type_validator]], audit of 2026-08-23) — a decision to reopen, not
   a change to make quietly.
3. **Checkpoint only the terminal jobs** whose replay cannot reach a stateful consumer. Needs a
   per-job analysis nobody has done.

### If it is revisited, these were the values proposed

30 s interval / 10 s min-pause / 5 min timeout, `tolerable-failed-checkpoints: 5` (default `0` fails
a job on one failure), `num-retained: 3` (default `1`), `RETAIN_ON_CANCELLATION`,
`state.backend.type: hashmap`, `pipeline.max-parallelism: 256`. Resume semantics for reference: a
**stop-with-savepoint resumes at the exact offset, zero replay**; a **crash rewinds to the last
checkpoint**, worst case ≈ interval + checkpoint duration, average ≈ half the interval.

---

## 04 · Closed decisions

- **S1 — keep `OffsetsInitializer.latest()`.** Closed 2026-08-29. Re-baselining from a snapshot is
  preferred over resuming at a committed offset, for the reason in 03. The downstream-first
  submission ordering in the root `Makefile` therefore stays load-bearing, and the cold-start resync
  burst stays a known, accepted cost (its `todo.md` item remains open on its own merits).
- **NiFi — out of scope.** Not managed from our side; development-only in this compose file. No NiFi
  change is proposed anywhere in this document.

---

## 05 · Should — after the section 02 items land

### S4 — Run the HistoryServer *(APPLIED 2026-08-31 — see "What has landed")*

When a job fails overnight and the JobManager restarts, the exception, the failing subtask and the
metrics vanish with it. Archiving is what makes post-mortems possible. Works without checkpointing.

```yaml
# jobmanager
jobmanager.archive.fs.dir: file:///opt/flink/archive
# historyserver service, same image, command: history-server
historyserver.archive.fs.dir: file:///opt/flink/archive
historyserver.archive.fs.refresh-interval: 10000
```

As applied this also needs `historyserver.web.address: 0.0.0.0` + `.web.port: 8082`, publishes on
**host 7071** (8082 and 8081 are taken), takes no `depends_on`, and required the Dockerfile
`mkdir`/`chown` for `/opt/flink/archive` — without which the JobManager cannot write the archive at
all. Only terminal jobs are archived; a job stuck restarting never appears.

### S5 — Health check the TaskManagers

The JobManager has one; the TaskManagers have none, so a hung JVM stays "up" forever.

```yaml
healthcheck:
    test: [ "CMD", "curl", "-f", "http://localhost:9250/metrics" ]
    interval: 20s
    timeout: 5s
    retries: 3
    start_period: 40s
```

Proves the JVM is serving, not that it is registered with the JobManager — that is M9's
"TaskManagers missing" alert. The two together cover it.

### S7 — Pin every image, and version the jars *(APPLIED 2026-08-31 — see "What has landed")*

`kafka-ui:latest`, `redis_exporter:latest` and `kafka-exporter:latest` mean a rebuild can silently
change what runs. Pin to a tag, ideally a digest. Separately every job jar is `1.0-SNAPSHOT`, so
nothing on a running cluster says which commit is deployed — stamp the git SHA into the manifest and
log it from `main()`.

**Two deviations when it was applied.** *Tags, not digests*: the three cached images report
`RepoDigests` whose value equals their local image ID, which is not a registry manifest digest and
would not resolve on another host — pinning to those would have produced a file that cannot pull.
*Manifest + upload filename, not `main()` logging*: logging the SHA at job start means editing 8
`main()` methods across three independently-built projects; the upload filename gets the SHA onto
the cluster's own `/jars` listing for one line in `run-job.sh`. The tradeoff is that the listing is
lost on a JobManager restart, so after an HA recovery the running jobs no longer say what they are.
`main()` logging is still the durable answer if that matters later.

### S8 — Kafka replication factor 1 caps what M4 can promise

Single-node broker with `OFFSETS_TOPIC_REPLICATION_FACTOR: 1` and
`TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1`. `acks=all` means "acknowledged by the one replica that
exists"; if the broker loses a partition there is no replica and no replay source. Its own piece of
work, on the same production checklist.

### S3 — RocksDB, if heap becomes the constraint *(watch)*

Without checkpointing the state backend still decides *where* state lives — `hashmap` keeps every
book on the task heap. If M2's sizing proves insufficient as subscribed markets grow,
`state.backend.type: rocksdb` moves it off heap (`managed.fraction` back to 0.4, plus disk for
`io.tmp.dirs`). Trigger is heap utilisation, not a schedule.

---

## 06 · Later

- **L1 — Application mode, one cluster per job.** A session cluster gives eight jobs a shared fate
  through shared JVMs. Application mode gives each its own JobManager and TaskManagers: real
  isolation, independent scaling and deploys. Costs eight clusters and a `run-job.sh` rewrite; M2's
  four TaskManagers buy most of the isolation for a fraction of the work. Also the natural home for
  route 1 in section 03.
- **L2 — Standby JobManager and a three-node ZooKeeper.** M3 gives job recovery; it does not remove
  the JobManager as a SPOF. Worth it once the recovery window itself is the problem.
- **L3 — Parallelism above 1.** `keyBy(exchange_id, pair_id)` parallelises cleanly, but throughput is
  capped by the input topic's partition count — raising parallelism without repartitioning gains
  nothing. Sequence: partitions, then `parallelism.default`, then re-measure.

`L4` (unaligned checkpoints) is parked with section 03.

---

## 07 · Configuration — the two service blocks as they should look after section 02

```yaml
jobmanager:
    build: ./flink/normalizer
    container_name: jobmanager
    hostname: jobmanager
    restart: unless-stopped                       # M7
    command: jobmanager
    depends_on:                                   # M3
        zookeeper:
            condition: service_healthy
    ports:
        - "7070:8081"                             # M8 DROPPED — see standing decisions
        - "9249:9249"
    environment:
        FLINK_PROPERTIES: |
            jobmanager.rpc.address: jobmanager
            jobmanager.memory.process.size: 2g

            # --- M1: explicit; the `disable` default applies only because
            # --- checkpointing is off. Backoff is long on purpose: with no
            # --- checkpoints every restart costs a full resync.
            restart-strategy.type: exponential-delay
            restart-strategy.exponential-delay.initial-backoff: 10 s
            restart-strategy.exponential-delay.max-backoff: 5 min
            restart-strategy.exponential-delay.backoff-multiplier: 2.0
            restart-strategy.exponential-delay.reset-backoff-threshold: 15 min

            # --- M3: recover submissions across a JM restart. No checkpoint to
            # --- restore, so recovered jobs start at `latest` and re-baseline.
            high-availability.type: zookeeper
            high-availability.zookeeper.quorum: zookeeper:2181
            high-availability.storageDir: file:///opt/flink/ha
            high-availability.cluster-id: /data-collector

            # --- S4
            jobmanager.archive.fs.dir: file:///opt/flink/archive

            metrics.reporters: prom
            metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
            metrics.reporter.prom.port: 9249
    volumes:
        - data-collector-jobmanager-logs:/opt/flink/log
        - data-collector-flink-ha:/opt/flink/ha            # M3
        - data-collector-flink-archive:/opt/flink/archive  # S4
    networks:
        - data-collector-net
    logging:                                      # M7
        driver: json-file
        options:
            max-size: "100m"
            max-file: "5"
    healthcheck:
        test: [ "CMD", "curl", "-f", "http://localhost:8081/overview" ]
        interval: 10s
        retries: 5
```

```yaml
# one of four — copy for taskmanager-2/3/4, changing container_name,
# hostname, the published metrics port and the log volume.
taskmanager-1:
    build: ./flink/normalizer
    container_name: taskmanager-1
    hostname: taskmanager-1
    restart: unless-stopped                       # M7
    command: taskmanager
    ports:
        - "9250:9250"
    depends_on:
        jobmanager:
            condition: service_healthy
    environment:
        FLINK_PROPERTIES: |
            jobmanager.rpc.address: jobmanager
            taskmanager.numberOfTaskSlots: 3      # M2 — was 8
            taskmanager.memory.process.size: 8g
            taskmanager.memory.managed.fraction: 0.1

            # per-process key — never override env.java.opts.all, the image
            # uses it for the Java 21 --add-opens flags Flink requires
            env.java.opts.taskmanager: -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/opt/flink/log

            metrics.reporters: prom
            metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
            metrics.reporter.prom.port: 9250
    volumes:
        - data-collector-taskmanager-1-logs:/opt/flink/log
    networks:
        - data-collector-net
    deploy:
        resources:
            limits:
                memory: 9g          # must exceed process.size
    logging:                                      # M7
        driver: json-file
        options:
            max-size: "100m"
            max-file: "5"
    healthcheck:                                  # S5
        test: [ "CMD", "curl", "-f", "http://localhost:9250/metrics" ]
        interval: 20s
        timeout: 5s
        retries: 3
        start_period: 40s
```

New named volumes: `data-collector-flink-ha`, `data-collector-flink-archive`,
`data-collector-zk-data`, `data-collector-zk-datalog`, and `data-collector-taskmanager-{1..4}-logs`.

**Host sizing.** Four TaskManagers at 8 g plus a 2 g JobManager is 34 g of Flink alone, before Kafka,
Postgres and the rest. Size to the box you actually have — **four smaller TaskManagers beat one large
one** regardless of the total, because the isolation is the point. If the host is small, 4 × 3 g still
improves on today's single 1728 m. **Host RAM has not been inspected.**

---

## 08 · Code — what changes inside the jobs

| Ref | Change | Applies to |
|---|---|---|
| **M4** | `acks=all`, `enable.idempotence=true`, `retries`, `delivery.timeout.ms` via `setProperty` | Every `KafkaSink` across all 8 jobs, including job 2's dead-letter sink |
| **S7** | Log the build SHA from `main()` | All 8 |

`.uid()` (M5) and the offsets change (S1) are **not** in this round — parked in 03 and closed in 04
respectively.

---

## 09 · Deploy path — three hazards in the current tooling

Not Flink settings; the reason a production incident would be self-inflicted.

- **H1 — `make refresh-normalizer` runs `docker compose down -v`.** `-v` deletes every named volume in
  the file, including Kafka's log directory and the Postgres data directory. One person running the
  familiar refresh command on the wrong host wipes production. **This target must not exist on a
  production box.** Split it: `refresh-normalizer` stays a development target; a separate production
  target does `up -d --build` with no `down`, no `-v`.
- **H2 — every deploy target opens with `git pull origin`** and builds from source on the box, so
  production runs whatever is on the branch with no way to name or roll back what is deployed. Build
  and tag images in CI. Same problem as S7 from the other end.
- **H3 — `run-job.sh` exits at `RUNNING`** and nothing re-checks afterwards. M1 covers job failures
  and M3 covers JobManager restarts, but a job that exhausts its restart budget still needs M9's "job
  count wrong" alert to be noticed at all. Treat that alert as part of the deploy, not monitoring
  polish.

See [[flink-deploy-tooling]] for what these scripts and targets actually do.

---

## 10 · Confidence

**Verified against the Flink 2.2 docs, the image, and the connector jar**

- Default restart strategy is `disable` without checkpointing, `exponential-delay` with it; every
  `restart-strategy.*` and `high-availability.*` key name and default quoted above.
- `KafkaSink`'s default is `DeliveryGuarantee.NONE`. `AT_LEAST_ONCE` flushes **on checkpoint** — the
  docs' own wording — so it is inert with checkpointing off.
- `KafkaSinkBuilder` exposes `setKafkaProducerConfig(Properties)` and `setProperty(String, String)`
  (flink-connector-kafka 5.0.0-2.2 javadoc).
- Image defaults read directly out of `flink:2.2.0-scala_2.12-java21`: TaskManager `1728m`,
  JobManager `1600m`, 1 slot, `parallelism.default: 1`, `failover-strategy: region`, and the
  `env.java.opts.all` flag list.
- No job calls `enableCheckpointing`, `uid` or `setDeliveryGuarantee`, or sets a state backend; all
  eight use `OffsetsInitializer.latest()`. `BookBuildFunction` reads `sequence_id` only to stamp
  output (line 93) — no monotonicity guard downstream of job 2.

**Unverified — check before or while applying**

- **The ~384 MB task-heap figure** is arithmetic from documented default fractions, not a reading.
  Read the TaskManager's own startup breakdown before finalising M2's sizing.
- **Whether `env.java.opts.taskmanager` appends to or replaces `env.java.opts.all`.** Stated as
  additive; confirm in the JVM options line at the top of the TaskManager log. Getting this wrong on
  Java 21 is a startup failure.
- **Whether HA job recovery works cleanly with no checkpoints** — expected to resubmit jobs fresh at
  `latest`, but not observed. This is the entire value of M3, so verify it by killing the JobManager
  once M3 is in.
- **Prometheus metric names** for M9's rules. Build from live scrape output.
- **Host RAM** for M2's figures — never inspected.
- **Whether `enable.idempotence=true` conflicts with anything in the current producer setup.** It
  constrains `max.in.flight.requests.per.connection` and `retries`; on modern clients the defaults
  are compatible, but confirm no job overrides those.
