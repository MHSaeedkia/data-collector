---
name: flink-deploy-tooling
description: flink/run-job.sh — the one script that builds and submits any Flink job (normalizer modules + merger), how it discovers jobs, and the Makefile targets that drive it
metadata:
    type: project
---

# One deploy script for all of `flink/`

Consolidated 2026-08-11 (user request). There was one `run-job.sh` per project — `flink/normalizer/`
(took a `<module>` arg) and `flink/merger/` (took none). They were ~95% identical. Now there is
exactly one: **`flink/run-job.sh <job>`**, and the per-project `Makefile`s are gone in favour of
**`flink/Makefile`**.

## Job discovery — no hardcoded table

A "job" is any Maven project or module whose pom declares a shade `<mainClass>`. That single check
does three things at once, which is why it was chosen over a job list:

- it is exactly what makes a jar submittable (the script submits with **no entry-class** and relies
  on the manifest — see [[normalizer-scaffold]]),
- it auto-excludes `normalizer/common`, a module that is not a job,
- a new job module, or a whole new project under `flink/`, is picked up with **no edit to the
  script**.

Two layouts are understood, distinguished by whether the project's own pom is the job:

| Layout                                | Example            | Build                             |
| ------------------------------------- | ------------------ | --------------------------------- |
| single-module project (`packaging jar`) | `flink/merger`     | `mvn -f <proj>/pom.xml package`   |
| aggregator (`packaging pom`) + modules | `flink/normalizer` | `mvn -f <proj>/pom.xml -pl <mod> -am package` |

`-am` is what pulls `common` into the reactor; do not drop it. Jar is found by glob
`*-1.0-SNAPSHOT.jar` excluding shade's `original-*` — so **artifactId should match the directory
name** (already a scaffold convention).

**Gotcha paid for:** the resolver must reject an empty job name before probing `"$project/$want"` —
with `want=""` that path collapses back to the project directory, so a bare `./run-job.sh` silently
resolved to `merger` and started building it instead of printing usage.

## Makefile targets

`flink/Makefile` — `run-local JOB=x` / `run-remote JOB=x` (192.168.150.104) / `test` (runs both
projects' suites). Note **`JOB=`**, not the old `MODULE=`.

Root `Makefile` keeps the job order in variables rather than repeated recipe lines:

- `NORMALIZER_JOBS` — the 6, downstream-first.
- `ALL_JOBS` = `merger` + those. **merger goes first**: it reads `p{id}-{side}`, so it is downstream
  of job 6 ([[price-merger]]).
- `refresh-normalizer` / `run-normalizer-jobs` — unchanged scope, normalizer only.
- **`run-all-jobs`** (new) — cancel, then submit `ALL_JOBS`. This is the only target that deploys
  the merger.

The downstream-first ordering is not cosmetic: every source reads from `latest`, so a job started
after its upstream misses everything produced in the gap ([[normalizer-scaffold]] has the full rule).

**Still open:** `refresh-normalizer` does a `down -v` + rebuild and then submits *only* the
normalizer jobs — after it runs, the merger is not running. Use `run-all-jobs` afterwards, or decide
to fold merger into `refresh-normalizer` too ([[scope-discipline]] — not done unasked).

---

# Running a job WITHOUT a cluster — `flink/run-local.sh <job>` (2026-08-18, user request)

Every job's `main()` ends in `StreamExecutionEnvironment.getExecutionEnvironment()`, which off-cluster
returns a `LocalStreamEnvironment` and starts an in-process MiniCluster. So a job needs neither the
custom Flink image nor a submission — `java -cp … <MainClass>` is enough, and that is all the new
script is. Kafka/postgres stay on docker; only Flink leaves.

Discovery is now in **`flink/job-discovery.sh`**, sourced by both `run-job.sh` and `run-local.sh`.
Extracted rather than copied: duplicating it would have re-created exactly the two-near-identical-
scripts situation the 2026-08-11 consolidation removed. `run-job.sh` is otherwise untouched.

## `provided` is not transitive — the whole difficulty, and why POMs changed

The deploy model is "the image supplies it" ([[normalizer-scaffold]]): every Flink/Kafka/Avro dep is
`provided`, the shaded jar is thin, and `java -jar` on it cannot work. `-Dmdep.includeScope=test`
resolves compile + provided + test and reconstructs most of that set — but two holes only ever show
up off-cluster, both fixed in the poms, both invisible to the cluster because `provided` is excluded
from shade (verified: the jars still contain zero `org/apache/flink` and `org/apache/avro` entries):

- **`flink-connector-base` is not a transitive dep of `flink-connector-kafka`.** It arrives inside
  `flink-dist` on the cluster, so nothing declared it. Now declared `provided` in both projects.
- **A module only resolves the slice of `/opt/flink/lib` its own pom names.** `job-rebaser` uses
  avro solely through `normalizer-common`, and `provided` does not flow across that edge — locally
  it died on `GenericRecord`. The normalizer parent now has a real `<dependencies>` block (not just
  `dependencyManagement`) listing the image's lib set, inherited by every module. **Keep it in step
  with `flink/normalizer/Dockerfile`, which is still the source of truth for the image.**
- **`flink-clients`** carries the `LocalExecutorFactory` that `env.execute()` looks up; without it
  you get "No ExecutorFactory found". Test scope, parent-level for normalizer, added to `merger`
  (which had no `flink-test-utils` to drag it in). The script fails with that explanation if a
  future project lacks it.

## Defaults and caveats

The script exports host-side `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`, `SCHEMA_REGISTRY_URL=
http://localhost:8082`, `POSTGRES_URL=jdbc:postgresql://localhost:5432/markets` unless already set —
the in-code defaults are docker-internal names. Kafka's `PLAINTEXT_HOST` listener advertises
`localhost:9092`, so this works from the host with no compose change.

Cancel the cluster's copy first or both write the same downstream topics. `latest` offsets and the
downstream-first rule apply exactly as they do on the cluster. MiniCluster state is in-memory, so
Ctrl-C loses the stateful jobs' keyed state.

**Verified 2026-08-18:** `merger` and `job-rebaser` both reach a running MiniCluster and stay up
(broker pointed at a dead port on purpose, so nothing was written to the live stack). Full suite
green (181 tests). **Not verified:** no job has been run locally against live Kafka, and whether
rebaser's postgres lookup actually fired was not confirmed — it was pointed at the real DB and did
not throw, which is weaker than a checked read.

**Naming collision, left alone:** `flink/Makefile`'s `run-local` target means "submit to the *local
cluster*" and has nothing to do with `run-local.sh`. Renaming was out of scope ([[scope-discipline]]).

**JDK caveat:** this machine has only Temurin 26; poms target 21 and Flink 2.2 is not tested above
21. MiniCluster works today (only `sun.misc.Unsafe` deprecation warnings from Pekko) — see
[[tdd-workflow]] for the matching JaCoCo-on-JDK26 noise.
