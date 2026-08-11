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
