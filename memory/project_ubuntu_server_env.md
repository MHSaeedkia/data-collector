---
name: server-build-env
description: Java/Maven toolchain fixes on the deploy server (Debian 12) needed for `make refresh-normalizer` / `run-job.sh` to build the Flink jobs
metadata:
  type: project
---

Deploy server (`ssh tibobit-data-collector`, Debian 12 "bookworm", x86_64) had **no Java and no
Maven installed at all** — `refresh-normalizer`'s `run-job.sh` step (`mvn -f pom.xml package`)
failed with `mvn: command not found`. Fixed 2026-07-07:

- `flink/normalizer` pins `java.version=21`.
  Debian 12 bookworm's apt repo (and its `bookworm-backports`, confirmed both empty of it on this
  server's mirror `repo.tibobit.ir`) only ships **openjdk-17**, not 21 — Debian doesn't backport
  OpenJDK major versions like this.
- Installed **Eclipse Temurin 21** by downloading the official tarball directly from
  `api.adoptium.net`/`github.com/adoptium` (checksum-verified) to `/opt/jdk-21`, then registered it
  system-wide via `update-alternatives --install/--set` for both `java` and `javac` (priority 2111)
  and appended `JAVA_HOME=/opt/jdk-21` to `/etc/environment`. Did NOT add the Adoptium apt repo —
  a one-off tarball avoids trusting/maintaining a third-party repo for a single package.
- Installed `maven` (3.8.7) from the normal bookworm apt repo — that's fine as-is, since Maven
  itself just needs *some* JVM to bootstrap; it detects the compiler JDK via `JAVA_HOME`/`java` on
  `PATH`, which now resolves to Temurin 21 via the alternatives above.
- **The whole `/opt/data-collector` checkout is root-owned** (0755, no group/world write) and
  `m_gholami` is NOT in the `docker` group, so `docker compose` (used by the same Makefile target)
  requires `sudo` — meaning the intended invocation is `sudo make refresh-normalizer`, which runs
  `mvn` as root too. Confirmed `sudo mvn -f pom.xml package -q -DskipTests` builds clean and
  `sudo ./run-job.sh` builds, uploads, submits, and reaches Flink `RUNNING` end-to-end. Running
  `mvn` as the unprivileged user directly on the root-owned tree fails with a *different* error
  (`could not create parent directories` under `target/`) — that's a permissions artifact of testing
  without sudo, not a real bug; always test/run this project's Maven builds under `sudo` on this box.

No application/job code was touched — this was purely a missing-toolchain issue on the host.

Captured as a reusable, idempotent script: `scripts/install-deps.sh` (mirrors `scripts/warmup.sh`
style/conventions). Re-running it is safe — it detects an existing `/opt/jdk-21` + `mvn` on PATH
and skips reinstalling. Also self-installs its own light prerequisites (`curl`, `jq`) via apt if
missing, rather than just erroring out — only `apt-get` itself is a hard requirement (can't
bootstrap the package manager). Verified 2026-07-07 by copying it to the server and re-running
twice: correctly no-ops every dep since all were already installed. If a fresh server ever needs
provisioning, run this script before `make refresh-normalizer`.
