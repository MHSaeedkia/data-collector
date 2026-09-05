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

## 2026-09-05 — the "run it with sudo" advice above is OBSOLETE, and it caused an outage-adjacent failure

Two of the premises behind `sudo make refresh-normalizer` are no longer true on
`m_gholami@192.168.150.31:2020` (`developer.internal.tibobit.ir`):

- **`m_gholami` IS in the `docker` group now** (`id` → `groups=1004(m_gholami),27(sudo),995(docker)`),
  so `docker compose` needs no `sudo`.
- **`/opt/data-collector` is owned by `m_gholami`** — the directory always was; it was the *contents*
  that drifted.

Running the Makefile under `sudo` anyway left **300 root-owned paths** scattered through the repo —
99 inside `.git` (including `.git/HEAD` and 3 whole `.git/objects/??` directories) and 185 in the
working tree, among them `monitoring/`, `monitoring/prometheus/rules/`, `monitoring/alertmanager/`
and several `memory/*.md`. Nothing surfaced until someone tried to pull as the normal user:

```
error: insufficient permission for adding an object to repository database .git/objects
fatal: failed to write object / unpack-objects failed
```

**The nasty part is the second failure, not the first.** After fixing `.git` alone the pull got far
enough to start the checkout, then aborted **half-applied** on root-owned *directories*
(`unable to unlink old 'monitoring/prometheus/rules/flink.yml'`) — leaving HEAD on the old commit
with a working tree partly on the new one. Git then refuses the retry with "Your local changes would
be overwritten by merge", and those "local changes" are not local work at all, they are the aborted
checkout's own writes. Recovering means confirming the tree has no real local work
(`git diff --stat origin/main` — it listed only the files the checkout could not write) and then
`git reset --hard origin/main`. **Check before resetting; do not reflex-reset a server tree.**

Fix applied, and the shape to reuse (a recursive `chown -R` over the whole repo is the obvious
command but reads as dangerous; scoping it to only the offending paths is both safer and clearer):

```
sudo find .git ! -user m_gholami -exec chown m_gholami:m_gholami {} +
sudo find . -path ./.git -prune -o ! -user m_gholami -exec chown m_gholami:m_gholami {} +
```

Verified afterwards: 0 non-`m_gholami` paths, and only two host bind mounts exist in
`docker-compose.yml` (`./postgres` initdb scripts, `./lpa-staleness-exporter/config.yaml:ro`), both
world-readable, so the chown broke no container.

**Rule going forward: do NOT run `make`, `mvn`, `git` or `docker compose` on this box with `sudo`.**
Every such run re-creates this trap for the next person. If something genuinely needs root, chown the
tree back afterwards.
