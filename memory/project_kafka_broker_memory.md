# The daily Kafka broker OOM — exactly-once producer-id churn

**CONFIRMED from broker logs 2026-09-05, fixed on `fix/kafka-broker-oom-producer-churn`, NOT YET
DEPLOYED OR OBSERVED.** The broker died of `java.lang.OutOfMemoryError: Java heap space` (first at
`2026-09-04 17:11:45`, `kafka.network.Processor`, then in every thread — request handlers, raft io
thread, quorum controller, log cleaner), ~4 times over 2026-09-02..04.

## The chain, in order

1. **PR #12 (`feat/checkpointing`) merged 2026-09-02 11:43.** It turned on checkpointing at a
   **10s default** (`CheckpointingConfigurer`, `CHECKPOINT_INTERVAL_MS`) and switched all 8
   normalizer sinks to `DeliveryGuarantee.EXACTLY_ONCE`. **The first OOM is the same day.** This
   reverses the postponement recorded in [[project_flink_production]] — that doc's reasoning is
   still sound, but the feature landed, and this is its bill.
2. **`KafkaSinkBuilder` defaults `TransactionNamingStrategy` to `INCREMENTING`** — verified by
   `javap` on `flink-connector-kafka-5.0.0-2.2.jar`, whose static initializer assigns
   `DEFAULT = INCREMENTING`. INCREMENTING mints a **brand-new `transactional.id` on every
   checkpoint**, and a new transactional.id means a new **producer id** from the coordinator.
   8 sinks x 6 checkpoints/min ≈ **70k dead producer ids per day**.
3. **The broker keeps every one of them.** `transactional.id.expiration.ms` defaults to **7 DAYS**,
   and each retained `TransactionMetadata` carries the set of partitions its transaction touched —
   up to a few hundred `TopicPartition` objects each. Separately,
   `producer.id.expiration.ms` (**24h**) governs the per-partition producer state: the shutdown log
   showed **~1700 producer ids on every busy partition** across ~3000 partitions.
4. **The heap was Kafka's 1 GB default** — `KAFKA_HEAP_OPTS` was set in NEITHER compose file.
   Verified live: `docker exec kafka bash -c 'ps -ef | grep -o "\-Xmx[^ ]*"'` → `-Xmx1G`.

The corroborating detail: `__transaction_state` segments compacting **104 MB → 25 KB**. That ratio
is what a flood of single-use transactional ids looks like.

## Why it needed a human every time

The JVM **exits 0** after the OOM, and Docker restarts a container for neither `exit 0` nor a failed
healthcheck — so `restart: on-failure` never fired. `docker inspect` showed `ExitCode 0`,
`OOMKilled=false`, `restarts=0`. **`OOMKilled` is the KERNEL killer**, which never ran because the
container has no memory limit; it says nothing about a JVM heap OOM. The symptom surfaced as
`unhealthy` because the healthcheck (`kafka-topics --list`, a JVM start plus the metadata of ~3000
topics, 10s timeout, every 15s) is the first thing to time out on a sick broker.

## The fix that shipped

1. **`setTransactionNamingStrategy(TransactionNamingStrategy.POOLING)` on all 8 EXACTLY_ONCE sinks.**
   Reuses a small fixed pool of transactional ids instead of one per checkpoint — this is the actual
   cure, everything else is containment. ⚠ **One-way switch**: INCREMENTING → POOLING is supported,
   the reverse is not. POOLING's abort strategy is `LISTING`, which needs the `ListTransactions` API
   (Kafka 3.0+; the broker is cp-kafka 7.8.0, fine) — it would need ACLs on a secured cluster.
2. **`KAFKA_TRANSACTIONAL_ID_EXPIRATION_MS: 3600000`** (from 7 days). Still 6x the sinks' own 10m
   `transaction.timeout.ms` and 4x the 15m `transaction.max.timeout.ms` ceiling, so it cannot catch
   a live transaction. This is what drains the backlog already on disk.
3. **`KAFKA_PRODUCER_ID_EXPIRATION_MS: 3600000`** (from 24h), matching the 1h retention `warmup.sh`
   puts on the stage topics — no value in remembering a producer longer than its data.
4. **`KAFKA_HEAP_OPTS`** — `-Xmx2G -Xms512m` in `docker-compose.yml` (**host-agnostic: that file runs
   on 5+ dev boxes**, same constraint as the 2g TaskManager) and `-Xmx4G -Xms4G` in the prod file.
   Margin, not fix.

## Traps for whoever reads this next

- **Never diagnose this broker from the log tail.** The tail is a clean SIGTERM shutdown
  (`kafka.utils.Exit$.addShutdownHook`) with an `IllegalStateException: initiateShutdown() was not
  called before awaitShutdown()` — a benign known KRaft shutdown-order warning. The real story is
  ~5 minutes earlier. The first read of this incident went down that path.
- `OutOfOrderSequenceException` and `Timed out when requesting AllocateProducerIds` appear **after**
  the first OOM. Symptoms.
- **Do not measure producer churn while the broker is down.**
  `docker logs taskmanager-1 | grep -c 'ProducerId set to'` returned 0 only because nothing was
  producing.
- **The server runs `docker-compose.yml`, not `docker-compose.prod.yml`.** Assuming the prod file
  cost a wrong first answer here.

## Merged to `main` 2026-09-05 (PR #15)

Merged as `7dc3a50`, **+252/-0 — identical to the PR's own stat**, so the two conflicts cost nothing.
Both were mechanical:

- **`TypeValidatorJob.java`** — the branch forked at PR #12 and `main` has since taken PR #13
  (staleness), which re-indented and restructured that whole file and added a **ninth**,
  non-transactional `control-plane-sink`. Git produced one 95-line conflict out of what is a
  two-line change. Resolved by taking `main`'s file whole and re-applying only the import plus the
  two `setTransactionNamingStrategy` calls at `main`'s indentation — **never by hand-merging the
  hunk**, which would have reverted PR #13's reformatting.
- **`todo.md`** — parallel appends at EOF, both kept.

`mvn -o clean test` on `flink/normalizer`: 8 modules SUCCESS, all tests green. Both compose files
pass `docker compose config -q`. The JaCoCo `Unsupported class file major version 70` stack trace in
that output is pre-existing noise (it tries to instrument JDK classes) and fails nothing.

## Review notes on the fix itself

- **The coverage is complete.** `grep -rn setDeliveryGuarantee flink --include='*.java'` returns
  exactly 8 hits post-merge and all 8 now set POOLING. `flink/merger` and `flink/adjustment` have no
  `EXACTLY_ONCE` sink at all, and PR #13's `control-plane-sink` is non-transactional — nothing is
  missed, and the 8 will need re-checking only when a 9th transactional sink is added.
- **The 1h `transactional.id.expiration.ms` does NOT shrink the tolerable downtime**, which is the
  obvious worry. `transaction.timeout.ms` is already pinned to **10m** on every sink, and the broker
  aborts a transaction that exceeds it regardless of the id expiration — so downtime past ~10m
  already forfeits the in-flight transaction and did so before this change. The 1h figure is not the
  binding constraint on anything.
- **`-XX:+ExitOnOutOfMemoryError` is the missing half of "it needed a human every time."** The fix
  makes the OOM unlikely; it does not make the container recover if one happens anyway, because a
  heap OOM still exits **0** and `restart: on-failure` still will not fire. One JVM flag on
  `KAFKA_OPTS` turns every future occurrence into an automatic restart. Deliberately NOT done here —
  out of this PR's scope — see todo.

## Open

- **Nothing is verified live.** No deploy, and the broker was still down at the time of writing.
- **`kafka-exporter` exposes no JVM heap metric** and M9's rules cover Flink only, which is why this
  built up unseen for three days. There is no alert that would catch the next one.
- **The 10s checkpoint interval is untouched** — a deliberate scope call, not an endorsement. With
  EXACTLY_ONCE and `read_committed`, each of the 6 chained jobs only publishes on commit, so the
  interval is a **per-hop latency floor**: 6 hops x 10s. Worth a decision of its own.
- Partition count grows linearly with subscribed markets; whatever heap is set has a market count
  at which it fails again. See [[project_kafka_topic_strategy]].

## 2026-09-05 — POOLING deployed, and it broke all 8 transactional sinks

First live deploy of the POOLING change (dev server `192.168.150.31`, `/opt/data-collector`, commit
`8592241`). **It does not start.** Every one of the 6 normalizer jobs enters an endless restart loop;
`orderbook-merger` and `orderbook-adjustment` stay RUNNING only because they have no EXACTLY_ONCE
sink. Pipeline output is flat — `p1-asks` frozen while `ex1-raw` keeps growing ~34 rec/s.

```
java.lang.IllegalStateException: The record serializer does not expose a static list of target topics.
  at ExactlyOnceKafkaWriter.lambda$getTopicNames$5(ExactlyOnceKafkaWriter.java:373)
  at TransactionAbortStrategyContextImpl.getOpenTransactionsForTopics(...:83)
  at ExactlyOnceKafkaWriter.abortLingeringTransactions(...:331)
  at ExactlyOnceKafkaWriter.initialize(...:194)
```

**Mechanism, read off the 5.0.0-2.2 sources — this is structural, not a misconfiguration.**
`TransactionNamingStrategy` binds each naming strategy to an abort strategy:
`INCREMENTING -> PROBING`, `POOLING -> LISTING`. LISTING must enumerate the sink's target topics to
find transactions to abort, so it calls `getTopicNames()`, which `orElseThrow`s unless the record
serializer exposes a `KafkaDatasetIdentifier`. `initialize()` calls `abortLingeringTransactions()`
**unconditionally**, so this fires on a clean first submit with no state — the job can never start.

**Why every sink is hit: `setTopicSelector` never provides that identifier.** In
`KafkaRecordSerializationSchemaBuilder`, `setTopic("x")` wraps a `ConstantTopicSelector` whose
`getDatasetIdentifier()` returns `ofTopics([x])`; `setTopicSelector(...)` wraps a
`CachingTopicSelector` that returns the identifier **only if the wrapped selector itself implements
`KafkaDatasetIdentifierProvider`** — and a lambda cannot. All 8 EXACTLY_ONCE sinks route dynamically
by pair/exchange id, so all 8 fail. The one `setTopic` sink (job 2's `control-plane`) is
non-transactional and unaffected — coincidence, not design.

**The fix that keeps dynamic routing:** replace each topic-selector lambda with a named static class
implementing both `TopicSelector<T>` and `KafkaDatasetIdentifierProvider`, returning
`DefaultKafkaDatasetIdentifier.ofPattern(...)` — `getTopicNames()` accepts a **pattern**, not just a
fixed list, and resolves it via `AdminUtils.getTopicsByPattern`. Patterns are already implied by
[[project_kafka_topic_strategy]] (`^p\d+-(asks|bids)$`, `^ex\d+-p\d+-<stage>-flink$`). No static
enumeration of pairs, so no resubmit when a market is added. All three APIs are `@PublicEvolving`.

Costs to weigh before committing to it: `getTopicsByPattern` does a **full `listTopics()` per writer
`initialize()`**, i.e. per subtask per restart, and LISTING then lists transactions for every matched
topic — unmeasured at this cluster's ~3000 partitions. Make the pattern as narrow as the stage.

**Reverting POOLING -> INCREMENTING is the other option and is now cheaper than it was**, because the
broker-side half of the OOM fix (`transactional.id.expiration.ms=1h`, `producer.id.expiration.ms=1h`)
landed in the same change and is doing the real work — the 7-day default was the actual leak. The
code comment "one-way switch: INCREMENTING -> POOLING is supported, the reverse is not" is about
migrating *live* pooled state; it does not forbid reverting code that has never successfully run.

**Process lesson, and the reason this reached a server:** the change was reasoned about entirely at
the broker layer and shipped with "nothing is verified live" already written in this file. A sink
naming strategy is not a broker setting — it changes the writer's startup path, which no amount of
broker reasoning covers.

**And no verify step ran, because the deploy was the dev target.** The live stack on this box is
`docker-compose.yml`, not `docker-compose.prod.yml` (confirmed via the containers'
`com.docker.compose.project.config_files` label, and by there being one `taskmanager` rather than
`taskmanager-1..4`). So the deploy was `make run-all-jobs`, and **only the `prod-*` targets have a
verify step** — `run-all-jobs` submits and stops. `make prod-verify` would have failed this loudly
(it counts RUNNING jobs and would have seen 2 of 8). Worth deciding whether the dev targets should
gain the same assertion, since the dev stack is what actually runs here.

## 2026-09-05 (later) — reverted: checkpointing, EXACTLY_ONCE and POOLING all removed

User's call, to buy time to study checkpointing properly rather than to fix the POOLING breakage
under pressure. Everything the checkpointing round added on the job side is gone; the pipeline is
back to the pre-`ec8d35c` shape. Verified `git diff ec8d35c^` on all 6 jobs is now **only** the M4
producer block (see below) — no checkpointing residue.

What went, and where:

- `CheckpointingConfigurer` **deleted** (the whole `checkpointingConfigurer` package), and its
  `configure(env)` call removed from all 6 normalizer jobs.
- `setDeliveryGuarantee(EXACTLY_ONCE)`, `setTransactionalIdPrefix(...)` and
  `setTransactionNamingStrategy(POOLING)` removed from all 8 sinks — sinks are back to the
  `DeliveryGuarantee.NONE` default.
- `transaction.timeout.ms=600000` removed (transaction-only setting; it existed solely to stay under
  the broker's `transaction.max.timeout.ms`).
- `isolation.level=read_committed` removed from **all 7 Java KafkaSources** (5 normalizer + merger +
  adjustment) **and** `kgo.FetchIsolationLevel(kgo.ReadCommitted())` from the two Go consumers
  (`web/internal/kafka/consumer.go`, `e2e/consumer/consumer.go`). Nothing writes transactionally any
  more, so it was config asserting a guarantee that no longer exists. ⚠ **web and e2e are separate
  deployables and need rebuilding**, not just the Flink jars.
- Shared checkpoint volume + all 7 mounts removed from **both** compose files, the volume declaration
  with them; `/opt/flink/checkpoints` dropped from the Dockerfile's mkdir/chown stanza (ha/archive
  stay — they are M3/S4, not checkpointing).
- Prometheus `flink-checkpoints` rule group (3 alerts) and the Alertmanager inhibit rule +
  alertname matchers that referenced them removed. `promtool check rules` → 10 rules,
  `amtool check-config` → SUCCESS, both green.

**KEPT deliberately — do not assume these went with it:**

- **M4's `acks=all` + `enable.idempotence=true` + `retries` + `delivery.timeout.ms` on every sink.**
  These post-date `ec8d35c^`, which is why the jobs do not diff clean against it. The idempotent
  producer is **independent of transactions** — it needs `acks=all`, `retries>0` and
  `max.in.flight<=5`, all still true — and it is the setting that stops retries reordering writes
  and corrupting the book. Removing it was never part of "disable exactly-once".
- **The broker-side OOM fix** (`KAFKA_HEAP_OPTS`, `transactional.id.expiration.ms=1h`,
  `producer.id.expiration.ms=1h`). Moot now that nothing is transactional, but harmless, and it is
  the thing that must stay if EXACTLY_ONCE ever returns.

**⚠ THE ONE REAL REGRESSION THIS CREATES — job restarts.** Flink defaults `restart-strategy.type` to
`disable` **when checkpointing is off**, so with checkpointing gone a job that throws once goes to
FAILED and stays there. `docker-compose.prod.yml` sets an explicit `exponential-delay` strategy (M1)
and is fine. **`docker-compose.yml` sets NO restart strategy — and the dev compose is what the dev
server actually runs.** So on that box a transient Kafka blip now kills a job permanently and
silently. (`CheckpointingConfigurer`'s own comment claimed docker-compose.yml set a restart strategy;
it never did — that was always prod-only.) Raised with the user, not applied: adding M1's five keys
to the dev file is the fix and is ~5 lines. See todo.
