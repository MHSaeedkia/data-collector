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

## Open

- **Nothing is verified live.** No deploy, and the broker was still down at the time of writing.
- **`kafka-exporter` exposes no JVM heap metric** and M9's rules cover Flink only, which is why this
  built up unseen for three days. There is no alert that would catch the next one.
- **The 10s checkpoint interval is untouched** — a deliberate scope call, not an endorsement. With
  EXACTLY_ONCE and `read_committed`, each of the 6 chained jobs only publishes on commit, so the
  interval is a **per-hop latency floor**: 6 hops x 10s. Worth a decision of its own.
- Partition count grows linearly with subscribed markets; whatever heap is set has a market count
  at which it fails again. See [[project_kafka_topic_strategy]].
