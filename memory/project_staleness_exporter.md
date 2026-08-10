---
name: staleness-exporter
description: lpa-staleness-exporter derives topic names from postgres and MUST mirror scripts/warmup.sh; the dead-letter is deliberately unmonitored and the serial poll loop has a scaling ceiling.
metadata:
    type: project
---

# The exporter's topic names are a mirror of warmup.sh

`lpa-staleness-exporter/exporter.py` builds its watch list from `exchange_markets`
(`WHERE status = 'subscribe'`) and **must produce exactly the names
`scripts/warmup.sh` creates**. If the two drift, the exporter silently watches topics
that do not exist — `check_topic` logs `topic not found on cluster` and reports
`stale=1`, which looks identical to a genuinely dead pipeline.

**That drift had actually happened.** Until 2026-08-10 the exporter derived
`ex{id}-p{id}-asks` / `-bids`, a naming warmup.sh has never created. Every DB-derived
series was a phantom pinned at `stale=1`. Fixed to derive:

- `ex{exchange_id}-p{pair_id}-{stage}` for the five stages in `NORMALIZER_STAGES`,
  which is copied from warmup.sh and verified equal to it string-for-string;
- `p{pair_id}-{asks,bids}` once per distinct subscribed pair.

**When a pipeline stage is added or renamed, `NORMALIZER_STAGES` must be edited in
both files.** There is no shared source of truth — [[project_kafka_topic_strategy]]
describes the naming, but nothing enforces it.

## The dead-letter is intentionally NOT monitored

`ex{id}-p{id}-rejected-flink` is excluded on purpose (user decision 2026-08-10). Its
staleness semantics are **inverted**: a silent dead-letter means nothing is being
rejected, i.e. the pipeline is healthy. Monitoring it the same way as the stages would
pin it at `stale=1` in the normal case. If dead-letter monitoring is ever wanted, it
needs the opposite check — alert on *traffic*, not on silence.

## Thresholds: per-row for stages, per-family for outputs

Stage topics inherit the row's `exchange_markets.staleness_threshold_seconds`
(`default_threshold_seconds` when NULL). The aggregated `p{pair}-{side}` topics
cannot — they are per-pair and are written whenever *any* exchange on that pair emits,
so they have no single row to inherit from. They use a new
`output_threshold_seconds` (10, matching the value the old hardcoded list used).
`ex{id}-raw` stays a hand-listed manual entry because it is per-exchange, not
per-subscription.

## `topic_source` selects where the watch list comes from (2026-08-10)

`both` (default) / `db` / `config`. **`db` ignores the `topics:` block outright** — the
user's explicit choice — so the hand-listed `ex{id}-raw` topics go unmonitored in that
mode; that is intended, not an oversight. `config` never opens a postgres connection at
all. An unrecognised value logs an error and falls back to `both` rather than failing
the process, because the exporter is a monitoring component: dying on a config typo
would take out the very thing that reports outages.

## Staleness episode metrics are in-memory and poll-resolution (2026-08-10)

An *episode* is one continuous stretch of `stale=1`. Five series expose its boundaries
so nobody needs a Prometheus range query: `..._stale_since_timestamp_seconds` (0 when
healthy), `..._last_recovery_timestamp_seconds`, `..._last_stale_duration_seconds`,
and the counters `..._stale_episodes_total` / `..._stale_seconds_total`.

Three properties worth knowing before trusting a number off a dashboard:

- **Poll-resolution only.** Transitions are observed solely at poll time, so a topic
  that goes stale and recovers inside one `poll_interval_seconds` (currently **60**) is
  never recorded at all, and every timestamp/duration is ±1 interval. A whole interval
  is attributed to the state the topic was in when that interval *started*.
- **In-memory** (user decision): counters reset to 0 and an in-flight episode is
  forgotten on restart. Deliberate — `rate()`/`increase()` handle counter resets, and
  the durable history lives in Prometheus. Persisting would need a writable volume,
  which the compose service does not have (config.yaml is mounted read-only).
- **Every series is zero-initialised on first sight of a topic.** Without that, a topic
  that has never gone stale would export no episode series, which Prometheus cannot
  tell apart from a topic the exporter does not know about.

## Known scaling ceiling (flagged, deliberately not fixed)

The poll loop is **serial**: per topic it does `partitions_for_topic`, then per
partition `assign` + `end_offsets` + possibly `seek` + `poll(timeout_ms=3000)`. The
2026-08-10 change took DB-derived topics from 2 per subscription to 5, so the loop is
~2.5× longer and can overrun `poll_interval_seconds: 15`, after which polls just run
back to back and staleness resolution silently degrades. The user chose to flag rather
than fix. If it needs fixing, note `kafka-python` consumers are **not thread-safe** —
parallelising needs one consumer per worker, not a shared one.

## Consumers of these metrics live outside this repo

No Grafana or Prometheus config is committed here; `docker-compose.yml` only exposes
port 9309. The rename changed the `topic=` label values, so **external dashboards and
the `KafkaTopicStale` alert rule (see the alert-gateway routing in
[[alert-gateway]]) must be updated by hand** — nothing in this repo will flag it.
