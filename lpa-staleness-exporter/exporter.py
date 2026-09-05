"""
Kafka topic staleness exporter.

Topic list comes from two sources, selected by config.yaml `topic_source:`
("both" the default, "db", or "config"):

  1. Postgres (exchange_markets, joined with markets/exchanges), producing the
     same topic names scripts/warmup.sh creates:
       - ex{exchange_id}-p{pair_id}-{stage} for each of the five normalizer
         stages, per active subscription, using that row's
         exchange_markets.staleness_threshold_seconds (falling back to
         default_threshold_seconds when NULL);
       - p{pair_id}-{asks,bids} once per distinct active pair, using
         output_threshold_seconds.
  2. config.yaml `topics:` — manually listed topics, for anything not derivable
     from a subscription row (the per-exchange ex{id}-raw ingest topics).
     These always win over a same-named DB entry.

The DB is re-queried every db_refresh_interval_seconds so new/removed
subscriptions are picked up without restarting the exporter; metrics for
topics that disappear from the merged list are removed so Grafana doesn't
keep showing dead rows.

For each topic in the merged list, exposes:

  kafka_topic_seconds_since_last_message{topic="..."}   -> float seconds
  kafka_topic_stale{topic="..."}                        -> 0/1 (per-topic threshold)
  kafka_topic_staleness_threshold_seconds{topic="..."}  -> effective threshold
  kafka_topic_last_message_timestamp_seconds{topic="..."} -> unix ts (0 if never seen)

Plus the staleness episode history — an episode is one continuous stretch of
stale=1, and these expose its boundaries without needing a Prometheus range query:

  kafka_topic_stale_since_timestamp_seconds{topic="..."}    -> start of the CURRENT
                                                              episode, 0 if healthy
  kafka_topic_last_recovery_timestamp_seconds{topic="..."}  -> when it last came back
  kafka_topic_last_stale_duration_seconds{topic="..."}      -> length of the last
                                                              COMPLETED episode
  kafka_topic_stale_episodes_total{topic="..."}             -> counter, episodes seen
  kafka_topic_stale_seconds_total{topic="..."}              -> counter, cumulative
                                                              seconds spent stale

Transitions are only observed at poll time, so every timestamp and duration above is
accurate to within one poll_interval_seconds.

Every completed episode is additionally persisted as a row in the Postgres table
topic_staleness_episodes (topic, stale_since, recovered_at, duration_seconds,
threshold_seconds) — this is the source for historical "which topic was down from
when to when" reporting in Grafana, since the Gauges above only ever expose the
CURRENT and the single most-recently-completed episode. The table is created
automatically on startup (see run_migrations); no manual migration step is needed.
An in-flight episode open when the exporter restarts is recovered from this table
at startup (see load_open_episodes), so it is not lost — only the Prometheus
Counters (kafka_topic_stale_episodes, kafka_topic_stale_seconds) reset to 0 on
restart, since those live in process memory only.

A topic has at most one open episode, enforced by a unique partial index. A topic
that leaves the watch list mid-episode has that episode closed at drop time, so
"dropped while stale" and "recovered" are indistinguishable in the table — an
open row always means genuinely still stale, never a bookkeeping leak.

This persistence runs regardless of topic_source: `config` mode still skips
postgres for the TOPIC LIST, but episode history is written whenever a
`postgres:` block is configured.

A topic with no partitions found, or with only empty partitions, is
reported as stale=1 and seconds_since=-1 (meaning "unknown/never").
"""

import logging
import time

import psycopg2
import yaml
from kafka import KafkaConsumer, TopicPartition
from prometheus_client import Counter, Gauge, start_http_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("lpa-staleness-exporter")

CONFIG_PATH = "/config/config.yaml"
METRICS_PORT = 9309

g_seconds_since = Gauge(
    "kafka_topic_seconds_since_last_message",
    "Seconds since the last message was produced to this topic (-1 = unknown/never)",
    ["topic"],
)
g_stale = Gauge(
    "kafka_topic_stale",
    "1 if topic exceeded its configured staleness threshold, else 0",
    ["topic"],
)
g_threshold = Gauge(
    "kafka_topic_staleness_threshold_seconds",
    "Effective staleness threshold for this topic",
    ["topic"],
)
g_last_ts = Gauge(
    "kafka_topic_last_message_timestamp_seconds",
    "Unix timestamp (seconds) of the last message seen in this topic",
    ["topic"],
)

g_stale_since = Gauge(
    "kafka_topic_stale_since_timestamp_seconds",
    "Unix timestamp when the topic's CURRENT stale episode began (0 if not stale)",
    ["topic"],
)
g_last_recovery = Gauge(
    "kafka_topic_last_recovery_timestamp_seconds",
    "Unix timestamp when the topic last recovered from a stale episode (0 if never)",
    ["topic"],
)
g_last_duration = Gauge(
    "kafka_topic_last_stale_duration_seconds",
    "Duration of the last COMPLETED stale episode (0 if none has completed yet)",
    ["topic"],
)
c_episodes = Counter(
    "kafka_topic_stale_episodes",
    "Number of times this topic has entered a stale episode",
    ["topic"],
)
c_stale_seconds = Counter(
    "kafka_topic_stale_seconds",
    "Cumulative seconds this topic has spent stale",
    ["topic"],
)

ALL_METRICS = (
    g_seconds_since, g_stale, g_threshold, g_last_ts,
    g_stale_since, g_last_recovery, g_last_duration,
    c_episodes, c_stale_seconds,
)

# enum values if they differ (e.g. 'subscribed' vs 'active').
DB_QUERY = """
    SELECT em.exchange_id, m.id AS pair_id, em.staleness_threshold_seconds
    FROM exchange_markets em
    JOIN markets m ON em.market_id = m.id
    WHERE em.status = 'subscribe'
"""

# Per-exchange+pair pipeline stages, mirroring NORMALIZER_STAGES in scripts/warmup.sh.
# Each entry is one raw-pipeline job's output topic, so a stalled stage points at the
# job that stopped emitting.
#
# ex{id}-p{id}-rejected-flink is deliberately NOT monitored here: a silent dead-letter
# means the pipeline is healthy, so a staleness check on it would report stale=1
# permanently in the good case.
TOPIC_SOURCES = ("db", "config", "both")

NORMALIZER_STAGES = (
    "raw-flink",                 # job 1 pair-extractor  out
    "type-validated-raw-flink",  # job 2 type-validator  out
    "rebased-flink",             # job 3 rebaser         out
    "applied-precision-flink",   # job 4 precision       out
    "orderbook-snapshot-flink",  # job 5 book-builder    out
)


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def build_pg_dsn(pg_cfg):
    if not pg_cfg:
        return None
    return (
        f"host={pg_cfg['host']} port={pg_cfg.get('port', 5432)} "
        f"dbname={pg_cfg['db']} user={pg_cfg['user']} password={pg_cfg['password']}"
    )


def run_migrations(pg_dsn):
    """Create/upgrade the episode-history table. Called once on startup — no manual
    migration step is needed, this is idempotent and safe to run on every boot."""
    if not pg_dsn:
        log.warning("no postgres configured — staleness episode history will not be persisted")
        return
    try:
        with psycopg2.connect(pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS topic_staleness_episodes (
                        id SERIAL PRIMARY KEY,
                        topic TEXT NOT NULL,
                        stale_since TIMESTAMPTZ NOT NULL,
                        recovered_at TIMESTAMPTZ,
                        duration_seconds DOUBLE PRECISION,
                        threshold_seconds DOUBLE PRECISION,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_tse_topic_time
                    ON topic_staleness_episodes (topic, stale_since)
                """)
                # Partial index: fast lookup of "the currently-open episode for topic X",
                # used by close_episode and by load_open_episodes on restart. UNIQUE so
                # a topic can never have two open episodes — that state is unrecoverable
                # (close_episode only ever closes the newest, so the older row would stay
                # open forever and the topic would read as permanently down). The older
                # non-unique index is dropped by name so existing deployments upgrade.
                cur.execute("DROP INDEX IF EXISTS idx_tse_open")
                cur.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_tse_open_unique
                    ON topic_staleness_episodes (topic)
                    WHERE recovered_at IS NULL
                """)
        log.info("topic_staleness_episodes table ready")
    except Exception as e:
        log.error("failed to run episode-table migration: %s", e)


def load_open_episodes(pg_dsn):
    """On startup, recover any episode that was still open (recovered_at IS NULL)
    when the exporter last stopped, so its original stale_since is preserved instead
    of a fresh one being started at process boot."""
    open_episodes = {}
    if not pg_dsn:
        return open_episodes
    try:
        with psycopg2.connect(pg_dsn) as conn:
            with conn.cursor() as cur:
                # ::float8 is load-bearing: on postgres >= 14 EXTRACT returns numeric,
                # which psycopg2 hands back as Decimal, and stale_since is later
                # subtracted from a time.time() float — that raises TypeError.
                cur.execute("""
                    SELECT DISTINCT ON (topic)
                           topic, extract(epoch FROM stale_since)::float8
                    FROM topic_staleness_episodes
                    WHERE recovered_at IS NULL
                    ORDER BY topic, id DESC
                """)
                for topic, stale_since in cur.fetchall():
                    open_episodes[topic] = stale_since
    except Exception as e:
        log.error("failed to load open episodes from postgres: %s", e)
    return open_episodes


def insert_episode_start(pg_dsn, topic, stale_since_ts, threshold):
    if not pg_dsn:
        return
    try:
        with psycopg2.connect(pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO topic_staleness_episodes
                        (topic, stale_since, threshold_seconds)
                    VALUES (%s, to_timestamp(%s), %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (topic, stale_since_ts, threshold),
                )
    except Exception as e:
        log.error("failed to persist episode start for %s: %s", topic, e)


def close_episode(pg_dsn, topic, recovered_at_ts, duration):
    if not pg_dsn:
        return
    try:
        with psycopg2.connect(pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE topic_staleness_episodes
                    SET recovered_at = to_timestamp(%s),
                        duration_seconds = %s
                    WHERE id = (
                        SELECT id FROM topic_staleness_episodes
                        WHERE topic = %s AND recovered_at IS NULL
                        ORDER BY id DESC LIMIT 1
                    )
                    """,
                    (recovered_at_ts, duration, topic),
                )
    except Exception as e:
        log.error("failed to persist episode close for %s: %s", topic, e)


def fetch_db_topics(pg_dsn, default_threshold, output_threshold):
    """Returns {topic_name: threshold_seconds} derived from exchange_markets.

    Two families, both named exactly as scripts/warmup.sh creates them:
      - ex{exchange_id}-p{pair_id}-{stage}, one per NORMALIZER_STAGES entry per
        subscribed row, carrying that row's threshold;
      - p{pair_id}-{asks,bids}, once per distinct subscribed pair. The aggregated
        book is per-pair and is written whenever any exchange on that pair emits,
        so it is not tied to a single subscription's threshold.
    """
    topics = {}
    if not pg_dsn:
        return topics
    try:
        with psycopg2.connect(pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(DB_QUERY)
                rows = cur.fetchall()
    except Exception as e:
        log.error("failed to refresh topic list from postgres: %s", e)
        return topics

    for exchange_id, pair_id, threshold in rows:
        t = threshold if threshold is not None else default_threshold
        for stage in NORMALIZER_STAGES:
            topics[f"ex{exchange_id}-p{pair_id}-{stage}"] = t

    for pair_id in {pair_id for _, pair_id, _ in rows}:
        for side in ("asks", "bids"):
            topics[f"p{pair_id}-{side}"] = output_threshold

    return topics


def resolve_topic_source(cfg):
    """Validate topic_source, falling back to 'both' on an unknown value."""
    source = cfg.get("topic_source", "both")
    if source not in TOPIC_SOURCES:
        log.error(
            "invalid topic_source %r (expected one of %s), falling back to 'both'",
            source, ", ".join(sorted(TOPIC_SOURCES)),
        )
        return "both"
    return source


def build_topic_config(cfg, pg_dsn):
    source = resolve_topic_source(cfg)
    default_threshold = cfg.get("default_threshold_seconds", 60)
    output_threshold = cfg.get("output_threshold_seconds", 10)

    # In 'config' mode postgres is never queried; in 'db' mode the manual block is
    # ignored outright, so an ex{id}-raw entry there stops being monitored.
    db_topics = (
        fetch_db_topics(pg_dsn, default_threshold, output_threshold)
        if source in ("db", "both") else {}
    )
    manual_topics = (cfg.get("topics") or {}) if source in ("config", "both") else {}

    combined = {**db_topics, **manual_topics}  # manual entries win on name collision
    log.info(
        "topic list refreshed (topic_source=%s): %d from db, %d manual, %d total",
        source, len(db_topics), len(manual_topics), len(combined),
    )
    return combined


def latest_offset_and_ts(consumer, tp, cache):
    """Return (end_offset, last_ts) for a single partition, refetching the
    last message only if the end offset advanced since the previous check."""
    consumer.assign([tp])
    end = consumer.end_offsets([tp])[tp]
    begin = consumer.beginning_offsets([tp])[tp]

    if end <= begin:
        return end, None  # partition has no messages at all

    cached_end, cached_ts = cache.get(tp, (None, None))
    if cached_end == end and cached_ts is not None:
        return end, cached_ts  # nothing new since last poll

    consumer.seek(tp, end - 1)
    records = consumer.poll(timeout_ms=3000, max_records=1).get(tp, [])
    if not records:
        return end, cached_ts  # transient fetch miss, reuse old value

    ts = records[-1].timestamp / 1000.0
    return end, ts


def init_topic_history(topic, history, stale_since=None):
    """Establish every episode series at 0 on first sight (or restore stale_since on
    restart-recovery). Without this a topic that has never gone stale exports no
    episode series at all, which in Prometheus is indistinguishable from a topic the
    exporter does not know about."""
    history[topic] = {"stale_since": stale_since, "last_eval": None}
    g_last_recovery.labels(topic=topic).set(0)
    g_last_duration.labels(topic=topic).set(0)
    c_episodes.labels(topic=topic).inc(0)
    c_stale_seconds.labels(topic=topic).inc(0)
    g_stale_since.labels(topic=topic).set(stale_since or 0)


def record_staleness(topic, is_stale, threshold, now, history, pg_dsn):
    """Track stale/healthy transitions for one topic, export the episode metrics, and
    persist each episode's boundaries to Postgres for historical reporting.

    Called once per topic per poll. A whole poll interval is attributed to the state
    the topic was in when that interval STARTED — a transition happening inside an
    interval is not observable at this resolution, so every timestamp and duration
    here is accurate only to within one poll_interval_seconds.
    """
    st = history.get(topic)
    if st is None:
        init_topic_history(topic, history)
        st = history[topic]

    was_stale = st["stale_since"] is not None

    if was_stale and st["last_eval"] is not None:
        c_stale_seconds.labels(topic=topic).inc(now - st["last_eval"])

    if is_stale and not was_stale:
        st["stale_since"] = now
        c_episodes.labels(topic=topic).inc()
        log.info("topic went stale: %s", topic)
        insert_episode_start(pg_dsn, topic, now, threshold)
    elif not is_stale and was_stale:
        duration = now - st["stale_since"]
        st["stale_since"] = None
        g_last_duration.labels(topic=topic).set(duration)
        g_last_recovery.labels(topic=topic).set(now)
        log.info("topic recovered: %s (stale for %.1fs)", topic, duration)
        close_episode(pg_dsn, topic, now, duration)

    g_stale_since.labels(topic=topic).set(st["stale_since"] or 0)
    st["last_eval"] = now


def check_topic(consumer, topic, threshold, partitions_by_topic, offset_cache, history, pg_dsn):
    now = time.time()
    g_threshold.labels(topic=topic).set(threshold)

    partitions = partitions_by_topic.get(topic)
    if not partitions:
        log.warning("topic not found on cluster: %s", topic)
        g_seconds_since.labels(topic=topic).set(-1)
        g_stale.labels(topic=topic).set(1)
        g_last_ts.labels(topic=topic).set(0)
        record_staleness(topic, True, threshold, now, history, pg_dsn)
        return

    last_ts = None
    for p in partitions:
        tp = TopicPartition(topic, p)
        try:
            end, ts = latest_offset_and_ts(consumer, tp, offset_cache)
            offset_cache[tp] = (end, ts)
        except Exception as e:
            log.error("error checking %s: %s", tp, e)
            continue
        if ts is not None and (last_ts is None or ts > last_ts):
            last_ts = ts

    if last_ts is None:
        g_seconds_since.labels(topic=topic).set(-1)
        g_stale.labels(topic=topic).set(1)
        g_last_ts.labels(topic=topic).set(0)
        record_staleness(topic, True, threshold, now, history, pg_dsn)
        return

    seconds_since = time.time() - last_ts
    is_stale = seconds_since > threshold
    g_seconds_since.labels(topic=topic).set(seconds_since)
    g_last_ts.labels(topic=topic).set(last_ts)
    g_stale.labels(topic=topic).set(1 if is_stale else 0)
    record_staleness(topic, is_stale, threshold, now, history, pg_dsn)


def drop_topic_metrics(topic, offset_cache, history, pg_dsn):
    # A topic dropped mid-episode must have that episode closed, otherwise the row
    # stays recovered_at IS NULL forever: the history panel reads it as still down,
    # every restart resurrects it, and if the topic is ever re-added its new episode
    # can never be closed past the orphan.
    st = history.get(topic)
    if st and st["stale_since"] is not None:
        now = time.time()
        log.info("topic dropped while stale, closing episode: %s", topic)
        close_episode(pg_dsn, topic, now, now - st["stale_since"])

    for m in ALL_METRICS:
        try:
            m.remove(topic)
        except KeyError:
            pass
    for tp in [tp for tp in offset_cache if tp.topic == topic]:
        del offset_cache[tp]
    history.pop(topic, None)


def main():
    cfg = load_config()
    bootstrap = cfg["kafka"]["bootstrap_servers"]
    poll_interval = cfg.get("poll_interval_seconds", 15)
    db_refresh_interval = cfg.get("db_refresh_interval_seconds", 60)
    pg_dsn = build_pg_dsn(cfg.get("postgres"))

    run_migrations(pg_dsn)

    log.info("connecting to kafka at %s", bootstrap)
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        client_id="lpa-staleness-exporter",
    )

    offset_cache = {}
    history = {}
    topics_cfg = {}
    last_db_refresh = 0.0

    open_episodes = load_open_episodes(pg_dsn)
    for topic, stale_since in open_episodes.items():
        init_topic_history(topic, history, stale_since=stale_since)
        c_episodes.labels(topic=topic).inc()  # this episode is active as of restart
        log.info("recovered open episode for %s (stale since %s)", topic, stale_since)
    # Seed known_topics with the recovered set so the first refresh below drops (and
    # closes the episode of) any topic that is no longer in the watch list. Starting
    # from an empty set would leave those exporting metrics nothing ever checks.
    known_topics = set(open_episodes)

    start_http_server(METRICS_PORT)
    log.info("metrics available on :%d/metrics", METRICS_PORT)

    while True:
        now = time.time()
        if now - last_db_refresh >= db_refresh_interval or not topics_cfg:
            topics_cfg = build_topic_config(cfg, pg_dsn)
            last_db_refresh = now

            removed = known_topics - set(topics_cfg)
            for t in removed:
                log.info("topic no longer active, dropping metrics: %s", t)
                drop_topic_metrics(t, offset_cache, history, pg_dsn)
            known_topics = set(topics_cfg)

        partitions_by_topic = {t: consumer.partitions_for_topic(t) for t in topics_cfg}
        for topic, threshold in topics_cfg.items():
            try:
                check_topic(consumer, topic, threshold, partitions_by_topic, offset_cache, history, pg_dsn)
            except Exception as e:
                log.error("failed checking topic %s: %s", topic, e)

        time.sleep(poll_interval)


if __name__ == "__main__":
    main()