"""
Kafka topic staleness exporter.

Topic list comes from two merged sources:

  1. Postgres (exchange_markets, joined with markets/exchanges) — one
     ex{exchange_id}-p{pair_id}-asks / -bids pair per active subscription,
     using exchange_markets.staleness_threshold_seconds as the threshold
     (falling back to default_threshold_seconds when NULL).
  2. config.yaml `topics:` — manually listed topics (consolidated outputs,
     raw topics, normalizer stages, anything not tied to one exchange+pair
     row). These always win over a same-named DB entry.

The DB is re-queried every db_refresh_interval_seconds so new/removed
subscriptions are picked up without restarting the exporter; metrics for
topics that disappear from the merged list are removed so Grafana doesn't
keep showing dead rows.

For each topic in the merged list, exposes:

  kafka_topic_seconds_since_last_message{topic="..."}   -> float seconds
  kafka_topic_stale{topic="..."}                        -> 0/1 (per-topic threshold)
  kafka_topic_staleness_threshold_seconds{topic="..."}  -> effective threshold
  kafka_topic_last_message_timestamp_seconds{topic="..."} -> unix ts (0 if never seen)

A topic with no partitions found, or with only empty partitions, is
reported as stale=1 and seconds_since=-1 (meaning "unknown/never").
"""

import logging
import time

import psycopg2
import yaml
from kafka import KafkaConsumer, TopicPartition
from prometheus_client import Gauge, start_http_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("kafka-staleness-exporter")

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

ALL_GAUGES = (g_seconds_since, g_stale, g_threshold, g_last_ts)

# enum values if they differ (e.g. 'subscribed' vs 'active').
DB_QUERY = """
    SELECT em.exchange_id, m.id AS pair_id, em.staleness_threshold_seconds
    FROM exchange_markets em
    JOIN markets m ON em.market_id = m.id
    WHERE em.status = 'subscribe'
"""


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


def fetch_db_topics(pg_dsn, default_threshold):
    """Returns {topic_name: threshold_seconds} derived from exchange_markets."""
    topics = {}
    if not pg_dsn:
        return topics
    try:
        with psycopg2.connect(pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(DB_QUERY)
                for exchange_id, pair_id, threshold in cur.fetchall():
                    t = threshold if threshold is not None else default_threshold
                    topics[f"ex{exchange_id}-p{pair_id}-asks"] = t
                    topics[f"ex{exchange_id}-p{pair_id}-bids"] = t
    except Exception as e:
        log.error("failed to refresh topic list from postgres: %s", e)
    return topics


def build_topic_config(cfg, pg_dsn):
    default_threshold = cfg.get("default_threshold_seconds", 60)
    db_topics = fetch_db_topics(pg_dsn, default_threshold)
    manual_topics = cfg.get("topics") or {}
    combined = {**db_topics, **manual_topics}  # manual entries win on name collision
    log.info(
        "topic list refreshed: %d from db, %d manual, %d total",
        len(db_topics), len(manual_topics), len(combined),
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


def check_topic(consumer, topic, threshold, partitions_by_topic, offset_cache):
    partitions = partitions_by_topic.get(topic)
    if not partitions:
        log.warning("topic not found on cluster: %s", topic)
        g_seconds_since.labels(topic=topic).set(-1)
        g_stale.labels(topic=topic).set(1)
        g_threshold.labels(topic=topic).set(threshold)
        g_last_ts.labels(topic=topic).set(0)
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

    g_threshold.labels(topic=topic).set(threshold)

    if last_ts is None:
        g_seconds_since.labels(topic=topic).set(-1)
        g_stale.labels(topic=topic).set(1)
        g_last_ts.labels(topic=topic).set(0)
        return

    seconds_since = time.time() - last_ts
    g_seconds_since.labels(topic=topic).set(seconds_since)
    g_last_ts.labels(topic=topic).set(last_ts)
    g_stale.labels(topic=topic).set(1 if seconds_since > threshold else 0)


def drop_topic_metrics(topic, offset_cache):
    for g in ALL_GAUGES:
        try:
            g.remove(topic)
        except KeyError:
            pass
    for tp in [tp for tp in offset_cache if tp.topic == topic]:
        del offset_cache[tp]


def main():
    cfg = load_config()
    bootstrap = cfg["kafka"]["bootstrap_servers"]
    poll_interval = cfg.get("poll_interval_seconds", 15)
    db_refresh_interval = cfg.get("db_refresh_interval_seconds", 60)
    pg_dsn = build_pg_dsn(cfg.get("postgres"))

    log.info("connecting to kafka at %s", bootstrap)
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        client_id="kafka-staleness-exporter",
    )

    offset_cache = {}
    known_topics = set()
    topics_cfg = {}
    last_db_refresh = 0.0

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
                drop_topic_metrics(t, offset_cache)
            known_topics = set(topics_cfg)

        partitions_by_topic = {t: consumer.partitions_for_topic(t) for t in topics_cfg}
        for topic, threshold in topics_cfg.items():
            try:
                check_topic(consumer, topic, threshold, partitions_by_topic, offset_cache)
            except Exception as e:
                log.error("failed checking topic %s: %s", topic, e)

        time.sleep(poll_interval)


if __name__ == "__main__":
    main()