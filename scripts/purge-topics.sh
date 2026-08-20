#!/usr/bin/env bash
#
# Purges the DATA out of every pipeline topic, leaving the topics themselves — their
# partition count, retention config and the Schema Registry subjects — untouched.
#
# The counterpart to warmup.sh: warmup CREATES the topics, this one EMPTIES them. Use it
# when you want a clean run without `make refresh-normalizer`, which does `docker compose
# down -v` and destroys the registry, the postgres volume and the Kafka data together.
#
# Deletion is done with kafka-delete-records, which moves each partition's low watermark up
# to its high watermark. That is a real deletion, not a retention trick: it is immediate,
# and it does NOT need the topic to be recreated or the broker restarted.
#
#   ./scripts/purge-topics.sh --dry-run   # list what would be purged, delete nothing
#   ./scripts/purge-topics.sh             # prompt, then purge
#   ./scripts/purge-topics.sh --yes       # purge without prompting (CI / scripted use)
#
# ⚠ Purging Kafka does NOT reset Flink. The jobs keep their keyed state — job 2's lastSeq
#   and awaitingSnapshot, job 5's order books — so after a purge the pipeline still believes
#   everything it saw before. Resubmit the jobs (`make run-normalizer-jobs`) for a true reset.
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"

DRY_RUN=false
ASSUME_YES=false
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=true ;;
        --yes|-y)  ASSUME_YES=true ;;
        -h|--help) sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown argument: $arg (try --help)"; exit 1 ;;
    esac
done

# Must stay in step with warmup.sh's NORMALIZER_STAGES — same duplication, same drift trap
# as the exporter's copy. A stage missing here is silently NOT purged.
NORMALIZER_STAGES=(
    raw-flink
    type-validated-raw-flink
    rebased-flink
    applied-precision-flink
    orderbook-snapshot-flink
)
stages_alt=$(IFS='|'; echo "${NORMALIZER_STAGES[*]}")

# Every family warmup.sh creates. Matching the live topic list rather than re-deriving from
# postgres is deliberate: it also catches topics for markets that have since been
# unsubscribed, which are exactly the ones left holding stale data.
TOPIC_PATTERN="^(control-plane|ex[0-9]+-raw|ex[0-9]+-p[0-9]+-(${stages_alt}|rejected-flink)|p[0-9]+-(asks|bids)(-merged)?)$"

kafka() { docker exec "$KAFKA_CONTAINER" "$@"; }

echo "Listing topics on $KAFKA_BOOTSTRAP..."
topics=$(kafka kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" --list \
         | grep -Ev '^__' | grep -E "$TOPIC_PATTERN" | sort || true)

if [ -z "$topics" ]; then
    echo "No pipeline topics found — nothing to purge."
    exit 0
fi

# kafka-get-offsets prints `topic:partition:highWatermark`, which is both the cut point for
# the delete and the record count, so one call does the plan and the report.
echo "Reading end offsets..."
offsets=""
while read -r topic; do
    line=$(kafka kafka-get-offsets \
        --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$topic" 2>/dev/null || true)
    [ -n "$line" ] && offsets+="$line"$'\n'
done <<< "$topics"

offsets=$(echo "$offsets" | grep -v '^$' || true)
if [ -z "$offsets" ]; then
    echo "No partitions resolved — nothing to purge."
    exit 0
fi

total=0
pending=""
while IFS=':' read -r topic partition end; do
    [ -z "${end:-}" ] && continue
    if [ "$end" -gt 0 ]; then
        total=$((total + end))
        pending+="$topic:$partition:$end"$'\n'
    fi
    printf '  %-46s p%-3s %10s records\n' "$topic" "$partition" "$end"
done <<< "$offsets"

echo
echo "$(echo "$topics" | wc -l | tr -d ' ') topics, $total records to delete."

if [ "$total" -eq 0 ]; then
    echo "All topics are already empty."
    exit 0
fi

if [ "$DRY_RUN" = true ]; then
    echo "Dry run — nothing deleted."
    exit 0
fi

if [ "$ASSUME_YES" != true ]; then
    printf 'Delete all %s records? This cannot be undone. [y/N] ' "$total"
    read -r reply
    case "$reply" in
        [yY]|[yY][eE][sS]) ;;
        *) echo "Aborted."; exit 1 ;;
    esac
fi

# One JSON for every partition, so the whole purge is a single broker round trip.
json=$(printf '%s' "$pending" | awk -F: 'BEGIN { printf "{\"partitions\":[" }
    NF == 3 { if (n++) printf ","; printf "{\"topic\":\"%s\",\"partition\":%s,\"offset\":%s}", $1, $2, $3 }
    END { printf "],\"version\":1}" }')

echo "$json" | docker exec -i "$KAFKA_CONTAINER" bash -c 'cat > /tmp/purge-offsets.json'
kafka kafka-delete-records \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --offset-json-file /tmp/purge-offsets.json
kafka rm -f /tmp/purge-offsets.json

echo
echo "Purged $total records."
echo
echo "⚠ Flink still holds its keyed state (job 2's lastSeq/awaitingSnapshot, job 5's books)."
echo "  For a true reset, resubmit the jobs:  make run-normalizer-jobs"
