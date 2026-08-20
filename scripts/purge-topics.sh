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
#   ./scripts/purge-topics.sh --quiet     # only the summary, no per-topic table
#
# Every broker call is echoed before it runs, with elapsed time after it, because each one
# starts a JVM inside the container and can take seconds — silence here looks like a hang.
#
# ⚠ Purging Kafka does NOT reset Flink. The jobs keep their keyed state — job 2's lastSeq
#   and awaitingSnapshot, job 5's order books — so after a purge the pipeline still believes
#   everything it saw before. Resubmit the jobs (`make run-normalizer-jobs`) for a true reset.
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"

DRY_RUN=false
ASSUME_YES=false
QUIET=false
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=true ;;
        --yes|-y)  ASSUME_YES=true ;;
        --quiet|-q) QUIET=true ;;
        -h|--help) sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown argument: $arg (try --help)"; exit 1 ;;
    esac
done

# All diagnostics go to STDERR on purpose: kafka_run returns broker output on stdout via
# command substitution, so anything logged to stdout would be captured as data. That bug
# really happened — the log lines were counted as topics.
log()  { printf '%s  %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
step() { printf '\n%s  == %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die()  { printf '\n%s  ERROR: %s\n' "$(date +%H:%M:%S)" "$*" >&2; exit 1; }

# Runs a command in the Kafka container, echoing it first and timing it after. Every broker
# call goes through here so the script is never silent for more than one call.
kafka_run() {
    local label="$1"; shift
    local start=$SECONDS out rc
    log "  \$ ${*}"
    set +e
    out=$(docker exec "$KAFKA_CONTAINER" "$@" 2>&1)
    rc=$?
    set -e
    if [ $rc -ne 0 ]; then
        printf '%s\n' "$out" >&2
        die "$label failed (exit $rc) after $((SECONDS - start))s — see the broker output above."
    fi
    log "  -> ok in $((SECONDS - start))s"
    printf '%s' "$out"
}

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

step "Preflight"
log "container=$KAFKA_CONTAINER bootstrap=$KAFKA_BOOTSTRAP dry_run=$DRY_RUN"
docker inspect -f '{{.State.Running}}' "$KAFKA_CONTAINER" 2>/dev/null | grep -q true \
    || die "container '$KAFKA_CONTAINER' is not running (set KAFKA_CONTAINER to override)."
log "  -> container is up"

step "Listing topics"
all_topics=$(kafka_run "kafka-topics --list" \
    kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" --list)
topics=$(printf '%s\n' "$all_topics" | grep -Ev '^__' | grep -E "$TOPIC_PATTERN" | sort || true)
topic_count=$(printf '%s' "$topics" | grep -c . || true)
log "$(printf '%s' "$all_topics" | grep -c . || true) topics on the broker, $topic_count match the pipeline families"
[ "$topic_count" -eq 0 ] && { log "Nothing to purge."; exit 0; }

# TWO bulk calls for the whole broker, then filtered locally. Per topic this meant a JVM
# start each time — minutes of silence on a full market list.
#
# Both watermarks are needed, and using only the latest is wrong: kafka-delete-records does
# not move the END offset, it raises the START offset. A purged topic still reports a large
# latest offset forever, so `latest` counts records that were deleted long ago. The number
# of readable records is latest - earliest.
step "Reading offsets (two bulk calls)"
raw_latest=$(kafka_run "kafka-get-offsets (latest)" \
    kafka-get-offsets --bootstrap-server "$KAFKA_BOOTSTRAP" --topic-partitions '.*')
raw_earliest=$(kafka_run "kafka-get-offsets (earliest)" \
    kafka-get-offsets --bootstrap-server "$KAFKA_BOOTSTRAP" --topic-partitions '.*' --time -2)

# topic:partition:earliest:latest, for the pipeline topics only.
offsets=$(awk -F: -v pat="$TOPIC_PATTERN" '
    NR == FNR { if (NF == 3 && $3 != "") low[$1 ":" $2] = $3; next }
    NF == 3 && $3 != "" && $1 ~ pat {
        k = $1 ":" $2
        print $1 ":" $2 ":" (k in low ? low[k] : 0) ":" $3
    }' <(printf '%s\n' "$raw_earliest") <(printf '%s\n' "$raw_latest") || true)

resolved=$(printf '%s' "$offsets" | grep -c . || true)
log "$resolved partitions resolved across $topic_count topics"
[ "$resolved" -eq 0 ] && die "no partitions resolved — kafka-get-offsets returned nothing for these topics."

step "Plan"
total=0
pending=""
while IFS=':' read -r topic partition earliest_off end; do
    [ -z "${end:-}" ] && continue
    readable=$((end - earliest_off))
    if [ "$readable" -gt 0 ]; then
        total=$((total + readable))
        # The delete offset is the END offset: "remove everything up to the high watermark".
        pending+="$topic:$partition:$end"$'\n'
        [ "$QUIET" = false ] && printf '  %-46s p%-3s %10s records\n' "$topic" "$partition" "$readable" >&2
    fi
done <<< "$offsets"

log "$topic_count topics, $resolved partitions, $total records to delete"
[ "$total" -eq 0 ] && { log "All topics are already empty."; exit 0; }

if [ "$DRY_RUN" = true ]; then
    log "Dry run — nothing deleted."
    exit 0
fi

if [ "$ASSUME_YES" != true ]; then
    printf '\nDelete all %s records? This cannot be undone. [y/N] ' "$total" >&2
    read -r reply
    case "$reply" in
        [yY]|[yY][eE][sS]) ;;
        *) echo "Aborted."; exit 1 ;;
    esac
fi

step "Deleting records"
# One JSON for every partition, so the whole purge is a single broker round trip.
json=$(printf '%s' "$pending" | awk -F: 'BEGIN { printf "{\"partitions\":[" }
    NF == 3 { if (n++) printf ","; printf "{\"topic\":\"%s\",\"partition\":%s,\"offset\":%s}", $1, $2, $3 }
    END { printf "],\"version\":1}" }')
log "  writing offset file for $(printf '%s' "$pending" | grep -c .) partitions"
printf '%s' "$json" | docker exec -i "$KAFKA_CONTAINER" bash -c 'cat > /tmp/purge-offsets.json' \
    || die "could not write /tmp/purge-offsets.json into the container."
kafka_run "kafka-delete-records" \
    kafka-delete-records --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --offset-json-file /tmp/purge-offsets.json >/dev/null
docker exec "$KAFKA_CONTAINER" rm -f /tmp/purge-offsets.json || true

step "Verifying"
# After a successful delete the earliest offset equals the latest: nothing readable is left.
earliest=$(kafka_run "kafka-get-offsets --time -2" \
    kafka-get-offsets --bootstrap-server "$KAFKA_BOOTSTRAP" --topic-partitions '.*' --time -2)
remaining=$(awk -F: '
    NR == FNR { if (NF == 3) low[$1 ":" $2] = $3; next }    # pass 1: earliest, AFTER the delete
    NF == 4   { k = $1 ":" $2                               # pass 2: the plan (…:earliest:latest)
                if (k in low && low[k] + 0 < $4 + 0) r += $4 - low[k] }
    END { print r + 0 }' <(printf '%s\n' "$earliest") <(printf '%s\n' "$offsets"))
if [ "$remaining" -eq 0 ]; then
    log "  -> all matched partitions are empty"
else
    log "  -> WARNING: $remaining records still readable; re-run or check broker logs"
fi

step "Done"
log "Purged $total records from $topic_count topics in ${SECONDS}s."
cat >&2 <<'WARN'

⚠ Flink still holds its keyed state (job 2's lastSeq/awaitingSnapshot, job 5's books).
  For a true reset, resubmit the jobs:  make run-normalizer-jobs
WARN
