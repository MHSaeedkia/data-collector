#!/usr/bin/env bash
set -euo pipefail

# Clears the pipeline's in-memory state so a scenario can be run in isolation.
#
# Every manual-test scenario targets the SAME pair (BTC-USDT, pair_id 1), so the only thing
# separating one scenario from the next is job state. Three jobs are stateful and must be
# recycled; the other three (pair-extractor, rebaser, precision) are stateless and are left
# alone:
#
#   normalizer-aggregator      per-exchange book + last event_time (keyed by pair+side)
#   normalizer-book-builder    the order book itself (MapState per side)
#   normalizer-type-validator  {lastSeq, awaitingSnapshot}   <- the one scenario 01 depends on
#
# There are no checkpoints configured anywhere in this platform, so cancel+resubmit IS the
# state reset. Jobs are cancelled and resubmitted DOWNSTREAM-FIRST (aggregator -> 5 -> 2)
# because every source reads `latest`: an upstream job restarted first would emit into a topic
# nobody is reading yet.
#
# Resubmission reuses the jar already uploaded to Flink, so there is no Maven rebuild. That
# means the jars must have been submitted at least once (`run-job.sh` / `make refresh-normalizer`).
#
# Usage: ./reset.sh
# Env overrides: FLINK_API, SETTLE

FLINK_API="${FLINK_API:-http://localhost:7070}"
SETTLE="${SETTLE:-8}"

command -v jq   >/dev/null || { echo "jq is required";   exit 1; }
command -v curl >/dev/null || { echo "curl is required"; exit 1; }

# "<flink job name>:<uploaded jar name prefix>", downstream-first.
JOBS=(
    "normalizer-aggregator:job-aggregator"
    "normalizer-book-builder:job-book-builder"
    "normalizer-type-validator:job-type-validator"
)

cancel_job() {
    local name="$1"
    local ids
    ids=$(curl -s "${FLINK_API}/jobs/overview" \
        | jq -r --arg n "$name" '.jobs[] | select(.name == $n and (.state == "RUNNING" or .state == "RESTARTING")) | .jid')
    [ -n "$ids" ] || { echo "    $name: not running"; return 0; }
    for id in $ids; do
        curl -s -X PATCH "${FLINK_API}/jobs/${id}?mode=cancel" >/dev/null
        while true; do
            case "$(curl -s "${FLINK_API}/jobs/${id}" | jq -r '.state')" in
                CANCELED|FAILED|FINISHED) break ;;
            esac
            sleep 1
        done
        echo "    $name: cancelled ($id)"
    done
}

submit_job() {
    local name="$1" prefix="$2"
    local jar
    # newest upload whose filename starts with the module's artifactId
    jar=$(curl -s "${FLINK_API}/jars" \
        | jq -r --arg p "$prefix" '[.files[] | select(.name | startswith($p))] | sort_by(.uploaded) | last | .id // empty')
    if [ -z "$jar" ]; then
        echo "ERROR: no uploaded jar matching '${prefix}*' on the cluster."
        echo "       Submit it once first (e.g. ../run-job.sh job-type-validator), then re-run reset."
        exit 1
    fi
    local jid
    jid=$(curl -s -X POST "${FLINK_API}/jars/${jar}/run" | jq -r '.jobid // empty')
    [ -n "$jid" ] || { echo "ERROR: submit failed for $name"; exit 1; }
    while true; do
        case "$(curl -s "${FLINK_API}/jobs/${jid}" | jq -r '.state')" in
            RUNNING) break ;;
            FAILED|CANCELED) echo "ERROR: $name entered a terminal state on startup"; exit 1 ;;
        esac
        sleep 1
    done
    echo "    $name: RUNNING ($jid)"
}

echo "==> Cancelling stateful jobs (downstream-first)..."
for entry in "${JOBS[@]}"; do cancel_job "${entry%%:*}"; done

echo "==> Resubmitting (downstream-first)..."
for entry in "${JOBS[@]}"; do submit_job "${entry%%:*}" "${entry##*:}"; done

echo "==> Settling ${SETTLE}s so every source is assigned before data arrives..."
sleep "$SETTLE"
echo "State reset complete."
