#!/usr/bin/env bash
set -euo pipefail

# Cancels every running job on the Flink cluster and waits until each reaches a
# terminal state, so their task slots are free before new jobs are submitted.

FLINK_API="${FLINK_API:-http://localhost:7070}"

JOB_IDS=$(curl -s "${FLINK_API}/jobs" | jq -r '.jobs[] | select(.status == "RUNNING" or .status == "RESTARTING") | .id')

if [[ -z "$JOB_IDS" ]]; then
    echo "==> No running jobs to cancel"
    exit 0
fi

for JOB_ID in $JOB_IDS; do
    echo "==> Cancelling ${JOB_ID}..."
    curl -s -X PATCH "${FLINK_API}/jobs/${JOB_ID}?mode=cancel" > /dev/null
done

for JOB_ID in $JOB_IDS; do
    while true; do
        STATUS=$(curl -s "${FLINK_API}/jobs/${JOB_ID}" | jq -r '.state')
        case "$STATUS" in
            CANCELED|FAILED|FINISHED)
                echo "    ${JOB_ID}: ${STATUS}"
                break
                ;;
        esac
        sleep 1
    done
done
