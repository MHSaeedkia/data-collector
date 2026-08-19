#!/usr/bin/env bash
set -euo pipefail

# Builds ONE Flink job — from any project under flink/ — and submits it to the cluster.
# Usage: ./run-job.sh <job>     e.g. ./run-job.sh job-aggregator
#                               e.g. ./run-job.sh merger
#
# Which jobs exist and where they live is job-discovery.sh's business. The jar's manifest
# Main-Class is the entry point, so there is no per-job class mapping here either.
#
# All projects deploy to the same cluster and the same image (flink/normalizer/Dockerfile).
# To run a job in-process instead, with no cluster at all, see run-local.sh.

source "$(dirname "${BASH_SOURCE[0]}")/job-discovery.sh"

FLINK_API="${FLINK_API:-http://localhost:7070}"

JOB="${1:-}"
read -r PROJECT MODULE <<< "$(resolve_job "$JOB")"

if [[ -z "${PROJECT:-}" ]]; then
    [[ -n "$JOB" ]] && echo "ERROR: unknown job '$JOB'" || echo "Usage: $0 <job>"
    echo "Available jobs:"
    list_jobs
    exit 1
fi

# 1. Build. Multi-module needs -am so common/ is built alongside; single-module builds whole project.
if [[ -n "${MODULE:-}" ]]; then
    echo "==> Building $MODULE ($(basename "$PROJECT"))..."
    mvn -f "$PROJECT/pom.xml" -pl "$MODULE" -am package -q -DskipTests
    TARGET="$PROJECT/$MODULE/target"
else
    echo "==> Building $JOB..."
    mvn -f "$PROJECT/pom.xml" package -q -DskipTests
    TARGET="$PROJECT/target"
fi

# The shaded job jar (artifactId may differ from the module dir; skip shade's original-* copy)
JAR=$(find "$TARGET" -maxdepth 1 -name "*-1.0-SNAPSHOT.jar" ! -name "original-*" | head -1)
if [[ -z "$JAR" ]]; then
    echo "ERROR: no jar found in $TARGET"
    exit 1
fi
echo "    Built: $JAR"

# 2. Upload JAR to Flink
echo "==> Uploading JAR..."
UPLOAD_RESP=$(curl -s -X POST -H "Expect:" -F "jarfile=@${JAR}" "${FLINK_API}/jars/upload")
JAR_ID=$(echo "$UPLOAD_RESP" | jq -r '.filename | split("/") | last')
if [[ -z "$JAR_ID" || "$JAR_ID" == "null" ]]; then
    echo "ERROR: JAR upload failed: $UPLOAD_RESP"
    exit 1
fi
echo "    Uploaded: ${JAR_ID}"

# 3. Submit job
echo "==> Submitting job..."
SINCE=$(date +%s)
SUBMIT_RESP=$(curl -s -X POST "${FLINK_API}/jars/${JAR_ID}/run")
JOB_ID=$(echo "$SUBMIT_RESP" | jq -r '.jobid')
if [[ -z "$JOB_ID" || "$JOB_ID" == "null" ]]; then
    echo "ERROR: Job submission failed:"
    echo "$SUBMIT_RESP" | jq . 2>/dev/null || echo "$SUBMIT_RESP"
    exit 1
fi
echo "    Job ID: ${JOB_ID}"

# 4. Wait until RUNNING or terminal state (streaming jobs never reach FINISHED)
echo "==> Waiting for job to start..."
while true; do
    STATUS=$(curl -s "${FLINK_API}/jobs/${JOB_ID}" | jq -r '.state')
    case "$STATUS" in
        RUNNING)
            echo "    Status: RUNNING"
            break
            ;;
        FAILED|CANCELED|RESTARTING)
            echo "ERROR: Job entered state: $STATUS"
            echo ""
            echo "==> Root cause:"
            curl -s "${FLINK_API}/jobs/${JOB_ID}/exceptions" \
                | jq -r '."root-exception" // .exceptionHistory.entries[0].stacktrace // "No exception detail available"'
            echo ""
            echo "==> TaskManager logs (last 50 lines):"
            docker logs --since "$SINCE" taskmanager 2>&1 | tail -50
            exit 1
            ;;
    esac
    sleep 1
done

# 5. Show relevant logs from this run
echo ""
echo "==> TaskManager logs:"
docker logs --since "$SINCE" taskmanager 2>&1 | grep -E "io.tibobit|ERROR|WARN" \
    || echo "    (no matching log lines — check 'docker logs taskmanager' for details)"
