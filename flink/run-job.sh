#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="$SCRIPT_DIR/orderbook-job/target/orderbook-job-1.0-SNAPSHOT.jar"
FLINK_API="http://localhost:7070"

# 1. Build
echo "==> Building..."
mvn -f "$SCRIPT_DIR/orderbook-job/pom.xml" package -q -DskipTests
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
docker logs --since "$SINCE" taskmanager 2>&1 | grep -E "OrderBookJob|ERROR|WARN" \
    || echo "    (no matching log lines — check 'docker logs taskmanager' for details)"
