#!/usr/bin/env bash
set -euo pipefail

# End-to-end smoke test for the type-validator job (raw pipeline job 2).
#
# Produces Confluent-Avro RawOrderBookEvents (subject raw-order-book-event) directly to the
# input topic and reads back where the running job routes them: valid events onto
# ex{id}-p{id}-type-validated-raw-flink, rejects onto the dead-letter ex{id}-p{id}-rejected-flink
# (with a reject_reason). This tests the LIVE wiring (input decode -> keyed validate -> both
# Avro sinks); the per-branch rule logic is covered by TypeValidateFunctionTest.
#
# Job 2 is STATEFUL (keyed lastSeq/awaitingSnapshot), so the test must survive a long-running
# job and repeat runs. It uses a synthetic key (ex99/p99 — collides with no real feed) and a
# strictly-increasing seq = now, so every run's first snapshot is accepted and the deliberately
# stale case (seq=1) is always rejected. Cases run in order on the same key.
#
# Prerequisites (this is a TEST, not a deploy):
#   - the normalizer stack is up (docker-compose-normalizer.yml)
#   - the job is already submitted:  ./run-job.sh job-type-validator
#
# Usage: ./smoke-type-validator.sh
# Env overrides: FLINK_API, KAFKA_CONTAINER, SR_CONTAINER, KAFKA_BOOTSTRAP, SR_URL,
#                CONSUME_TIMEOUT_MS

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEMA_FILE="$(cd "$SCRIPT_DIR/../.." && pwd)/schemas/raw_order_book_event.avsc"

FLINK_API="${FLINK_API:-http://localhost:7070}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
SR_CONTAINER="${SR_CONTAINER:-schema-registry}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
SR_URL="${SR_URL:-http://schema-registry:8082}"
JOB_NAME="normalizer-type-validator"
CONSUME_TIMEOUT_MS="${CONSUME_TIMEOUT_MS:-25000}"

# Synthetic key — no real exchange 99 / pair 99, so we never clash with live pipeline state.
EX=99
PAIR=99
IN_TOPIC="ex${EX}-p${PAIR}-raw-flink"
VALID_TOPIC="ex${EX}-p${PAIR}-type-validated-raw-flink"
REJECT_TOPIC="ex${EX}-p${PAIR}-rejected-flink"

command -v jq   >/dev/null || { echo "jq is required";   exit 1; }
command -v curl >/dev/null || { echo "curl is required"; exit 1; }
[[ -f "$SCHEMA_FILE" ]] || { echo "schema not found: $SCHEMA_FILE"; exit 1; }
SCHEMA="$(jq -c . "$SCHEMA_FILE")"

# --- precondition: the job is RUNNING ---
running=$(curl -s --max-time 5 "$FLINK_API/jobs/overview" \
    | jq -r --arg n "$JOB_NAME" '.jobs[]? | select(.state=="RUNNING" and .name==$n) | .name' 2>/dev/null || true)
if [[ -z "$running" ]]; then
    echo "ERROR: job '$JOB_NAME' is not RUNNING at $FLINK_API."
    echo "       Submit it first:  $SCRIPT_DIR/run-job.sh job-type-validator"
    exit 1
fi

for t in "$IN_TOPIC" "$VALID_TOPIC" "$REJECT_TOPIC"; do
    docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" \
        --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true
done

pass=0
fail=0

# Build one RawOrderBookEvent Avro-JSON record (union fields wrapped: sequence_id->{long},
# asks/bids->{array}). event_time = seq keeps things simple; timings left null (job 2 stamps its own).
raw_record() {
    local type="$1" seq="$2" jump="$3"
    jq -cn --argjson ex "$EX" --argjson pair "$PAIR" --arg type "$type" \
        --argjson seq "$seq" --argjson jump "$jump" '
        {exchange_id:$ex, pair_id:$pair, type:$type,
         sequence_id:{long:$seq}, sequence_jump:$jump, event_time:$seq,
         asks:{array:[{price:"100",quantity:"1"}]},
         bids:{array:[{price:"99",quantity:"1"}]},
         pipeline_timings:null}'
}

produce() {
    local record="$1"
    # avro.use.logical.type.converters=true: the JSON reader turns event_time (timestamp-millis)
    # into an Instant, so the serializer must convert it back — without this it ClassCastExceptions.
    printf '%s\n' "$record" | docker exec -i "$SR_CONTAINER" kafka-avro-console-producer \
        --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$IN_TOPIC" \
        --property schema.registry.url="$SR_URL" \
        --property avro.use.logical.type.converters=true \
        --property "value.schema=$SCHEMA" >/dev/null 2>&1
}

end_offset() {
    docker exec "$KAFKA_CONTAINER" kafka-get-offsets --bootstrap-server "$KAFKA_BOOTSTRAP" \
        --topic "$1" --time latest 2>/dev/null | cut -d: -f3
}

read_one() {
    local topic="$1" start="$2"
    docker exec "$SR_CONTAINER" kafka-avro-console-consumer \
        --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$topic" \
        --property schema.registry.url="$SR_URL" \
        --partition 0 --offset "$start" --max-messages 1 \
        --timeout-ms "$CONSUME_TIMEOUT_MS" 2>/dev/null | grep -m1 '^{' || true
}

# Case: produce one record, expect it on $out_topic; run assertions (jq filter -> expected).
run_case() {
    local label="$1" out_topic="$2" record="$3"; shift 3
    local start; start="$(end_offset "$out_topic")"; start="${start:-0}"
    if ! produce "$record"; then
        echo "FAIL  $label — produce to $IN_TOPIC failed"; fail=$((fail+1)); return
    fi
    local rec; rec="$(read_one "$out_topic" "$start")"
    if [[ -z "$rec" ]]; then
        echo "FAIL  $label — no event on $out_topic within ${CONSUME_TIMEOUT_MS}ms"
        fail=$((fail+1)); return
    fi
    local errs=()
    while (( "$#" )); do
        local filter="$1" want="$2"; shift 2
        local got; got="$(jq -r "$filter" <<<"$rec")"
        [[ "$got" == "$want" ]] || errs+=("$filter=$got want $want")
    done
    if [[ ${#errs[@]} -eq 0 ]]; then
        echo "PASS  $label -> $out_topic"
        pass=$((pass+1))
    else
        echo "FAIL  $label -> $out_topic"
        printf '        - %s\n' "${errs[@]}"
        fail=$((fail+1))
    fi
}

NOW="$(( $(date +%s) * 1000 ))"   # epoch millis (portable) — greater than any prior run's baseline

echo "Smoke-testing '$JOB_NAME' (synthetic key ex${EX}/p${PAIR})..."
echo

# These cases run IN ORDER on the same keyed state (ex99/p99), walking one full delta-feed
# lifecycle: baseline -> contiguous updates -> a sequence gap -> updates held while awaiting a
# re-sync -> snapshot re-sync -> contiguous again -> a stale snapshot. jump is 1 throughout, so
# each contiguous seq is the previous + 1; the gap skips ahead so seq != last + jump.

# 1. delta-feed snapshot (jump 1) = fresh baseline -> valid; job 2 stamps its own type_validate_out
#    (Avro union keys are namespace-qualified; upstream pair_extract_* stay null — produced directly).
run_case "snapshot(baseline) accepted" "$VALID_TOPIC" "$(raw_record snapshot "$NOW" 1)" \
    '.exchange_id' "$EX" '.pair_id' "$PAIR" '.type' "snapshot" '.sequence_id.long' "$NOW" \
    '(.pipeline_timings["io.tibobit.orderbook.PipelineTimings"].type_validate_out != null)' "true"

# 2. first contiguous update (seq = baseline + jump) -> valid.
run_case "contiguous update #1 accepted" "$VALID_TOPIC" "$(raw_record update "$((NOW+1))" 1)" \
    '.exchange_id' "$EX" '.type' "update" '.sequence_id.long' "$((NOW+1))"

# 3. second contiguous update (snapshot->update->update chain) -> valid.
run_case "contiguous update #2 accepted" "$VALID_TOPIC" "$(raw_record update "$((NOW+2))" 1)" \
    '.exchange_id' "$EX" '.type' "update" '.sequence_id.long' "$((NOW+2))"

# 4. gap: seq jumps to NOW+5 (expected NOW+3) -> dead-letter sequence_gap; enters awaiting-snapshot.
run_case "gap update rejected sequence_gap" "$REJECT_TOPIC" "$(raw_record update "$((NOW+5))" 1)" \
    '.reject_reason' "sequence_gap" '.event.type' "update" '.event.sequence_id.long' "$((NOW+5))"

# 5. still awaiting after the gap: next update -> dead-letter awaiting_snapshot (held until re-sync).
run_case "held update rejected awaiting_snapshot" "$REJECT_TOPIC" "$(raw_record update "$((NOW+6))" 1)" \
    '.reject_reason' "awaiting_snapshot" '.event.type' "update" '.event.sequence_id.long' "$((NOW+6))"

# 6. re-sync: a newer snapshot -> valid, clears awaiting-snapshot and rebaselines lastSeq.
run_case "snapshot re-sync accepted" "$VALID_TOPIC" "$(raw_record snapshot "$((NOW+10))" 1)" \
    '.type' "snapshot" '.sequence_id.long' "$((NOW+10))"

# 7. contiguous update after re-sync -> valid again (recovery confirmed).
run_case "contiguous update after re-sync accepted" "$VALID_TOPIC" "$(raw_record update "$((NOW+11))" 1)" \
    '.type' "update" '.sequence_id.long' "$((NOW+11))"

# 8. stale snapshot (seq far in the past) -> dead-letter with reason stale_or_duplicate.
run_case "stale snapshot rejected" "$REJECT_TOPIC" "$(raw_record snapshot 1 1)" \
    '.reject_reason' "stale_or_duplicate" '.event.exchange_id' "$EX" '.event.type' "snapshot"

echo
echo "Result: $pass passed, $fail failed."
[[ "$fail" -eq 0 ]]
