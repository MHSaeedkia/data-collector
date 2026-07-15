#!/usr/bin/env bash
set -euo pipefail

# End-to-end smoke test for the type-validator job (raw pipeline job 2).
#
# SMOKE RULE for the normalizer pipeline (applies to every job's smoke, jobs 1..6):
#   Send RAW exchange payloads ONLY (to ex{id}-raw). Let the WHOLE Flink chain run
#   (job1 pair-extract -> job2 type-validate -> ...). Then capture the topic of the job
#   under test and assert its contract. Never produce a job's intermediate input directly —
#   that bypasses upstream stamping and hides real behaviour. And stamp event_time = the
#   wall-clock time of execution, so the per-stage pipeline_timings (in/out for each step)
#   can be verified as real, monotonically-increasing processing times.
#
# This smoke drives ex8 (OKX): its parser reads a single `ts` field that becomes BOTH
# event_time AND sequence_id (jump 300), so setting ts = now gives an execution-time
# event_time and full control of the sequence lifecycle. OKX BTC-USDT resolves to pair_id 1
# in the warmed DB, so the live key is (8, 1). Raw goes to ex8-raw; job 1 emits onto
# ex8-p1-raw-flink; job 2 routes valid -> ex8-p1-type-validated-raw-flink, rejects ->
# dead-letter ex8-p1-rejected-flink.
#
# Job 2 is STATEFUL (keyed lastSeq/awaitingSnapshot), so the test must survive a long-running
# job and repeat runs. seq = ts = now (epoch millis) is strictly increasing across runs, so
# every run's first snapshot is a fresh-or-newer baseline and the deliberately stale case
# (ts=1) is always rejected. Cases run in order on the same key.
#
# Prerequisites (this is a TEST, not a deploy):
#   - the normalizer stack is up (docker-compose-normalizer.yml)
#   - the DB is warmed (scripts/warmup.sh) so exchange_markets resolves OKX BTC-USDT -> p1
#   - BOTH jobs are submitted:  ./run-job.sh job-pair-extractor  AND  ./run-job.sh job-type-validator
#   - no competing live OKX feed is producing onto ex8-* (this writes real (8,1) state)
#
# Usage: ./smoke-type-validator.sh
# Env overrides: FLINK_API, KAFKA_CONTAINER, SR_CONTAINER, KAFKA_BOOTSTRAP, SR_URL,
#                CONSUME_TIMEOUT_MS

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

FLINK_API="${FLINK_API:-http://localhost:7070}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
SR_CONTAINER="${SR_CONTAINER:-schema-registry}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
SR_URL="${SR_URL:-http://schema-registry:8082}"
PAIR_EXTRACT_JOB="normalizer-pair-extractor"
TYPE_VALIDATE_JOB="normalizer-type-validator"
CONSUME_TIMEOUT_MS="${CONSUME_TIMEOUT_MS:-25000}"

# Real key: ex8 OKX, BTC-USDT -> pair_id 1 (warmed DB). We feed raw and let job 1 assign it.
EX=8
PAIR=1
JUMP=300                       # OKX's hardcoded per-message sequence cadence (see OkxParser)
RAW_TOPIC="ex${EX}-raw"                                 # job 1 input (verbatim exchange payload)
INTER_TOPIC="ex${EX}-p${PAIR}-raw-flink"               # job 1 -> job 2 (pre-create so job 2 sees it)
VALID_TOPIC="ex${EX}-p${PAIR}-type-validated-raw-flink"
REJECT_TOPIC="ex${EX}-p${PAIR}-rejected-flink"

# Decoded Avro union key for pipeline_timings is namespace-qualified; reject wraps under .event.
PT='.pipeline_timings["io.tibobit.orderbook.PipelineTimings"]'
EPT='.event.pipeline_timings["io.tibobit.orderbook.PipelineTimings"]'

command -v jq   >/dev/null || { echo "jq is required";   exit 1; }
command -v curl >/dev/null || { echo "curl is required"; exit 1; }

# --- precondition: BOTH jobs RUNNING (the chain needs job 1 to parse raw for job 2) ---
overview=$(curl -s --max-time 5 "$FLINK_API/jobs/overview" 2>/dev/null || true)
for job in "$PAIR_EXTRACT_JOB" "$TYPE_VALIDATE_JOB"; do
    running=$(jq -r --arg n "$job" '.jobs[]? | select(.state=="RUNNING" and .name==$n) | .name' \
        <<<"$overview" 2>/dev/null || true)
    if [[ -z "$running" ]]; then
        echo "ERROR: job '$job' is not RUNNING at $FLINK_API."
        echo "       Submit both:  $SCRIPT_DIR/run-job.sh job-pair-extractor"
        echo "                     $SCRIPT_DIR/run-job.sh job-type-validator"
        exit 1
    fi
done

for t in "$RAW_TOPIC" "$INTER_TOPIC" "$VALID_TOPIC" "$REJECT_TOPIC"; do
    docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" \
        --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true
done

pass=0
fail=0

# Build one verbatim OKX raw payload (books-grouped envelope). ts is a STRING epoch-millis and
# is the ONLY timestamp: OkxParser maps it to BOTH event_time and sequence_id. Set ts = now so
# event_time is the execution time and each downstream stage's timings must exceed it.
raw_okx() {
    local action="$1" ts="$2"
    jq -cn --arg action "$action" --arg ts "$ts" '
        {arg:{channel:"books-grouped",instId:"BTC-USDT",grouping:"1"},
         action:$action,
         data:[{asks:[["62770","2.2"]],bids:[["62769","0.5"]],ts:$ts}]}'
}

produce() {
    # Raw topic is verbatim bytes — plain JSON, no schema registry (unlike the Avro output topics).
    printf '%s\n' "$1" | docker exec -i "$KAFKA_CONTAINER" kafka-console-producer \
        --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$RAW_TOPIC" >/dev/null 2>&1
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

# Case: produce one raw OKX message, expect the resulting event on $out_topic; run assertions
# (jq filter -> expected). The output topic's end offset is captured BEFORE producing so we read
# exactly the event our produce triggers (deterministic, no consumer-group race).
run_case() {
    local label="$1" out_topic="$2" record="$3"; shift 3
    local start; start="$(end_offset "$out_topic")"; start="${start:-0}"
    if ! produce "$record"; then
        echo "FAIL  $label — produce to $RAW_TOPIC failed"; fail=$((fail+1)); return
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

echo "Smoke-testing the chain job1->job2 via raw ex${EX} (OKX), key ex${EX}/p${PAIR}..."
echo

# Cases run IN ORDER on the same keyed (8,1) state, walking one full delta-feed lifecycle.
# OKX jump = 300, so each contiguous ts is the previous + 300; the gap skips ahead.

# 1. snapshot baseline -> valid. The whole chain stamped it: assert event_time == the ts we sent
#    (execution time), and the per-stage timings are populated and monotonically increasing:
#    event_time <= pair_extract_in <= pair_extract_out <= type_validate_in <= type_validate_out.
run_case "snapshot(baseline) accepted" "$VALID_TOPIC" "$(raw_okx snapshot "$NOW")" \
    '.exchange_id' "$EX" '.pair_id' "$PAIR" '.type' "snapshot" '.sequence_id.long' "$NOW" \
    '.event_time' "$NOW" \
    "($PT.pair_extract_in.long >= .event_time)" "true" \
    "($PT.pair_extract_in.long <= $PT.pair_extract_out.long)" "true" \
    "($PT.pair_extract_out.long <= $PT.type_validate_in.long)" "true" \
    "($PT.type_validate_in.long <= $PT.type_validate_out.long)" "true"

# 2. first contiguous update (ts = baseline + jump) -> valid.
run_case "contiguous update #1 accepted" "$VALID_TOPIC" "$(raw_okx update "$((NOW+JUMP))")" \
    '.type' "update" '.sequence_id.long' "$((NOW+JUMP))" '.event_time' "$((NOW+JUMP))"

# 3. second contiguous update (snapshot->update->update chain) -> valid.
run_case "contiguous update #2 accepted" "$VALID_TOPIC" "$(raw_okx update "$((NOW+2*JUMP))")" \
    '.type' "update" '.sequence_id.long' "$((NOW+2*JUMP))"

# 4. gap: ts jumps to NOW+4*JUMP (expected NOW+3*JUMP) -> dead-letter sequence_gap; enters awaiting.
#    Upstream pair_extract_* must PRESERVE onto the reject (job 2 only adds type_validate_in here).
run_case "gap update rejected sequence_gap" "$REJECT_TOPIC" "$(raw_okx update "$((NOW+4*JUMP))")" \
    '.reject_reason' "sequence_gap" '.event.type' "update" '.event.sequence_id.long' "$((NOW+4*JUMP))" \
    '.event.event_time' "$((NOW+4*JUMP))" \
    "($EPT.pair_extract_in.long <= $EPT.pair_extract_out.long)" "true" \
    "($EPT.pair_extract_out.long <= $EPT.type_validate_in.long)" "true" \
    "($EPT.type_validate_out == null)" "true"

# 5. still awaiting after the gap: next update -> dead-letter awaiting_snapshot (held until re-sync).
run_case "held update rejected awaiting_snapshot" "$REJECT_TOPIC" "$(raw_okx update "$((NOW+5*JUMP))")" \
    '.reject_reason' "awaiting_snapshot" '.event.type' "update" '.event.sequence_id.long' "$((NOW+5*JUMP))"

# 6. re-sync: a newer snapshot -> valid, clears awaiting-snapshot and rebaselines lastSeq.
run_case "snapshot re-sync accepted" "$VALID_TOPIC" "$(raw_okx snapshot "$((NOW+10*JUMP))")" \
    '.type' "snapshot" '.sequence_id.long' "$((NOW+10*JUMP))" \
    "($PT.type_validate_out.long >= $PT.type_validate_in.long)" "true"

# 7. contiguous update after re-sync -> valid again (recovery confirmed).
run_case "contiguous update after re-sync accepted" "$VALID_TOPIC" "$(raw_okx update "$((NOW+11*JUMP))")" \
    '.type' "update" '.sequence_id.long' "$((NOW+11*JUMP))"

# 8. stale snapshot (ts far in the past) -> dead-letter with reason stale_or_duplicate.
run_case "stale snapshot rejected" "$REJECT_TOPIC" "$(raw_okx snapshot 1)" \
    '.reject_reason' "stale_or_duplicate" '.event.exchange_id' "$EX" '.event.type' "snapshot"

echo
echo "Result: $pass passed, $fail failed."
[[ "$fail" -eq 0 ]]
