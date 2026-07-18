#!/usr/bin/env bash
set -euo pipefail

# End-to-end smoke test for the rebaser job (raw pipeline job 3).
#
# SMOKE RULE for the normalizer pipeline (applies to every job's smoke, jobs 1..6):
#   Send RAW exchange payloads ONLY (to ex{id}-raw). Let the WHOLE Flink chain run
#   (job1 pair-extract -> job2 type-validate -> job3 rebase -> ...). Then capture the topic
#   of the job under test and assert its contract. Never produce a job's intermediate input
#   directly — that bypasses upstream stamping and hides real behaviour. And stamp
#   event_time = the wall-clock time of execution, so the per-stage pipeline_timings (in/out
#   for each step) can be verified as real, monotonically-increasing processing times.
#
# Drives ex8 (OKX) for the same reason as smoke-type-validator.sh: its parser reads a single
# `ts` field that becomes BOTH event_time AND sequence_id (jump 300), so ts = now gives an
# execution-time event_time and full control of the sequence lifecycle. OKX BTC-USDT resolves
# to pair_id 1, so the live key is (8, 1). Chain: ex8-raw -> ex8-p1-raw-flink ->
# ex8-p1-type-validated-raw-flink -> ex8-p1-rebased-flink (rejects -> ex8-p1-rejected-flink).
#
# Job 3 needs a NONZERO rebase row to prove anything, and the seeded row is 0/0 (identity).
# So this script temporarily sets exchange_markets.{price,volume}_amount_rebase for (8, p1),
# waits out the RefreshingLookup interval so the running job reloads, and RESTORES the original
# values on exit. That wait is why this smoke is slower than the others.
#
# NOT covered here (deliberately): the `no_rebase_row` dead-letter path. It cannot be reached
# from raw — job 1 resolves pair_id from the SAME exchange_markets row, so deleting it makes
# job 1 drop the event (dropped-no-parser/unknown market) and nothing ever reaches job 3.
# That branch is covered by RebaseFunctionTest instead.
#
# Prerequisites (this is a TEST, not a deploy):
#   - the normalizer stack is up (docker-compose-normalizer.yml)
#   - the DB is warmed (scripts/warmup.sh) so exchange_markets resolves OKX BTC-USDT -> p1
#   - ALL THREE jobs are submitted: ./run-job.sh job-pair-extractor
#                                   ./run-job.sh job-type-validator
#                                   ./run-job.sh job-rebaser
#   - no competing live OKX feed is producing onto ex8-* (this writes real (8,1) state)
#
# Usage: ./smoke-rebaser.sh
# Env overrides: FLINK_API, KAFKA_CONTAINER, SR_CONTAINER, PG_CONTAINER, KAFKA_BOOTSTRAP,
#                SR_URL, CONSUME_TIMEOUT_MS, REFRESH_WAIT_S

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

FLINK_API="${FLINK_API:-http://localhost:7070}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
SR_CONTAINER="${SR_CONTAINER:-schema-registry}"
PG_CONTAINER="${PG_CONTAINER:-postgres}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
SR_URL="${SR_URL:-http://schema-registry:8082}"
CONSUME_TIMEOUT_MS="${CONSUME_TIMEOUT_MS:-25000}"
# Must exceed the job's REFRESH_INTERVAL_MS (default 60s) so the running lookup reloads.
REFRESH_WAIT_S="${REFRESH_WAIT_S:-65}"

PAIR_EXTRACT_JOB="normalizer-pair-extractor"
TYPE_VALIDATE_JOB="normalizer-type-validator"
REBASE_JOB="normalizer-rebaser"

EX=8
PAIR=1
JUMP=300                       # OKX's hardcoded per-message sequence cadence (see OkxParser)
RAW_TOPIC="ex${EX}-raw"
INTER_TOPIC="ex${EX}-p${PAIR}-raw-flink"
VALID_TOPIC="ex${EX}-p${PAIR}-type-validated-raw-flink"
REBASED_TOPIC="ex${EX}-p${PAIR}-rebased-flink"
REJECT_TOPIC="ex${EX}-p${PAIR}-rejected-flink"

# The exponents under test: price shifts RIGHT (+2), quantity shifts LEFT (-3) — one case
# exercises both directions, and neither is expressible exactly in binary floating point.
PRICE_REBASE=2
VOLUME_REBASE=-3
RAW_ASK_PRICE="62770";  RAW_ASK_QTY="2.2"
RAW_BID_PRICE="62769";  RAW_BID_QTY="0.5"
WANT_ASK_PRICE="6277000"; WANT_ASK_QTY="0.0022"
WANT_BID_PRICE="6276900"; WANT_BID_QTY="0.0005"

# Decoded Avro union keys are namespace-qualified; nullable arrays decode under ".array".
PT='.pipeline_timings["io.tibobit.orderbook.PipelineTimings"]'
EPT='.event.pipeline_timings["io.tibobit.orderbook.PipelineTimings"]'

command -v jq   >/dev/null || { echo "jq is required";   exit 1; }
command -v curl >/dev/null || { echo "curl is required"; exit 1; }

# --- precondition: ALL THREE jobs RUNNING (job 3's input only exists if 1 and 2 run) ---
overview=$(curl -s --max-time 5 "$FLINK_API/jobs/overview" 2>/dev/null || true)
for job in "$PAIR_EXTRACT_JOB" "$TYPE_VALIDATE_JOB" "$REBASE_JOB"; do
    running=$(jq -r --arg n "$job" '.jobs[]? | select(.state=="RUNNING" and .name==$n) | .name' \
        <<<"$overview" 2>/dev/null || true)
    if [[ -z "$running" ]]; then
        echo "ERROR: job '$job' is not RUNNING at $FLINK_API."
        echo "       Submit all three:  $SCRIPT_DIR/run-job.sh job-pair-extractor"
        echo "                          $SCRIPT_DIR/run-job.sh job-type-validator"
        echo "                          $SCRIPT_DIR/run-job.sh job-rebaser"
        exit 1
    fi
done

for t in "$RAW_TOPIC" "$INTER_TOPIC" "$VALID_TOPIC" "$REBASED_TOPIC" "$REJECT_TOPIC"; do
    docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" \
        --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true
done

psql_do() {
    docker exec "$PG_CONTAINER" psql -U postgres -d markets -tAc "$1" 2>/dev/null
}

# --- set the rebase exponents, remembering the originals so we can put them back ---
ORIGINAL="$(psql_do "SELECT price_amount_rebase || ' ' || volume_amount_rebase FROM exchange_markets WHERE exchange_id=$EX AND market_id=$PAIR")"
if [[ -z "$ORIGINAL" ]]; then
    echo "ERROR: no exchange_markets row for exchange_id=$EX market_id=$PAIR — is the DB warmed?"
    exit 1
fi
ORIG_PRICE="${ORIGINAL%% *}"
ORIG_VOLUME="${ORIGINAL##* }"

restore_rebase() {
    psql_do "UPDATE exchange_markets SET price_amount_rebase=$ORIG_PRICE, volume_amount_rebase=$ORIG_VOLUME WHERE exchange_id=$EX AND market_id=$PAIR" >/dev/null
    echo "Restored exchange_markets rebase for ($EX, p$PAIR) to $ORIG_PRICE / $ORIG_VOLUME."
    echo "(the running job keeps the test values until its next refresh, up to ${REFRESH_WAIT_S}s)"
}
trap restore_rebase EXIT

psql_do "UPDATE exchange_markets SET price_amount_rebase=$PRICE_REBASE, volume_amount_rebase=$VOLUME_REBASE WHERE exchange_id=$EX AND market_id=$PAIR" >/dev/null
echo "Set rebase for ($EX, p$PAIR) to price=$PRICE_REBASE volume=$VOLUME_REBASE (was $ORIG_PRICE / $ORIG_VOLUME)."
echo "Waiting ${REFRESH_WAIT_S}s for the running job's RefreshingLookup to reload..."
sleep "$REFRESH_WAIT_S"
echo

pass=0
fail=0

# One verbatim OKX raw payload (books-grouped envelope). ts is a STRING epoch-millis and is the
# ONLY timestamp: OkxParser maps it to BOTH event_time and sequence_id. ts = now, so event_time
# is the execution time and every downstream stage's timings must exceed it.
raw_okx() {
    local action="$1" ts="$2"
    jq -cn --arg action "$action" --arg ts "$ts" \
        --arg ap "$RAW_ASK_PRICE" --arg aq "$RAW_ASK_QTY" \
        --arg bp "$RAW_BID_PRICE" --arg bq "$RAW_BID_QTY" '
        {arg:{channel:"books-grouped",instId:"BTC-USDT",grouping:"1"},
         action:$action,
         data:[{asks:[[$ap,$aq]],bids:[[$bp,$bq]],ts:$ts}]}'
}

produce() {
    # Raw topic is verbatim bytes — plain JSON, no schema registry (unlike the Avro outputs).
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

NOW="$(( $(date +%s) * 1000 ))"   # epoch millis (portable) — newer than any prior run's baseline

echo "Smoke-testing the chain job1->job2->job3 via raw ex${EX} (OKX), key ex${EX}/p${PAIR}..."
echo

# Cases run IN ORDER on the same keyed (8,1) state in job 2 (job 3 itself is stateless).

# 1. snapshot baseline -> rebased. Asserts the rebase arithmetic in BOTH directions on both
#    sides, that event_time is still the ts we sent (job 3 must not touch it), and that the
#    timing chain now extends through job 3:
#    event_time <= pair_extract_in <= pair_extract_out <= type_validate_in <= type_validate_out
#                <= rebase_in <= rebase_out.
run_case "snapshot(baseline) rebased" "$REBASED_TOPIC" "$(raw_okx snapshot "$NOW")" \
    '.exchange_id' "$EX" '.pair_id' "$PAIR" '.type' "snapshot" \
    '.sequence_id.long' "$NOW" '.event_time' "$NOW" \
    '.asks.array[0].price' "$WANT_ASK_PRICE" '.asks.array[0].quantity' "$WANT_ASK_QTY" \
    '.bids.array[0].price' "$WANT_BID_PRICE" '.bids.array[0].quantity' "$WANT_BID_QTY" \
    "($PT.pair_extract_in.long >= .event_time)" "true" \
    "($PT.pair_extract_in.long <= $PT.pair_extract_out.long)" "true" \
    "($PT.pair_extract_out.long <= $PT.type_validate_in.long)" "true" \
    "($PT.type_validate_in.long <= $PT.type_validate_out.long)" "true" \
    "($PT.type_validate_out.long <= $PT.rebase_in.long)" "true" \
    "($PT.rebase_in.long <= $PT.rebase_out.long)" "true"

# 2. contiguous update (ts = baseline + jump) -> rebased the same way (job 3 is type-agnostic).
run_case "contiguous update rebased" "$REBASED_TOPIC" "$(raw_okx update "$((NOW+JUMP))")" \
    '.type' "update" '.sequence_id.long' "$((NOW+JUMP))" '.event_time' "$((NOW+JUMP))" \
    '.asks.array[0].price' "$WANT_ASK_PRICE" '.asks.array[0].quantity' "$WANT_ASK_QTY" \
    "($PT.rebase_out.long >= $PT.rebase_in.long)" "true"

# 3. an event job 2 rejects must never reach job 3: the gap lands on the dead-letter with
#    rebase_in still null, proving the rebaser never saw it (rather than asserting an absence).
run_case "gap update dead-lettered before rebase" "$REJECT_TOPIC" "$(raw_okx update "$((NOW+4*JUMP))")" \
    '.reject_reason' "sequence_gap" '.event.type' "update" \
    '.event.sequence_id.long' "$((NOW+4*JUMP))" \
    "($EPT.type_validate_out == null)" "true" \
    "($EPT.rebase_in == null)" "true" \
    "($EPT.rebase_out == null)" "true"

echo
echo "Result: $pass passed, $fail failed."
[[ "$fail" -eq 0 ]]
