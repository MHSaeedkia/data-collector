#!/usr/bin/env bash
set -euo pipefail

# End-to-end smoke test for the precision job (raw pipeline job 4).
#
# SMOKE RULE for the normalizer pipeline (applies to every job's smoke, jobs 1..6):
#   Send RAW exchange payloads ONLY (to ex{id}-raw). Let the WHOLE Flink chain run
#   (job1 pair-extract -> job2 type-validate -> job3 rebase -> job4 precision -> ...). Then
#   capture the topic of the job under test and assert its contract. Never produce a job's
#   intermediate input directly — that bypasses upstream stamping and hides real behaviour.
#   And stamp event_time = the wall-clock time of execution, so the per-stage pipeline_timings
#   (in/out for each step) can be verified as real, monotonically-increasing processing times.
#
# Drives ex8 (OKX) for the same reason as smoke-type-validator.sh: its parser reads a single
# `ts` field that becomes BOTH event_time AND sequence_id (jump 300), so ts = now gives an
# execution-time event_time and full control of the sequence lifecycle. OKX BTC-USDT resolves
# to pair_id 1, so the live key is (8, 1). Chain: ex8-raw -> ex8-p1-raw-flink ->
# ex8-p1-type-validated-raw-flink -> ex8-p1-rebased-flink -> ex8-p1-applied-precision-flink
# (rejects -> ex8-p1-rejected-flink).
#
# Unlike smoke-rebaser.sh this does NOT mutate the DB. A job whose behaviour depends on
# reference data has to make that data non-trivial, but the seeded markets row for p1 is
# already price_precision=2 / quantity_precision=8 — non-identity — so feeding values with MORE
# decimals than that exercises real truncation. (The seeded rebase row IS identity, 0/0, which
# is what we want here: it keeps job 3 a no-op so every digit change is job 4's doing.)
#
# Covered here, and worth stating because it is the milestone's resolved design flag: a nonzero
# quantity that truncates to exactly 0 is emitted as quantity "0" and its level is KEPT — job 5
# then reads that as a level-delete, which is the intent (size below the lot precision is not
# representable liquidity). Case 1 sends a dust ask alongside a real one and asserts both come
# out, the dust one carrying "0".
#
# Also covered: truncating prices makes distinct wire prices COLLIDE, and colliding levels are
# merged into one carrying the summed quantity (user decision 2026-07-20). Each case sends three
# asks, two of which collide, and asserts two come out — this job can now change a side's length.
#
# Prerequisites (this is a TEST, not a deploy):
#   - the normalizer stack is up (docker-compose.yml)
#   - the DB is warmed (scripts/warmup.sh) so exchange_markets resolves OKX BTC-USDT -> p1
#   - ALL FOUR jobs are submitted: ./run-job.sh job-pair-extractor
#                                  ./run-job.sh job-type-validator
#                                  ./run-job.sh job-rebaser
#                                  ./run-job.sh job-precision
#   - no competing live OKX feed is producing onto ex8-* (this writes real (8,1) state)
#
# Usage: ./smoke-precision.sh
# Env overrides: FLINK_API, KAFKA_CONTAINER, SR_CONTAINER, PG_CONTAINER, KAFKA_BOOTSTRAP,
#                SR_URL, CONSUME_TIMEOUT_MS

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

FLINK_API="${FLINK_API:-http://localhost:7070}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
SR_CONTAINER="${SR_CONTAINER:-schema-registry}"
PG_CONTAINER="${PG_CONTAINER:-postgres}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
SR_URL="${SR_URL:-http://schema-registry:8082}"
CONSUME_TIMEOUT_MS="${CONSUME_TIMEOUT_MS:-25000}"

PAIR_EXTRACT_JOB="normalizer-pair-extractor"
TYPE_VALIDATE_JOB="normalizer-type-validator"
REBASE_JOB="normalizer-rebaser"
PRECISION_JOB="normalizer-precision"

EX=8
PAIR=1
JUMP=300                       # OKX's hardcoded per-message sequence cadence (see OkxParser)
RAW_TOPIC="ex${EX}-raw"
INTER_TOPIC="ex${EX}-p${PAIR}-raw-flink"
VALID_TOPIC="ex${EX}-p${PAIR}-type-validated-raw-flink"
REBASED_TOPIC="ex${EX}-p${PAIR}-rebased-flink"
PRECISION_TOPIC="ex${EX}-p${PAIR}-applied-precision-flink"
REJECT_TOPIC="ex${EX}-p${PAIR}-rejected-flink"

# The seeded precisions this smoke asserts against. Checked against the live DB below — if the
# seed changes, the script fails loudly rather than silently asserting the wrong digits.
WANT_PRICE_PRECISION=2
WANT_QTY_PRECISION=8

# Raw inputs carry MORE decimals than the precisions allow, and the extra digits are all 9s so a
# rounding-up bug is unmissable (62770.98765 -> 62770.99 would be wrong; DOWN gives 62770.98).
RAW_ASK_PRICE="62770.98765";  RAW_ASK_QTY="0.123456789"
RAW_BID_PRICE="62769.43219";  RAW_BID_QTY="1.999999999"
WANT_BID_PRICE="62769.43";    WANT_BID_QTY="1.99999999"

# A second ask that truncates to the SAME price as the first (62770.98) — the collision case.
# The two must MERGE into one level carrying the SUM, never race and overwrite each other.
# The expected quantity is also chosen to prove the ORDER of the two lossy steps: summing the raw
# quantities and truncating once gives 0.234567900 -> "0.2345679", whereas truncating each first
# and then adding would give "0.23456789". Only the former is correct.
COLLIDE_ASK_PRICE="62770.98123"; COLLIDE_ASK_QTY="0.111111111"
WANT_ASK_PRICE="62770.98";       WANT_ASK_QTY="0.2345679"

# A second ask level whose quantity is below one unit at precision 8 — it must come out as "0"
# with its level KEPT. This is the milestone's truncate-to-zero decision, live.
DUST_ASK_PRICE="62771.5";     DUST_ASK_QTY="0.000000001"

# Decoded Avro union keys are namespace-qualified; nullable arrays decode under ".array".
PT='.pipeline_timings["io.tibobit.orderbook.PipelineTimings"]'
EPT='.event.pipeline_timings["io.tibobit.orderbook.PipelineTimings"]'

command -v jq   >/dev/null || { echo "jq is required";   exit 1; }
command -v curl >/dev/null || { echo "curl is required"; exit 1; }

# --- precondition: ALL FOUR jobs RUNNING (job 4's input only exists if 1..3 run) ---
overview=$(curl -s --max-time 5 "$FLINK_API/jobs/overview" 2>/dev/null || true)
for job in "$PAIR_EXTRACT_JOB" "$TYPE_VALIDATE_JOB" "$REBASE_JOB" "$PRECISION_JOB"; do
    running=$(jq -r --arg n "$job" '.jobs[]? | select(.state=="RUNNING" and .name==$n) | .name' \
        <<<"$overview" 2>/dev/null || true)
    if [[ -z "$running" ]]; then
        echo "ERROR: job '$job' is not RUNNING at $FLINK_API."
        echo "       Submit all four:   $SCRIPT_DIR/run-job.sh job-pair-extractor"
        echo "                          $SCRIPT_DIR/run-job.sh job-type-validator"
        echo "                          $SCRIPT_DIR/run-job.sh job-rebaser"
        echo "                          $SCRIPT_DIR/run-job.sh job-precision"
        exit 1
    fi
done

for t in "$RAW_TOPIC" "$INTER_TOPIC" "$VALID_TOPIC" "$REBASED_TOPIC" "$PRECISION_TOPIC" "$REJECT_TOPIC"; do
    docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" \
        --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true
done

psql_do() {
    docker exec "$PG_CONTAINER" psql -U postgres -d markets -tAc "$1" 2>/dev/null
}

# --- precondition: the reference data is what these assertions assume ---
PRECISIONS="$(psql_do "SELECT price_precision || ' ' || quantity_precision FROM markets WHERE id=$PAIR")"
if [[ "$PRECISIONS" != "$WANT_PRICE_PRECISION $WANT_QTY_PRECISION" ]]; then
    echo "ERROR: markets row for pair $PAIR has precisions '$PRECISIONS'," \
         "expected '$WANT_PRICE_PRECISION $WANT_QTY_PRECISION'."
    echo "       Re-warm the DB, or update the expected values at the top of this script."
    exit 1
fi
REBASE="$(psql_do "SELECT price_amount_rebase || ' ' || volume_amount_rebase FROM exchange_markets WHERE exchange_id=$EX AND market_id=$PAIR")"
if [[ "$REBASE" != "0 0" ]]; then
    echo "ERROR: exchange_markets rebase for ($EX, p$PAIR) is '$REBASE', expected '0 0'."
    echo "       Job 3 must be a no-op here or the expected digits below are wrong."
    echo "       (a smoke-rebaser.sh run may still be in flight — it restores on exit)"
    exit 1
fi

pass=0
fail=0

# One verbatim OKX raw payload (books-grouped envelope). ts is a STRING epoch-millis and is the
# ONLY timestamp: OkxParser maps it to BOTH event_time and sequence_id. ts = now, so event_time
# is the execution time and every downstream stage's timings must exceed it.
#
# THREE asks go in and TWO must come out: the real level and the colliding one merge into
# asks[0], the dust level stays asks[1]. That 3->2 shrink is itself the assertion that the merge
# happened — before the merge existed this job could not change a side's length at all.
raw_okx() {
    local action="$1" ts="$2"
    jq -cn --arg action "$action" --arg ts "$ts" \
        --arg ap "$RAW_ASK_PRICE" --arg aq "$RAW_ASK_QTY" \
        --arg cp "$COLLIDE_ASK_PRICE" --arg cq "$COLLIDE_ASK_QTY" \
        --arg dp "$DUST_ASK_PRICE" --arg dq "$DUST_ASK_QTY" \
        --arg bp "$RAW_BID_PRICE" --arg bq "$RAW_BID_QTY" '
        {arg:{channel:"books-grouped",instId:"BTC-USDT",grouping:"1"},
         action:$action,
         data:[{asks:[[$ap,$aq],[$cp,$cq],[$dp,$dq]],bids:[[$bp,$bq]],ts:$ts}]}'
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

echo "Smoke-testing the chain job1->job2->job3->job4 via raw ex${EX} (OKX), key ex${EX}/p${PAIR}..."
echo

# Cases run IN ORDER on the same keyed (8,1) state in job 2 (job 4 itself is stateless).

# 1. snapshot baseline -> truncated. Asserts DOWN-truncation on both sides, that the two asks
#    colliding on 62770.98 MERGE into one level carrying their summed quantity (3 asks in, 2 out),
#    that the dust level is KEPT and carries quantity "0", that event_time is untouched,
#    and that the timing chain now extends through job 4:
#    event_time <= pair_extract_in <= ... <= rebase_out <= precision_in <= precision_out.
run_case "snapshot(baseline) truncated + collision merged + dust zeroed" "$PRECISION_TOPIC" "$(raw_okx snapshot "$NOW")" \
    '.exchange_id' "$EX" '.pair_id' "$PAIR" '.type' "snapshot" \
    '.sequence_id.long' "$NOW" '.event_time' "$NOW" \
    '(.asks.array | length)' "2" \
    '.asks.array[0].price' "$WANT_ASK_PRICE" '.asks.array[0].quantity' "$WANT_ASK_QTY" \
    '.asks.array[1].price' "$DUST_ASK_PRICE" '.asks.array[1].quantity' "0" \
    '(.bids.array | length)' "1" \
    '.bids.array[0].price' "$WANT_BID_PRICE" '.bids.array[0].quantity' "$WANT_BID_QTY" \
    "($PT.pair_extract_in.long >= .event_time)" "true" \
    "($PT.pair_extract_in.long <= $PT.pair_extract_out.long)" "true" \
    "($PT.pair_extract_out.long <= $PT.type_validate_in.long)" "true" \
    "($PT.type_validate_in.long <= $PT.type_validate_out.long)" "true" \
    "($PT.type_validate_out.long <= $PT.rebase_in.long)" "true" \
    "($PT.rebase_in.long <= $PT.rebase_out.long)" "true" \
    "($PT.rebase_out.long <= $PT.precision_in.long)" "true" \
    "($PT.precision_in.long <= $PT.precision_out.long)" "true"

# 2. contiguous update (ts = baseline + jump) -> truncated the same way (job 4 is type-agnostic:
#    it never inspects snapshot-vs-update, so dust becomes "0" on updates too — which is where
#    job 5 acts on it as a level-delete — and colliding prices merge by SUM on updates too, the
#    deliberate approximation recorded in PrecisionFunction's javadoc).
run_case "contiguous update truncated + collision merged + dust zeroed" "$PRECISION_TOPIC" "$(raw_okx update "$((NOW+JUMP))")" \
    '.type' "update" '.sequence_id.long' "$((NOW+JUMP))" '.event_time' "$((NOW+JUMP))" \
    '(.asks.array | length)' "2" \
    '.asks.array[0].price' "$WANT_ASK_PRICE" '.asks.array[0].quantity' "$WANT_ASK_QTY" \
    '.asks.array[1].quantity' "0" \
    "($PT.precision_out.long >= $PT.precision_in.long)" "true"

# 3. an event job 2 rejects must never reach job 4: the gap lands on the dead-letter with
#    precision_in still null, proving the precision job never saw it (rather than asserting an
#    absence). Also pins that job 4 adds NO dead-letter of its own — an unconfigured precision is
#    a passthrough, so the only rejects on this topic still come from jobs 2 and 3.
run_case "gap update dead-lettered before precision" "$REJECT_TOPIC" "$(raw_okx update "$((NOW+4*JUMP))")" \
    '.reject_reason' "sequence_gap" '.event.type' "update" \
    '.event.sequence_id.long' "$((NOW+4*JUMP))" \
    "($EPT.type_validate_out == null)" "true" \
    "($EPT.rebase_in == null)" "true" \
    "($EPT.precision_in == null)" "true" \
    "($EPT.precision_out == null)" "true"

echo
echo "Result: $pass passed, $fail failed."
[[ "$fail" -eq 0 ]]
