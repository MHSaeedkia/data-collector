#!/usr/bin/env bash
set -euo pipefail

# End-to-end smoke test for the book builder job (raw pipeline job 5).
#
# SMOKE RULE for the normalizer pipeline (applies to every job's smoke, jobs 1..6):
#   Send RAW exchange payloads ONLY (to ex{id}-raw). Let the WHOLE Flink chain run
#   (job1 pair-extract -> job2 type-validate -> job3 rebase -> job4 precision -> job5 build).
#   Then capture the topic of the job under test and assert its contract. Never produce a job's
#   intermediate input directly — that bypasses upstream stamping and hides real behaviour.
#   And stamp event_time = the wall-clock time of execution, so the per-stage pipeline_timings
#   (in/out for each step) can be verified as real, monotonically-increasing processing times.
#
# Drives ex8 (OKX): its parser reads a single `ts` field that becomes BOTH event_time AND
# sequence_id (jump 300), so ts = now gives an execution-time event_time and full control of the
# sequence lifecycle. OKX BTC-USDT resolves to pair_id 1, so the live key is (8, 1).
#
# The three cases run IN ORDER on one accumulating book — that is the point, since job 5 is the
# first stateful job whose state IS the deliverable. They cover, in order: a snapshot seeding the
# book, an update MERGING into it (not replacing it — the bid from case 1 must survive), and a
# quantity-0 update DELETING a level.
#
# Case 1 also proves the truncate-to-zero decision end to end: the raw payload carries a dust ask
# (below one unit at quantity_precision 8) which job 4 emits as quantity "0" and job 5 must then
# NOT store — so the emitted book has ONE ask, not two. That delete originates from precision
# truncation; no exchange ever sent it.
#
# NOT covered here: the ex3 (wallex) per-side snapshot merge, where a null side must leave the
# other side's state intact. OKX always sends both sides, so a null side is unreachable from this
# feed — it is covered by the module test `nullSideKeepsOtherSideState`.
#
# Prerequisites (this is a TEST, not a deploy):
#   - the normalizer stack is up (docker-compose.yml)
#   - the DB is warmed (scripts/warmup.sh) so exchange_markets resolves OKX BTC-USDT -> p1
#   - ALL FIVE jobs are submitted (see the precondition check below)
#   - no competing live OKX feed is producing onto ex8-* (this writes real (8,1) state)
#
# Usage: ./smoke-book-builder.sh
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

JOBS=(normalizer-pair-extractor normalizer-type-validator normalizer-rebaser
      normalizer-precision normalizer-book-builder)
MODULES=(job-pair-extractor job-type-validator job-rebaser job-precision job-book-builder)

EX=8
PAIR=1
JUMP=300                       # OKX's hardcoded per-message sequence cadence (see OkxParser)
RAW_TOPIC="ex${EX}-raw"
BOOK_TOPIC="ex${EX}-p${PAIR}-orderbook-snapshot-flink"
CHAIN_TOPICS=("ex${EX}-p${PAIR}-raw-flink" "ex${EX}-p${PAIR}-type-validated-raw-flink"
              "ex${EX}-p${PAIR}-rebased-flink" "ex${EX}-p${PAIR}-applied-precision-flink"
              "ex${EX}-p${PAIR}-rejected-flink")

WANT_PRICE_PRECISION=2
WANT_QTY_PRECISION=8

# Raw inputs carry more decimals than the precisions allow; job 4 truncates DOWN, and these are
# the digits job 5 must key the book by.
RAW_ASK_PRICE="62770.98765";  RAW_ASK_QTY="0.123456789"
RAW_BID_PRICE="62769.43219";  RAW_BID_QTY="1.999999999"
WANT_ASK_PRICE="62770.98";    WANT_ASK_QTY="0.12345678"
WANT_BID_PRICE="62769.43";    WANT_BID_QTY="1.99999999"

# A dust ask: nonzero on the wire, below one unit at quantity_precision 8. Job 4 turns it into
# "0"; job 5 must treat that as "no level here" and keep it out of the book.
DUST_ASK_PRICE="62771.5";     DUST_ASK_QTY="0.000000001"

# A second ask added by the update in case 2, then deleted by case 3.
RAW_ASK2_PRICE="62772.98765"; RAW_ASK2_QTY="0.5"
WANT_ASK2_PRICE="62772.98"

# order-book-snapshot has NON-nullable asks/bids (a built book always has both), so they decode
# as bare arrays — unlike raw-order-book-event's nullable ".array" unions. last_sequence_id is
# still a nullable union.
PT='.pipeline_timings["io.tibobit.orderbook.PipelineTimings"]'

command -v jq   >/dev/null || { echo "jq is required";   exit 1; }
command -v curl >/dev/null || { echo "curl is required"; exit 1; }

# --- precondition: ALL FIVE jobs RUNNING (job 5's input only exists if 1..4 run) ---
overview=$(curl -s --max-time 5 "$FLINK_API/jobs/overview" 2>/dev/null || true)
for i in "${!JOBS[@]}"; do
    running=$(jq -r --arg n "${JOBS[$i]}" '.jobs[]? | select(.state=="RUNNING" and .name==$n) | .name' \
        <<<"$overview" 2>/dev/null || true)
    if [[ -z "$running" ]]; then
        echo "ERROR: job '${JOBS[$i]}' is not RUNNING at $FLINK_API."
        echo "       Submit all five:"
        printf "         %s/run-job.sh %s\n" "$SCRIPT_DIR" "${MODULES[@]}"
        exit 1
    fi
done

for t in "$RAW_TOPIC" "$BOOK_TOPIC" "${CHAIN_TOPICS[@]}"; do
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

# The snapshot payload: the real ask FIRST, the dust ask second, one bid.
raw_snapshot() {
    jq -cn --arg ts "$1" \
        --arg ap "$RAW_ASK_PRICE" --arg aq "$RAW_ASK_QTY" \
        --arg dp "$DUST_ASK_PRICE" --arg dq "$DUST_ASK_QTY" \
        --arg bp "$RAW_BID_PRICE" --arg bq "$RAW_BID_QTY" '
        {arg:{channel:"books-grouped",instId:"BTC-USDT",grouping:"1"},
         action:"snapshot",
         data:[{asks:[[$ap,$aq],[$dp,$dq]],bids:[[$bp,$bq]],ts:$ts}]}'
}

# An update touching ONE ask level and reporting no bids at all. The empty bids array is
# deliberate: for an update it means "no bid changes", and the book's existing bid must survive.
raw_update() {
    local ts="$1" price="$2" quantity="$3"
    jq -cn --arg ts "$ts" --arg p "$price" --arg q "$quantity" '
        {arg:{channel:"books-grouped",instId:"BTC-USDT",grouping:"1"},
         action:"update",
         data:[{asks:[[$p,$q]],bids:[],ts:$ts}]}'
}

produce() {
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

echo "Smoke-testing the chain job1->job5 via raw ex${EX} (OKX), key ex${EX}/p${PAIR}..."
echo

# 1. Snapshot seeds the book. The dust ask must NOT be in it (job 4 zeroed it, job 5 dropped it),
#    so asks has length 1. Also extends the verified timing chain through job 5.
run_case "snapshot seeds the book, dust never rests in it" "$BOOK_TOPIC" "$(raw_snapshot "$NOW")" \
    '.exchange_id' "$EX" '.pair_id' "$PAIR" \
    '.event_time' "$NOW" '.last_sequence_id.long' "$NOW" \
    '(.asks | length)' "1" \
    '.asks[0].price' "$WANT_ASK_PRICE" '.asks[0].quantity' "$WANT_ASK_QTY" \
    '(.bids | length)' "1" \
    '.bids[0].price' "$WANT_BID_PRICE" '.bids[0].quantity' "$WANT_BID_QTY" \
    "($PT.precision_out.long <= $PT.book_build_in.long)" "true" \
    "($PT.book_build_in.long <= $PT.book_build_out.long)" "true" \
    "($PT.book_build_in.long >= .event_time)" "true"

# 2. A contiguous update MERGES: the new ask is added, the ask from case 1 stays, and — the real
#    point — the bid from case 1 survives even though this event reported no bids at all. Asks
#    come out sorted ascending.
run_case "update merges into the book, other side untouched" "$BOOK_TOPIC" \
    "$(raw_update "$((NOW+JUMP))" "$RAW_ASK2_PRICE" "$RAW_ASK2_QTY")" \
    '.last_sequence_id.long' "$((NOW+JUMP))" \
    '(.asks | length)' "2" \
    '.asks[0].price' "$WANT_ASK_PRICE" \
    '.asks[1].price' "$WANT_ASK2_PRICE" '.asks[1].quantity' "$RAW_ASK2_QTY" \
    '(.bids | length)' "1" '.bids[0].price' "$WANT_BID_PRICE"

# 3. quantity 0 deletes that level and leaves the rest of the book alone.
run_case "quantity 0 deletes the level" "$BOOK_TOPIC" \
    "$(raw_update "$((NOW+2*JUMP))" "$RAW_ASK2_PRICE" "0")" \
    '(.asks | length)' "1" \
    '.asks[0].price' "$WANT_ASK_PRICE" '.asks[0].quantity' "$WANT_ASK_QTY" \
    '(.bids | length)' "1" '.bids[0].price' "$WANT_BID_PRICE"

echo
echo "Result: $pass passed, $fail failed."
[[ "$fail" -eq 0 ]]
