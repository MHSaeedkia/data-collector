#!/usr/bin/env bash
set -euo pipefail

# End-to-end smoke test for the aggregator job (raw pipeline TERMINAL job).
#
# SMOKE RULE for the normalizer pipeline (applies to every job's smoke):
#   Send RAW exchange payloads ONLY (to ex{id}-raw). Let the WHOLE Flink chain run
#   (job1 pair-extract -> job2 type-validate -> job3 rebase -> job4 precision -> job5 build
#    -> aggregator). Then capture the topic of the job under test and assert its contract. Never
#   produce a job's intermediate input directly — that bypasses upstream stamping and hides real
#   behaviour. And stamp event_time = the wall-clock time of execution.
#
# Drives ex8 (OKX), key (8, 1): its parser reads a single `ts` field that becomes BOTH event_time
# AND sequence_id (jump 300), so ts = now gives an execution-time event_time and full control of
# the sequence lifecycle. OKX BTC-USDT resolves to pair_id 1, so the live key is (8, 1) and the
# aggregator's output is p1-asks / p1-bids (subject aggregated-order-book-event — the FROZEN web
# contract). That contract carries NO pipeline_timings, so — unlike the jobs 1..5 smokes — there
# is no timing chain to assert here; the deliverable is the aggregated levels.
#
# WHY THE ASSERTIONS FILTER BY exchange_id: the aggregator unions ALL exchanges' books for a
# (pair, side) and holds MapState<exchange_id, book> that survives across cases AND across previous
# runs of this script. So it cannot assert "p1 has exactly N levels" — another exchange (or a prior
# run) may still contribute. Every assertion below is therefore scoped to ex8's own levels.
#
# The four cases run IN ORDER against one accumulating book and cover the WHOLE point of this job —
# the gap-driven exchange drop:
#   1. snapshot     -> ex8's ask+bid appear in p1-asks / p1-bids (stamped exchange_id 8).
#   2. update adds  -> ex8's second ask appears; the union grew.
#   3. GAP update   -> job 2 emits a type="reset" (and dead-letters the offending update), job 5
#                      empties ex8's book, and the aggregator DROPS ex8 from the union: p1-asks and
#                      p1-bids come out with NO ex8 levels. This is the milestone's core behaviour.
#   4. snapshot re-sync -> ex8 re-appears in the aggregated book.
#
# PREREQUISITE specific to case 3: the `raw-order-book-event` Type enum in the registry MUST include
# the "reset" symbol (schemas/raw_order_book_event.avsc) AND the jobs must have been (re)submitted
# after that registration — a serializer caches the schema on first use. If "reset" is missing, job 2
# NPEs serializing the reset and case 3 fails with no event on p1. See project_type_validator.md.
#
# Prerequisites (this is a TEST, not a deploy):
#   - the normalizer stack is up (docker-compose.yml)
#   - the DB is warmed (scripts/warmup.sh) so exchange_markets resolves OKX BTC-USDT -> p1
#   - ALL SIX jobs are submitted (see the precondition check below)
#   - no competing live OKX feed is producing onto ex8-* (this writes real (8,1) state)
#   - NOTE this writes to the real web-output topics p1-{side}; a running web UI will render it.
#
# Usage: ./smoke-aggregator.sh
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
      normalizer-precision normalizer-book-builder normalizer-aggregator)
MODULES=(job-pair-extractor job-type-validator job-rebaser job-precision job-book-builder
         job-aggregator)

EX=8
PAIR=1
JUMP=300                       # OKX's hardcoded per-message sequence cadence (see OkxParser)
RAW_TOPIC="ex${EX}-raw"
ASKS_TOPIC="p${PAIR}-asks"
BIDS_TOPIC="p${PAIR}-bids"
CHAIN_TOPICS=("ex${EX}-p${PAIR}-raw-flink" "ex${EX}-p${PAIR}-type-validated-raw-flink"
              "ex${EX}-p${PAIR}-rebased-flink" "ex${EX}-p${PAIR}-applied-precision-flink"
              "ex${EX}-p${PAIR}-orderbook-snapshot-flink" "ex${EX}-p${PAIR}-rejected-flink")

WANT_PRICE_PRECISION=2
WANT_QTY_PRECISION=8

# Raw inputs carry more decimals than the precisions allow; job 4 truncates DOWN. These are the
# digits that must surface in the aggregated book.
RAW_ASK_PRICE="62770.98765";  RAW_ASK_QTY="0.123456789"
RAW_BID_PRICE="62769.43219";  RAW_BID_QTY="1.999999999"
WANT_ASK_PRICE="62770.98";    WANT_ASK_QTY="0.12345678"
WANT_BID_PRICE="62769.43";    WANT_BID_QTY="1.99999999"

# A second ask added by case 2.
RAW_ASK2_PRICE="62772.98765"; RAW_ASK2_QTY="0.5"
WANT_ASK2_PRICE="62772.98"

command -v jq   >/dev/null || { echo "jq is required";   exit 1; }
command -v curl >/dev/null || { echo "curl is required"; exit 1; }

# --- precondition: ALL SIX jobs RUNNING (the aggregator's input only exists if 1..5 run) ---
overview=$(curl -s --max-time 5 "$FLINK_API/jobs/overview" 2>/dev/null || true)
for i in "${!JOBS[@]}"; do
    running=$(jq -r --arg n "${JOBS[$i]}" '.jobs[]? | select(.state=="RUNNING" and .name==$n) | .name' \
        <<<"$overview" 2>/dev/null || true)
    if [[ -z "$running" ]]; then
        echo "ERROR: job '${JOBS[$i]}' is not RUNNING at $FLINK_API."
        echo "       Submit all six (terminal-first is fine — they read latest):"
        printf "         %s/run-job.sh %s\n" "$SCRIPT_DIR" "${MODULES[@]}"
        exit 1
    fi
done

for t in "$RAW_TOPIC" "$ASKS_TOPIC" "$BIDS_TOPIC" "${CHAIN_TOPICS[@]}"; do
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
    exit 1
fi

pass=0
fail=0

raw_snapshot() {
    jq -cn --arg ts "$1" \
        --arg ap "$RAW_ASK_PRICE" --arg aq "$RAW_ASK_QTY" \
        --arg bp "$RAW_BID_PRICE" --arg bq "$RAW_BID_QTY" '
        {arg:{channel:"books-grouped",instId:"BTC-USDT",grouping:"1"},
         action:"snapshot",
         data:[{asks:[[$ap,$aq]],bids:[[$bp,$bq]],ts:$ts}]}'
}

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

# One raw message produces exactly one aggregated event per side (job 5 emits one book -> the
# splitter two sides -> the aggregator one event per side), so read a single record per topic.
read_one() {
    local topic="$1" start="$2"
    docker exec "$SR_CONTAINER" kafka-avro-console-consumer \
        --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$topic" \
        --property schema.registry.url="$SR_URL" \
        --partition 0 --offset "$start" --max-messages 1 \
        --timeout-ms "$CONSUME_TIMEOUT_MS" 2>/dev/null | grep -m1 '^{' || true
}

# Assertions scoped to ex8's levels within a aggregated record.
expect_ex8_level() {   # <record> <price> <quantity>
    local rec="$1" price="$2" quantity="$3" got
    got="$(jq -r --arg p "$price" '.levels[] | select(.exchange_id==8 and .price==$p) | .quantity' <<<"$rec" | head -1)"
    [[ "$got" == "$quantity" ]] || errs+=("ex8 $price: quantity '${got:-<absent>}' want '$quantity'")
}
expect_ex8_absent() {  # <record>  (ex8 dropped from the union)
    local rec="$1" n
    n="$(jq -r '[.levels[] | select(.exchange_id==8)] | length' <<<"$rec")"
    [[ "$n" == "0" ]] || errs+=("expected ex8 dropped from union but $n ex8 level(s) present")
}
expect_field() {       # <record> <jq-filter> <want>
    local rec="$1" filter="$2" want="$3" got
    got="$(jq -r "$filter" <<<"$rec")"
    [[ "$got" == "$want" ]] || errs+=("$filter=$got want $want")
}

report() {
    local label="$1"
    if [[ ${#errs[@]} -eq 0 ]]; then
        echo "PASS  $label"
        pass=$((pass+1))
    else
        echo "FAIL  $label"
        printf '        - %s\n' "${errs[@]}"
        fail=$((fail+1))
    fi
}

NOW="$(( $(date +%s) * 1000 ))"   # epoch millis (portable) — newer than any prior run's baseline

echo "Smoke-testing the chain job1->aggregator via raw ex${EX} (OKX), key ex${EX}/p${PAIR} -> ${ASKS_TOPIC}/${BIDS_TOPIC}..."
echo

# --- case 1: a snapshot puts ex8 into the aggregated book, stamped with its exchange_id ---
errs=()
a_start="$(end_offset "$ASKS_TOPIC")"; a_start="${a_start:-0}"
b_start="$(end_offset "$BIDS_TOPIC")"; b_start="${b_start:-0}"
produce "$(raw_snapshot "$NOW")"
a_rec="$(read_one "$ASKS_TOPIC" "$a_start")"
b_rec="$(read_one "$BIDS_TOPIC" "$b_start")"
if [[ -z "$a_rec" ]]; then errs+=("no event on $ASKS_TOPIC"); fi
if [[ -z "$b_rec" ]]; then errs+=("no event on $BIDS_TOPIC"); fi
if [[ -n "$a_rec" ]]; then
    expect_field "$a_rec" '.pair_id' "$PAIR"
    expect_field "$a_rec" '.side' "asks"
    expect_field "$a_rec" '.event_time' "$NOW"
    expect_ex8_level "$a_rec" "$WANT_ASK_PRICE" "$WANT_ASK_QTY"
fi
if [[ -n "$b_rec" ]]; then
    expect_field "$b_rec" '.side' "bids"
    expect_ex8_level "$b_rec" "$WANT_BID_PRICE" "$WANT_BID_QTY"
fi
report "snapshot -> ex8 appears in the aggregated book (exchange_id stamped)"

# --- case 2: a contiguous update adds a second ask -> it joins ex8's union entry ---
errs=()
a_start="$(end_offset "$ASKS_TOPIC")"; a_start="${a_start:-0}"
produce "$(raw_update "$((NOW+JUMP))" "$RAW_ASK2_PRICE" "$RAW_ASK2_QTY")"
a_rec="$(read_one "$ASKS_TOPIC" "$a_start")"
if [[ -z "$a_rec" ]]; then errs+=("no event on $ASKS_TOPIC"); fi
if [[ -n "$a_rec" ]]; then
    expect_ex8_level "$a_rec" "$WANT_ASK_PRICE" "$WANT_ASK_QTY"
    expect_ex8_level "$a_rec" "$WANT_ASK2_PRICE" "$RAW_ASK2_QTY"
fi
report "update -> ex8's second ask joins the aggregated book"

# --- case 3: a sequence GAP -> reset -> ex8 drops out of BOTH sides of the union ---
errs=()
a_start="$(end_offset "$ASKS_TOPIC")"; a_start="${a_start:-0}"
b_start="$(end_offset "$BIDS_TOPIC")"; b_start="${b_start:-0}"
# ts jumps to NOW+4*JUMP (expected NOW+2*JUMP) -> job 2 sees a forward gap, dead-letters this
# update, and emits a reset. Job 5 empties ex8's book; the aggregator drops ex8 from the union.
produce "$(raw_update "$((NOW+4*JUMP))" "$RAW_ASK2_PRICE" "$RAW_ASK2_QTY")"
a_rec="$(read_one "$ASKS_TOPIC" "$a_start")"
b_rec="$(read_one "$BIDS_TOPIC" "$b_start")"
if [[ -z "$a_rec" ]]; then errs+=("no event on $ASKS_TOPIC (reset never propagated — is 'reset' in the Type enum? are jobs resubmitted?)"); fi
if [[ -z "$b_rec" ]]; then errs+=("no event on $BIDS_TOPIC (reset never propagated — is 'reset' in the Type enum? are jobs resubmitted?)"); fi
[[ -n "$a_rec" ]] && expect_ex8_absent "$a_rec"
[[ -n "$b_rec" ]] && expect_ex8_absent "$b_rec"
report "gap -> reset -> ex8 drops out of the aggregated book"

# --- case 4: a snapshot re-sync brings ex8 back ---
errs=()
a_start="$(end_offset "$ASKS_TOPIC")"; a_start="${a_start:-0}"
b_start="$(end_offset "$BIDS_TOPIC")"; b_start="${b_start:-0}"
produce "$(raw_snapshot "$((NOW+10*JUMP))")"
a_rec="$(read_one "$ASKS_TOPIC" "$a_start")"
b_rec="$(read_one "$BIDS_TOPIC" "$b_start")"
if [[ -z "$a_rec" ]]; then errs+=("no event on $ASKS_TOPIC"); fi
if [[ -z "$b_rec" ]]; then errs+=("no event on $BIDS_TOPIC"); fi
[[ -n "$a_rec" ]] && expect_ex8_level "$a_rec" "$WANT_ASK_PRICE" "$WANT_ASK_QTY"
[[ -n "$b_rec" ]] && expect_ex8_level "$b_rec" "$WANT_BID_PRICE" "$WANT_BID_QTY"
report "snapshot re-sync -> ex8 returns to the aggregated book"

echo
echo "Result: $pass passed, $fail failed."
[[ "$fail" -eq 0 ]]
