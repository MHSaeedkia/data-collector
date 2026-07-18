#!/usr/bin/env bash
set -euo pipefail

# End-to-end smoke test for the level emitter job (raw pipeline job 6 — the LAST job).
#
# SMOKE RULE for the normalizer pipeline (applies to every job's smoke, jobs 1..6):
#   Send RAW exchange payloads ONLY (to ex{id}-raw). Let the WHOLE Flink chain run
#   (job1 pair-extract -> job2 type-validate -> job3 rebase -> job4 precision -> job5 build
#    -> job6 emit). Then capture the topic of the job under test and assert its contract. Never
#   produce a job's intermediate input directly — that bypasses upstream stamping and hides real
#   behaviour. And stamp event_time = the wall-clock time of execution, so the per-stage
#   pipeline_timings can be verified as real, monotonically-increasing processing times.
#
# Drives ex8 (OKX), key (8, 1) — same feed as the job-5 smoke, for the same reason: its `ts`
# becomes BOTH event_time and sequence_id, so the sequence lifecycle is fully controllable.
#
# WHY THE ASSERTIONS LOOK DIFFERENT FROM EVERY OTHER SMOKE HERE:
# job 6 emits a DIFF, so what it emits depends on what it emitted BEFORE — including in previous
# runs of this script, whose state is still in the job. So this script cannot assert "the topic
# received exactly N records". Instead:
#   - every run uses a price band derived from the current clock, so its levels are new to the job
#   - each case reads the batch of records produced by its own event and asserts that a specific
#     (price, quantity) is present, or absent
# Deletes of a previous run's levels may ride along in the same batch. That is correct behaviour,
# not noise, and it is exactly why the assertions are per-level rather than per-count.
#
# The three cases run IN ORDER against one accumulating book:
#   1. snapshot        -> the new ask and bid are emitted as upserts (a price job 6 has never seen)
#   2. update adds ask -> ONLY the added ask is emitted; the untouched ask from case 1 is NOT
#                         re-emitted. This is the whole point of the job: job 5 re-sends the full
#                         book on every tick, job 6 must not forward that amplification.
#   3. update qty 0    -> the level is emitted with quantity "0", the consolidator's delete signal.
#
# Output goes to the EXISTING ex{id}-p{id}-{side} topics with the frozen `price-level-event`
# subject — the same bytes the consolidator already consumes from NiFi today.
#
# Prerequisites (this is a TEST, not a deploy):
#   - the normalizer stack is up (docker-compose-normalizer.yml)
#   - the DB is warmed (scripts/warmup.sh) so exchange_markets resolves OKX BTC-USDT -> p1
#   - ALL SIX jobs are submitted (see the precondition check below)
#   - no competing live OKX feed is producing onto ex8-*
#   - NOTE this writes to the real consolidator input topics; a running consolidator will ingest
#     these levels. That is intended (it is the M7 verification path), but it is not a dry run.
#
# Usage: ./smoke-level-emitter.sh
# Env overrides: FLINK_API, KAFKA_CONTAINER, SR_CONTAINER, PG_CONTAINER, KAFKA_BOOTSTRAP,
#                SR_URL, BATCH_TIMEOUT_MS

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

FLINK_API="${FLINK_API:-http://localhost:7070}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
SR_CONTAINER="${SR_CONTAINER:-schema-registry}"
PG_CONTAINER="${PG_CONTAINER:-postgres}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
SR_URL="${SR_URL:-http://schema-registry:8082}"
BATCH_TIMEOUT_MS="${BATCH_TIMEOUT_MS:-15000}"
BATCH_MAX="${BATCH_MAX:-20}"

JOBS=(normalizer-pair-extractor normalizer-type-validator normalizer-rebaser
      normalizer-precision normalizer-book-builder normalizer-level-emitter)
MODULES=(job-pair-extractor job-type-validator job-rebaser job-precision job-book-builder
         job-level-emitter)

EX=8
PAIR=1
JUMP=300                       # OKX's hardcoded per-message sequence cadence (see OkxParser)
RAW_TOPIC="ex${EX}-raw"
ASKS_TOPIC="ex${EX}-p${PAIR}-asks"
BIDS_TOPIC="ex${EX}-p${PAIR}-bids"
CHAIN_TOPICS=("ex${EX}-p${PAIR}-raw-flink" "ex${EX}-p${PAIR}-type-validated-raw-flink"
              "ex${EX}-p${PAIR}-rebased-flink" "ex${EX}-p${PAIR}-applied-precision-flink"
              "ex${EX}-p${PAIR}-orderbook-snapshot-flink" "ex${EX}-p${PAIR}-rejected-flink")

WANT_PRICE_PRECISION=2
WANT_QTY_PRECISION=8

NOW="$(( $(date +%s) * 1000 ))"   # epoch millis (portable)

# A price band unique to this run, so every level below is one job 6 has never emitted before.
BAND=$(( 60000 + (NOW / 1000) % 2000 ))

RAW_ASK_PRICE="${BAND}.98765";  RAW_ASK_QTY="0.123456789"
RAW_BID_PRICE="${BAND}.43219";  RAW_BID_QTY="1.999999999"
WANT_ASK_PRICE="${BAND}.98";    WANT_ASK_QTY="0.12345678"
WANT_BID_PRICE="${BAND}.43";    WANT_BID_QTY="1.99999999"

# A second ask added by case 2, then deleted by case 3.
RAW_ASK2_PRICE="$((BAND+1)).98765"; RAW_ASK2_QTY="0.5"
WANT_ASK2_PRICE="$((BAND+1)).98"

PT='.pipeline_timings["io.tibobit.orderbook.PipelineTimings"]'

command -v jq   >/dev/null || { echo "jq is required";   exit 1; }
command -v curl >/dev/null || { echo "curl is required"; exit 1; }

# --- precondition: ALL SIX jobs RUNNING (job 6's input only exists if 1..5 run) ---
overview=$(curl -s --max-time 5 "$FLINK_API/jobs/overview" 2>/dev/null || true)
for i in "${!JOBS[@]}"; do
    running=$(jq -r --arg n "${JOBS[$i]}" '.jobs[]? | select(.state=="RUNNING" and .name==$n) | .name' \
        <<<"$overview" 2>/dev/null || true)
    if [[ -z "$running" ]]; then
        echo "ERROR: job '${JOBS[$i]}' is not RUNNING at $FLINK_API."
        echo "       Submit all six:"
        printf "         %s/run-job.sh %s\n" "$SCRIPT_DIR" "${MODULES[*]}"
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
    exit 1
fi
REBASE="$(psql_do "SELECT price_amount_rebase || ' ' || volume_amount_rebase FROM exchange_markets WHERE exchange_id=$EX AND market_id=$PAIR")"
if [[ "$REBASE" != "0 0" ]]; then
    echo "ERROR: exchange_markets rebase for ($EX, p$PAIR) is '$REBASE', expected '0 0'."
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

# Reads everything this event produced on a topic. Unlike the other smokes' read_one, the count is
# not known in advance (a diff emits as many records as levels changed), so this drains until the
# timeout and returns one JSON record per line.
read_batch() {
    local topic="$1" start="$2"
    docker exec "$SR_CONTAINER" kafka-avro-console-consumer \
        --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$topic" \
        --property schema.registry.url="$SR_URL" \
        --partition 0 --offset "$start" --max-messages "$BATCH_MAX" \
        --timeout-ms "$BATCH_TIMEOUT_MS" 2>/dev/null | grep '^{' || true
}

# Asserts that a batch contains (or does not contain) a record for a given price.
#   expect_level  <batch> <price> <quantity>
#   expect_absent <batch> <price>
expect_level() {
    local batch="$1" price="$2" quantity="$3"
    local got
    got="$(jq -r --arg p "$price" 'select(.price == $p) | .quantity' <<<"$batch" | head -1)"
    if [[ "$got" == "$quantity" ]]; then
        return 0
    fi
    errs+=("price $price: quantity '${got:-<not emitted>}' want '$quantity'")
}

expect_absent() {
    local batch="$1" price="$2"
    local got
    got="$(jq -r --arg p "$price" 'select(.price == $p) | .quantity' <<<"$batch" | head -1)"
    [[ -z "$got" ]] || errs+=("price $price: emitted quantity '$got' but must not have been emitted")
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

echo "Smoke-testing the chain job1->job6 via raw ex${EX} (OKX), key ex${EX}/p${PAIR}, price band ${BAND}..."
echo

# --- case 1: a snapshot introduces two levels job 6 has never emitted -> both are upserts ---
errs=()
asks_start="$(end_offset "$ASKS_TOPIC")"; asks_start="${asks_start:-0}"
bids_start="$(end_offset "$BIDS_TOPIC")"; bids_start="${bids_start:-0}"
produce "$(raw_snapshot "$NOW")"
asks_batch="$(read_batch "$ASKS_TOPIC" "$asks_start")"
bids_batch="$(read_batch "$BIDS_TOPIC" "$bids_start")"

expect_level "$asks_batch" "$WANT_ASK_PRICE" "$WANT_ASK_QTY"
expect_level "$bids_batch" "$WANT_BID_PRICE" "$WANT_BID_QTY"

# The record's shape is the frozen consolidator contract — check it once, here.
ask_rec="$(jq -c --arg p "$WANT_ASK_PRICE" 'select(.price == $p)' <<<"$asks_batch" | head -1)"
if [[ -z "$ask_rec" ]]; then
    errs+=("no ask record for $WANT_ASK_PRICE to inspect")
else
    checks=('.exchange_id' "$EX"
            '.pair_id' "$PAIR"
            '.side' "asks"
            '.event_time' "$NOW"
            "($PT.book_build_out.long <= $PT.level_emit_in.long)" "true"
            "($PT.level_emit_in.long <= $PT.level_emit_out.long)" "true"
            "($PT.level_emit_in.long >= .event_time)" "true")
    for ((i = 0; i < ${#checks[@]}; i += 2)); do
        got="$(jq -r "${checks[i]}" <<<"$ask_rec")"
        [[ "$got" == "${checks[i+1]}" ]] || errs+=("${checks[i]}=$got want ${checks[i+1]}")
    done
fi
report "snapshot emits each new level once, in the consolidator's format"

# --- case 2: an update adds ONE ask -> only that ask is emitted, the untouched one is not ---
errs=()
asks_start="$(end_offset "$ASKS_TOPIC")"; asks_start="${asks_start:-0}"
produce "$(raw_update "$((NOW+JUMP))" "$RAW_ASK2_PRICE" "$RAW_ASK2_QTY")"
asks_batch="$(read_batch "$ASKS_TOPIC" "$asks_start")"

expect_level "$asks_batch" "$WANT_ASK2_PRICE" "$RAW_ASK2_QTY"
expect_absent "$asks_batch" "$WANT_ASK_PRICE"
report "update emits only the changed level, not the whole book"

# --- case 3: quantity 0 -> the delete the consolidator acts on ---
errs=()
asks_start="$(end_offset "$ASKS_TOPIC")"; asks_start="${asks_start:-0}"
produce "$(raw_update "$((NOW+2*JUMP))" "$RAW_ASK2_PRICE" "0")"
asks_batch="$(read_batch "$ASKS_TOPIC" "$asks_start")"

expect_level "$asks_batch" "$WANT_ASK2_PRICE" "0"
expect_absent "$asks_batch" "$WANT_ASK_PRICE"
report "a vanished level is emitted as quantity 0"

echo
echo "Result: $pass passed, $fail failed."
[[ "$fail" -eq 0 ]]
