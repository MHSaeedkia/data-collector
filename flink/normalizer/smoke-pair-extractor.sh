#!/usr/bin/env bash
set -euo pipefail

# End-to-end smoke test for the pair-extractor job (raw pipeline job 1).
#
# For every exchange it: produces ONE verbatim raw fixture to `ex{id}-raw`, then reads back
# the Confluent-Avro event the running job emits on `ex{id}-p{pair_id}-raw-flink` and asserts
# the routing/parse contract (exchange_id, pair_id, type, sequence_id, side shape).
#
# Prerequisites (this is a TEST, not a deploy):
#   - the normalizer stack is up (docker-compose.yml)
#   - the DB is warmed (scripts/warmup.sh) so exchange_markets resolves each fixture's market
#   - the job is already submitted:  ./run-job.sh job-pair-extractor
#
# Usage: ./smoke-pair-extractor.sh
# Env overrides: FLINK_API, KAFKA_CONTAINER, SR_CONTAINER, KAFKA_BOOTSTRAP, SR_URL,
#                EXPECTED_PAIR_ID, ASSIGN_WAIT, CONSUME_TIMEOUT_MS

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURES="$SCRIPT_DIR/job-pair-extractor/src/test/resources/fixtures"

FLINK_API="${FLINK_API:-http://localhost:7070}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
SR_CONTAINER="${SR_CONTAINER:-schema-registry}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
SR_URL="${SR_URL:-http://schema-registry:8082}"
JOB_NAME="normalizer-pair-extractor"

# Every seeded BTC market maps to market_id 1 in the warmed DB, so all fixtures route to p1.
EXPECTED_PAIR_ID="${EXPECTED_PAIR_ID:-1}"
CONSUME_TIMEOUT_MS="${CONSUME_TIMEOUT_MS:-25000}"

# exid : fixture (no .json) : expected type : side shape (both|bids|asks|any)
# ex7 is postponed (no parser); ex3 sends one side per message; ex6/ex8 also carry an update.
# ex1 nobitex has TWO streams: the REST snapshot (null seq) and the WS delta (update, jump 1).
CASES=(
    "1:ex1-snapshot:snapshot:both"
    "1:ex1-update:update:both"
    "2:ex2-snapshot:snapshot:both"
    "3:ex3-buy-depth:snapshot:bids"
    "3:ex3-sell-depth:snapshot:asks"
    "4:ex4-snapshot:snapshot:both"
    "5:ex5-snapshot:snapshot:both"
    "6:ex6-snapshot:snapshot:both"
    "6:ex6-delta:update:any"
    "8:ex8-snapshot:snapshot:both"
    "8:ex8-update:update:any"
)

command -v jq   >/dev/null || { echo "jq is required";   exit 1; }
command -v curl >/dev/null || { echo "curl is required"; exit 1; }

# --- precondition: the job is RUNNING ---
running=$(curl -s --max-time 5 "$FLINK_API/jobs/overview" \
    | jq -r --arg n "$JOB_NAME" '.jobs[]? | select(.state=="RUNNING" and .name==$n) | .name' 2>/dev/null || true)
if [[ -z "$running" ]]; then
    echo "ERROR: job '$JOB_NAME' is not RUNNING at $FLINK_API."
    echo "       Submit it first:  $SCRIPT_DIR/run-job.sh job-pair-extractor"
    exit 1
fi

pass=0
fail=0

run_case() {
    local exid="$1" fixture="$2" want_type="$3" shape="$4"
    local in_topic="ex${exid}-raw"
    local out_topic="ex${exid}-p${EXPECTED_PAIR_ID}-raw-flink"
    local label="ex${exid}/${fixture}"
    local fixture_file="$FIXTURES/${fixture}.json"

    if [[ ! -f "$fixture_file" ]]; then
        echo "FAIL  $label — fixture not found: $fixture_file"; fail=$((fail+1)); return
    fi

    # Pre-create both topics so neither the produce nor the fresh consumer races topic creation.
    docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" \
        --create --if-not-exists --topic "$in_topic"  --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true
    docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" \
        --create --if-not-exists --topic "$out_topic" --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true

    # Capture the output topic's end offset BEFORE producing, then read from exactly there:
    # deterministic (no consumer-group / auto.offset.reset=latest race), and it sees only the
    # event our produce triggers — nothing else writes this topic.
    local start_off; start_off=$(docker exec "$KAFKA_CONTAINER" kafka-get-offsets \
        --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$out_topic" --time latest 2>/dev/null | cut -d: -f3)
    start_off="${start_off:-0}"

    # Produce the fixture verbatim, compacted to a single line = a single Kafka record.
    if ! jq -c . "$fixture_file" | docker exec -i "$KAFKA_CONTAINER" kafka-console-producer \
            --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$in_topic" >/dev/null 2>&1; then
        echo "FAIL  $label — produce to $in_topic failed"; fail=$((fail+1)); return
    fi

    # Read the one record the job emits at/after that offset (log4j noise shares stdout).
    local rec; rec=$(docker exec "$SR_CONTAINER" kafka-avro-console-consumer \
        --bootstrap-server "$KAFKA_BOOTSTRAP" \
        --topic "$out_topic" \
        --property schema.registry.url="$SR_URL" \
        --partition 0 --offset "$start_off" \
        --max-messages 1 --timeout-ms "$CONSUME_TIMEOUT_MS" 2>/dev/null | grep -m1 '^{' || true)

    if [[ -z "$rec" ]]; then
        echo "FAIL  $label — no event on $out_topic within ${CONSUME_TIMEOUT_MS}ms"
        fail=$((fail+1)); return
    fi

    # --- assertions (union fields are wrapped: sequence_id->{long}, asks/bids->{array}) ---
    local errs=()
    local got_ex got_pair got_type
    got_ex=$(jq -r '.exchange_id'  <<<"$rec")
    got_pair=$(jq -r '.pair_id'    <<<"$rec")
    got_type=$(jq -r '.type'       <<<"$rec")
    [[ "$got_ex"   == "$exid"             ]] || errs+=("exchange_id=$got_ex want $exid")
    [[ "$got_pair" == "$EXPECTED_PAIR_ID" ]] || errs+=("pair_id=$got_pair want $EXPECTED_PAIR_ID")
    [[ "$got_type" == "$want_type"        ]] || errs+=("type=$got_type want $want_type")

    # sequence_id: null for feeds with no ordering field on the wire — ex3 (wallex) and the
    # ex1 (nobitex) REST snapshot; everyone else (incl. the ex1 WS update) stamps one.
    local seq_null; seq_null=$(jq -r '.sequence_id == null' <<<"$rec")
    if [[ "$exid" == "3" || ( "$exid" == "1" && "$want_type" == "snapshot" ) ]]; then
        [[ "$seq_null" == "true" ]] || errs+=("sequence_id not null (expected null)")
    else
        [[ "$seq_null" == "false" ]] || errs+=("sequence_id null (expected a value)")
    fi

    # side shape
    local na nb
    na=$(jq -r '(.asks.array | length) // 0' <<<"$rec")
    nb=$(jq -r '(.bids.array | length) // 0' <<<"$rec")
    local a_null b_null
    a_null=$(jq -r '.asks == null' <<<"$rec")
    b_null=$(jq -r '.bids == null' <<<"$rec")
    case "$shape" in
        both) { [[ "$na" -gt 0 && "$nb" -gt 0 ]] || errs+=("want both sides, got asks=$na bids=$nb"); } ;;
        bids) { [[ "$nb" -gt 0 && "$a_null" == "true" ]] || errs+=("want bids-only, got asks=$na(null=$a_null) bids=$nb"); } ;;
        asks) { [[ "$na" -gt 0 && "$b_null" == "true" ]] || errs+=("want asks-only, got asks=$na bids=$nb(null=$b_null)"); } ;;
        any)  { [[ $((na + nb)) -gt 0 ]] || errs+=("want >=1 level, got asks=$na bids=$nb"); } ;;
    esac

    if [[ ${#errs[@]} -eq 0 ]]; then
        local note=""
        [[ "$(jq -r 'has("pipeline_timings")' <<<"$rec")" == "true" ]] || note="  (no pipeline_timings on this build)"
        echo "PASS  $label -> $out_topic  [ex=$got_ex pair=$got_pair type=$got_type asks=$na bids=$nb]$note"
        pass=$((pass+1))
    else
        echo "FAIL  $label -> $out_topic"
        printf '        - %s\n' "${errs[@]}"
        fail=$((fail+1))
    fi
}

echo "Smoke-testing '$JOB_NAME' across ${#CASES[@]} fixtures (expected pair_id=$EXPECTED_PAIR_ID)..."
echo
for c in "${CASES[@]}"; do
    IFS=':' read -r exid fixture want_type shape <<<"$c"
    run_case "$exid" "$fixture" "$want_type" "$shape"
done

echo
echo "Result: $pass passed, $fail failed."
[[ "$fail" -eq 0 ]]
