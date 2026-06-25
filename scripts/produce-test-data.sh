#!/usr/bin/env bash
set -euo pipefail

# Streams randomized order book snapshots to Kafka so the UI shows LIVE updates.
# Each tick: pick a random (pair, side, exchange), generate a fresh randomized snapshot
# (prices drift around a mid; random quantities and level counts), emit it, and pause on
# some steps to create visible bursts and gaps. Runs for DURATION_SECONDS (default 10 min).
#
# Pairs: BTC-USDT, TON-USDT — both sides — exchanges: nobitex, wallex, bitpin.
#
# Prereqs:
#   - Kafka up:  docker compose up -d
#   - For the FLINK JOB to consume these, BTC-USDT and TON-USDT must be status='subscribe'
#     in postgres for nobitex/wallex/bitpin, AND the job must be RUNNING before producing
#     (the source uses latest() offsets — earlier messages are skipped).
#
# Config (env vars):
#   DURATION_SECONDS  total run time in seconds (default 600)
#   KAFKA_CONTAINER   kafka container name (default 'kafka')
#   KAFKA_BOOTSTRAP   broker address (default 'kafka:29092')

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
DURATION_SECONDS="${DURATION_SECONDS:-600}"

EXCHANGES=(nobitex wallex bitpin)
PAIRS=(BTC-USDT TON-USDT)
SIDES=(asks bids)

command -v jq  >/dev/null 2>&1 || { echo "jq is required but not installed."; exit 1; }
command -v awk >/dev/null 2>&1 || { echo "awk is required but not installed."; exit 1; }
docker exec "$KAFKA_CONTAINER" true 2>/dev/null \
    || { echo "Kafka container '$KAFKA_CONTAINER' is not running. Start it: docker compose up -d"; exit 1; }

# --- ensure input topics exist (idempotent) ---
echo "Ensuring topics exist..."
for pair in "${PAIRS[@]}"; do
    for side in "${SIDES[@]}"; do
        for ex in "${EXCHANGES[@]}"; do
            docker exec "$KAFKA_CONTAINER" kafka-topics \
                --bootstrap-server "$KAFKA_BOOTSTRAP" \
                --create --if-not-exists \
                --topic "${pair}-${side}-${ex}" \
                --partitions 1 --replication-factor 1 >/dev/null
        done
    done
done

# Per-pair generation params: base step price_decimals qty_min qty_max qty_decimals
pair_params() {
    case "$1" in
        BTC-USDT) echo "100000 20 2 0.01 2.5 4" ;;
        TON-USDT) echo "5.50 0.01 4 20 900 2" ;;
        *)        echo "100 1 2 1 100 2" ;;
    esac
}

# Generate a randomized levels JSON array for a (pair, side).
gen_levels() {
    local pair="$1" side="$2"
    read -r base step pdec qmin qmax qdec <<< "$(pair_params "$pair")"
    local n=$(( (RANDOM % 5) + 3 ))   # 3..7 levels
    awk -v base="$base" -v step="$step" -v pdec="$pdec" -v qmin="$qmin" -v qmax="$qmax" \
        -v qdec="$qdec" -v side="$side" -v n="$n" -v seed="$RANDOM" 'BEGIN {
        srand(seed);
        mid = base + (rand() * 10 - 5) * step;   # drift the mid a few steps each snapshot
        printf("[");
        for (i = 0; i < n; i++) {
            off = (i + 1) * step * (1 + rand());
            price = (side == "asks") ? mid + off : mid - off;
            qty = qmin + rand() * (qmax - qmin);
            if (i > 0) printf(",");
            printf("{\"price\":\"%.*f\",\"quantity\":\"%.*f\"}", pdec, price, qdec, qty);
        }
        printf("]");
    }'
}

# Emit one snapshot to {pair}-{side}-{exchange}.
emit() {
    local pair="$1" side="$2" exchange="$3" levels="$4"
    local topic="${pair}-${side}-${exchange}"
    local msg
    msg=$(jq -cn \
        --arg exchange "$exchange" --arg pair "$pair" --arg side "$side" \
        --argjson event_time "$(date +%s)000" --argjson levels "$levels" \
        '{exchange:$exchange,pair:$pair,side:$side,event_time:$event_time,levels:$levels}')
    echo "$msg" | docker exec -i "$KAFKA_CONTAINER" kafka-console-producer \
        --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$topic" 2>/dev/null
}

# --- main loop ---
count=0
trap 'echo; echo "Stopped. Sent $count snapshots."; exit 0' INT
end=$(( $(date +%s) + DURATION_SECONDS ))
echo "Streaming randomized snapshots for ${DURATION_SECONDS}s (Ctrl-C to stop)..."

while [ "$(date +%s)" -lt "$end" ]; do
    pair="${PAIRS[$((RANDOM % ${#PAIRS[@]}))]}"
    side="${SIDES[$((RANDOM % ${#SIDES[@]}))]}"
    exchange="${EXCHANGES[$((RANDOM % ${#EXCHANGES[@]}))]}"

    emit "$pair" "$side" "$exchange" "$(gen_levels "$pair" "$side")" \
        || printf '\n  emit failed, continuing\n'
    count=$((count + 1))
    printf '\r  sent %d  (last: %-9s %-4s %-8s)   ' "$count" "$pair" "$side" "$exchange"

    # Pause on ~1 of every 3 steps to create visible bursts and gaps.
    if [ $((RANDOM % 3)) -eq 0 ]; then
        sleep "$(awk -v s="$RANDOM" 'BEGIN{srand(s); printf("%.1f", 1 + rand() * 2)}')"
    fi
done

echo
echo "Done. Sent $count snapshots over ${DURATION_SECONDS}s."
