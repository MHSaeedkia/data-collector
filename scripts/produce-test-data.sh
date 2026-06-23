#!/usr/bin/env bash
set -euo pipefail

# Sends curated fake order book snapshots to Kafka to exercise OrderBookMerger.
# Pairs: BTC-USDT, TON-USDT — both sides — exchanges: nobitex, wallex, bitpin.
# Same-price collisions across exchanges are intentional: they verify the merge does
# UNION (not sum) and applies the equal-price tie-break (larger quantity first).
#
# Prereqs:
#   - Kafka up:  docker compose up -d
#   - For the FLINK JOB to consume these, BTC-USDT and TON-USDT must be status='subscribe'
#     in postgres for nobitex/wallex/bitpin, AND the job must be RUNNING before producing
#     (the source uses latest() offsets — earlier messages are skipped).

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
EVENT_TIME="${EVENT_TIME:-$(date +%s)000}"   # epoch milliseconds

EXCHANGES=(nobitex wallex bitpin)
PAIRS=(BTC-USDT TON-USDT)
SIDES=(asks bids)

command -v jq >/dev/null 2>&1 || { echo "jq is required but not installed."; exit 1; }
docker exec "$KAFKA_CONTAINER" true 2>/dev/null \
    || { echo "Kafka container '$KAFKA_CONTAINER' is not running. Start it: docker compose up -d"; exit 1; }

# --- ensure topics exist (idempotent) ---
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

# --- produce one snapshot per (pair, side, exchange) ---
emit() {
    local pair="$1" side="$2" exchange="$3" levels="$4"
    local topic="${pair}-${side}-${exchange}"
    local msg
    msg=$(jq -cn \
        --arg exchange "$exchange" --arg pair "$pair" --arg side "$side" \
        --argjson event_time "$EVENT_TIME" --argjson levels "$levels" \
        '{exchange:$exchange,pair:$pair,side:$side,event_time:$event_time,levels:$levels}')
    echo "$msg" | docker exec -i "$KAFKA_CONTAINER" kafka-console-producer \
        --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$topic" 2>/dev/null
    echo "  -> $topic"
}

echo "Producing snapshots (event_time=$EVENT_TIME)..."

# BTC-USDT asks — collisions at 100000 (wallex>nobitex) and 100100 (nobitex>bitpin)
emit BTC-USDT asks nobitex '[{"price":"100000","quantity":"0.5"},{"price":"100100","quantity":"1.2"},{"price":"100250","quantity":"0.8"}]'
emit BTC-USDT asks wallex  '[{"price":"100000","quantity":"0.9"},{"price":"100150","quantity":"0.4"},{"price":"100300","quantity":"2.0"}]'
emit BTC-USDT asks bitpin  '[{"price":"100050","quantity":"0.3"},{"price":"100100","quantity":"0.7"},{"price":"100400","quantity":"1.5"}]'

# BTC-USDT bids — collisions at 99950, 99900, 99850
emit BTC-USDT bids nobitex '[{"price":"99950","quantity":"0.6"},{"price":"99900","quantity":"1.0"},{"price":"99800","quantity":"0.5"}]'
emit BTC-USDT bids wallex  '[{"price":"99950","quantity":"0.4"},{"price":"99850","quantity":"1.3"},{"price":"99750","quantity":"0.9"}]'
emit BTC-USDT bids bitpin  '[{"price":"99900","quantity":"0.7"},{"price":"99850","quantity":"0.2"},{"price":"99700","quantity":"2.1"}]'

# TON-USDT asks — collisions at 5.50 (wallex>nobitex) and 5.52 (nobitex>bitpin)
emit TON-USDT asks nobitex '[{"price":"5.50","quantity":"100"},{"price":"5.52","quantity":"250"},{"price":"5.55","quantity":"80"}]'
emit TON-USDT asks wallex  '[{"price":"5.50","quantity":"150"},{"price":"5.53","quantity":"200"},{"price":"5.56","quantity":"300"}]'
emit TON-USDT asks bitpin  '[{"price":"5.51","quantity":"90"},{"price":"5.52","quantity":"120"},{"price":"5.58","quantity":"400"}]'

# TON-USDT bids — collisions at 5.49 (wallex>nobitex) and 5.48 (wallex>bitpin)
emit TON-USDT bids nobitex '[{"price":"5.49","quantity":"120"},{"price":"5.47","quantity":"300"},{"price":"5.45","quantity":"90"}]'
emit TON-USDT bids wallex  '[{"price":"5.49","quantity":"200"},{"price":"5.48","quantity":"150"},{"price":"5.44","quantity":"500"}]'
emit TON-USDT bids bitpin  '[{"price":"5.48","quantity":"80"},{"price":"5.46","quantity":"250"},{"price":"5.43","quantity":"600"}]'

echo "Done. 12 snapshots sent (2 pairs x 2 sides x 3 exchanges)."
echo "Watch merged output:  docker logs taskmanager | grep -E 'BTC-USDT|TON-USDT'"
