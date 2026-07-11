#!/usr/bin/env bash
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-markets}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8082}"
AVRO_SCHEMA_SUBJECT="${AVRO_SCHEMA_SUBJECT:-orderbook-event}"
JSON_SCHEMA_SUBJECT="${JSON_SCHEMA_SUBJECT:-orderbook-event-json}"
PRICE_LEVEL_SCHEMA_SUBJECT="${PRICE_LEVEL_SCHEMA_SUBJECT:-price-level-event}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AVRO_SCHEMA_FILE="$SCRIPT_DIR/../schemas/orderbook_event.avsc"
PRICE_LEVEL_SCHEMA_FILE="$SCRIPT_DIR/../schemas/price_level_event.avsc"

command -v jq >/dev/null 2>&1 || { echo "jq is required but not installed."; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "curl is required but not installed."; exit 1; }

register_schema() {
    local subject="$1"
    local schema_type="$2"
    local file="$3"

    echo "Registering $schema_type schema (subject: $subject)..."

    payload=$(jq -n --arg schema "$(cat "$file")" --arg type "$schema_type" \
        '{"schemaType": $type, "schema": $schema}')

    tmpfile=$(mktemp)
    http_code=$(curl -s -o "$tmpfile" -w "%{http_code}" \
        -X POST "$SCHEMA_REGISTRY_URL/subjects/$subject/versions" \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "$payload")
    body=$(cat "$tmpfile")
    rm -f "$tmpfile"

    if [ "$http_code" != "200" ]; then
        echo "  -> Failed (HTTP $http_code): $body"
        exit 1
    fi

    schema_id=$(echo "$body" | jq -r '.id')
    echo "  -> Registered (id: $schema_id)"
}

# --- Schema Registry ---

register_schema "$AVRO_SCHEMA_SUBJECT" "AVRO" "$AVRO_SCHEMA_FILE"
register_schema "$PRICE_LEVEL_SCHEMA_SUBJECT" "AVRO" "$PRICE_LEVEL_SCHEMA_FILE"

# --- Kafka Topics ---

echo "Fetching active markets from postgres..."

pairs=$(docker exec "$POSTGRES_CONTAINER" psql \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -t -A -F'|' \
    -c "SELECT m.id, em.exchange_id, e.name as exchange_name
        FROM exchange_markets em
        JOIN markets m ON em.market_id = m.id
        JOIN exchanges e ON e.id = em.exchange_id")

if [ -z "$pairs" ]; then
    echo "No active subscriptions found in exchange_markets."
    exit 0
fi

echo "Creating Kafka topics..."

INPUT_RETENTION_MS=3600000    # 1 hour
OUTPUT_RETENTION_MS=21600000  # 6 hours
PARALLEL_JOBS="${PARALLEL_JOBS:-20}"

create_topic() {
    local topic="$1"
    local retention_ms="$2"
    docker exec "$KAFKA_CONTAINER" kafka-topics \
        --bootstrap-server "$KAFKA_BOOTSTRAP" \
        --create \
        --if-not-exists \
        --topic "$topic" \
        --partitions 1 \
        --replication-factor 1 \
        --config "retention.ms=$retention_ms" \
        && echo "  -> $topic (retention: ${retention_ms}ms)"
}
export -f create_topic
export KAFKA_CONTAINER KAFKA_BOOTSTRAP

# Each `kafka-topics --create` call starts a fresh JVM inside the container, so
# creating topics one at a time is dominated by JVM startup, not Kafka itself.
# Build the full topic list up front and fan the calls out with xargs -P so
# many JVMs start concurrently instead of sequentially.
distinct_pairs=$(echo "$pairs" | cut -d'|' -f1 | sort -u)
{
    # Input topics — one per side+pair+exchange (NiFi produces, Flink source consumes).
    while IFS='|' read -r pair_id exchange_id exchange_name; do
        for side in asks bids; do
            echo "${side}-p${pair_id}-ex${exchange_id} $INPUT_RETENTION_MS"
        done
    done <<< "$pairs"

    # Output topics — one per side+pair (Flink aggregation writes the consolidated book here).
    while IFS='|' read -r pair_id; do
        for side in asks bids; do
            echo "${side}-p${pair_id} $OUTPUT_RETENTION_MS"
        done
    done <<< "$distinct_pairs"
} | xargs -P "$PARALLEL_JOBS" -n2 bash -c 'create_topic "$1" "$2"' _

echo "Done."
