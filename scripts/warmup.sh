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

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AVRO_SCHEMA_FILE="$SCRIPT_DIR/../schemas/orderbook_event.avsc"
JSON_SCHEMA_FILE="$SCRIPT_DIR/../schemas/orderbook_event.json"

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
register_schema "$JSON_SCHEMA_SUBJECT" "JSON" "$JSON_SCHEMA_FILE"

# --- Kafka Topics ---

echo "Fetching active markets from postgres..."

pairs=$(docker exec "$POSTGRES_CONTAINER" psql \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -t -A -F'|' \
    -c "SELECT m.base, m.quote, em.exchange
        FROM exchange_markets em
        JOIN markets m ON em.market_id = m.id
        WHERE em.status = 'subscribe'")

if [ -z "$pairs" ]; then
    echo "No active subscriptions found in exchange_markets."
    exit 0
fi

echo "Creating Kafka topics..."

while IFS='|' read -r base quote exchange; do
    for side in asks bids; do
        topic="${base}-${quote}-${side}-${exchange}"
        echo "  -> $topic"
        docker exec "$KAFKA_CONTAINER" kafka-topics \
            --bootstrap-server "$KAFKA_BOOTSTRAP" \
            --create \
            --if-not-exists \
            --topic "$topic" \
            --partitions 1 \
            --replication-factor 1
    done
done <<< "$pairs"

echo "Done."
