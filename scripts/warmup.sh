#!/usr/bin/env bash
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-markets}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"

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
