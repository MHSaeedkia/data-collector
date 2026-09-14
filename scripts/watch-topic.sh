#!/usr/bin/env bash
#
# Live-tails one topic partition, printing partition, offset and CreateTime per record,
# in the order the broker returns them. Use it instead of Kafka UI's live mode when the
# order matters. Values are hidden because they are Avro binary.
#
#   ./scripts/watch-topic.sh p1-asks
#
# Offsets within one partition always increase by one. CreateTime does not have to: Flink
# carries the input record's timestamp through instead of stamping the write time.
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"

if [[ $# -ne 1 || -z "$1" ]]; then
    echo "usage: $0 <topic>" >&2
    exit 1
fi
TOPIC="$1"

# -t only on a terminal, so Ctrl-C reaches the consumer; without one (piped output) -t fails.
TTY=()
[[ -t 1 ]] && TTY=(-t)

docker exec -i ${TTY[@]+"${TTY[@]}"} "$KAFKA_CONTAINER" kafka-console-consumer \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --topic "$TOPIC" --partition 0 --offset latest \
    --property print.partition=true \
    --property print.offset=true \
    --property print.timestamp=true \
    --property print.value=false
