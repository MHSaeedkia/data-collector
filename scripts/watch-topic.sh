#!/usr/bin/env bash
#
# Live-tails one topic partition, printing partition, offset and CreateTime per record,
# in the order the broker returns them. Use it instead of Kafka UI's live mode when the
# order matters. Values are hidden because they are Avro binary.
#
#   ./scripts/watch-topic.sh p1-asks
#
# Output: 2026-09-14 14:25:02.006 +0330  Partition:0  Offset:5  CreateTime:1789383302006
# The date is converted on the host, so it is in the server's timezone (override with TZ=...),
# not the container's.
#
# Offsets within one partition always increase. If one does not (equal to or below the previous
# offset), the script prints both offsets, stops the consumer and exits 2. A jump forward is not
# out of order and does not stop it. CreateTime is not checked: Flink carries the input record's
# timestamp through instead of stamping the write time, so it is not monotonic.
#
# Needs bash >= 4.4 (printf's %(...)T formats without forking `date` per record; `wait` on a
# process substitution returns the consumer's exit status).
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"

if [[ $# -ne 1 || -z "$1" ]]; then
    echo "usage: $0 <topic>" >&2
    exit 1
fi
TOPIC="$1"

# -t only when run from a terminal, so Ctrl-C reaches the consumer inside the container.
TTY=()
[[ -t 0 ]] && TTY=(-t)

exec 3< <(docker exec -i ${TTY[@]+"${TTY[@]}"} "$KAFKA_CONTAINER" kafka-console-consumer \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --topic "$TOPIC" --partition 0 --offset latest \
    --property print.partition=true \
    --property print.offset=true \
    --property print.timestamp=true \
    --property print.value=false)
consumer=$!

prev=""
while IFS=$'\t' read -r ts partition offset _; do
    # A TTY ends lines with \r\n.
    ts="${ts%$'\r'}" partition="${partition%$'\r'}" offset="${offset%$'\r'}"
    ms="${ts#CreateTime:}"
    if [[ "$ms" =~ ^[0-9]+$ ]]; then
        printf -v when '%(%Y-%m-%d %H:%M:%S)T' "$((ms / 1000))"
        printf -v zone '%(%z)T' "$((ms / 1000))"
        printf '%s.%03d %s\t%s\t%s\t%s\n' "$when" "$((ms % 1000))" "$zone" "$partition" "$offset" "$ts"
    else
        # Consumer messages, errors, or a record with no timestamp: pass through untouched.
        printf '%s\n' "$ts${partition:+$'\t'$partition}${offset:+$'\t'$offset}"
    fi

    n="${offset#Offset:}"
    [[ "$n" =~ ^[0-9]+$ ]] || continue
    if [[ -n "$prev" ]] && (( n <= prev )); then
        echo "ERROR: offset out of order on $TOPIC: $n after $prev" >&2
        kill "$consumer" 2>/dev/null || true
        exit 2
    fi
    prev="$n"
done <&3

# The consumer ended on its own (container gone, bad topic, ...): exit with its status.
wait "$consumer"
