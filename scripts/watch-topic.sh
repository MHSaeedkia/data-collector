#!/usr/bin/env bash
#
# Live-tails one topic partition, printing the record timestamp, partition and offset per record, in the
# order the broker returns them. Use it instead of Kafka UI's live mode when the order matters.
# Values are hidden because they are Avro binary.
#
#   ./scripts/watch-topic.sh p1-asks
#
# Output: 2026-09-14 14:25:02.006 +0330  partition=0  offset=5  create_ms=1789383302006
# The date is converted on the host, so it is in the server's timezone (override with TZ=...),
# not the container's.
#
# Offsets within one partition always increase. If one does not (equal to or below the previous
# offset), the script prints both offsets, stops the consumer and exits 2. A jump forward is not
# out of order and does not stop it. The timestamp is not checked: under CreateTime Flink carries
# the input record's timestamp through, so it is not monotonic (the broker is LogAppendTime now).
#
# Needs bash >= 4.2 (printf's %(...)T formats without forking `date` per record).
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"

if [[ $# -ne 1 || -z "$1" ]]; then
    echo "usage: $0 <topic>" >&2
    exit 1
fi
TOPIC="$1"

# No `docker exec -t`: a TTY puts the terminal in raw mode, which staircases every line and echoes
# ^C into the data. Without one, killing the docker client does NOT stop the process inside the
# container, so the consumer is tied to its stdin instead: when stdin closes, a watcher kills it.
# The coproc's stdin is a pipe this script holds, so it closes however the script ends (Ctrl-C,
# exit 2, kill). The watcher's output goes to /dev/null so it cannot keep the exec stream open
# after the consumer ends on its own.
coproc CONSUMER {
    docker exec -i "$KAFKA_CONTAINER" sh -c '
        exec 3<&0
        kafka-console-consumer "$@" 2>&1 &
        c=$!
        # <&3 is required: sh points a background job'"'"'s stdin at /dev/null otherwise.
        (cat <&3 >/dev/null; kill "$c") >/dev/null 2>&1 &
        wait "$c"' sh \
        --bootstrap-server "$KAFKA_BOOTSTRAP" \
        --topic "$TOPIC" --partition 0 --offset latest \
        --property print.partition=true \
        --property print.offset=true \
        --property print.timestamp=true \
        --property print.value=false
}
consumer_pid=$CONSUMER_PID
# Duplicate the read end now: bash unsets CONSUMER as soon as the coproc exits.
exec 3<&"${CONSUMER[0]}"

prev=""
while IFS=$'\t' read -r ts partition offset _; do
    ms="${ts#*Time:}" # CreateTime:<ms> or LogAppendTime:<ms>, depending on the topic's timestamp type
    n="${offset#Offset:}"
    if [[ "$ms" =~ ^[0-9]+$ && "$n" =~ ^[0-9]+$ ]]; then
        printf -v when '%(%Y-%m-%d %H:%M:%S)T' "$((ms / 1000))"
        printf -v zone '%(%z)T' "$((ms / 1000))"
        printf '%s.%03d %s  partition=%s  offset=%s  create_ms=%s\n' \
            "$when" "$((ms % 1000))" "$zone" "${partition#Partition:}" "$n" "$ms"
    else
        # Consumer messages, errors, or a record with no timestamp: pass through untouched.
        printf '%s\n' "$ts${partition:+  $partition}${offset:+  $offset}"
    fi

    [[ "$n" =~ ^[0-9]+$ ]] || continue
    if [[ -n "$prev" ]] && (( n <= prev )); then
        echo "ERROR: offset out of order on $TOPIC: $n after $prev" >&2
        kill "$consumer_pid" 2>/dev/null || true
        exit 2
    fi
    prev="$n"
done <&3

# The consumer ended on its own (container gone, bad topic, ...): exit with its status.
wait "$consumer_pid"
