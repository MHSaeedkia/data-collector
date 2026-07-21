#!/usr/bin/env bash
set -euo pipefail

# Feed one BTC-USDT manual-test scenario to the raw topics and watch the result in the UI.
#
# Every scenario is SELF-CONTAINED: it brings its own baseline snapshot and its own ts
# timeline, and by default this script resets the pipeline's stateful jobs (./reset.sh) before
# producing. So any scenario can be run on its own, in any order, any number of times.
#
# The reset is what makes that true, not the data: all scenarios share pair_id 1, so without
# it the previous run's job state leaks in — most visibly in scenario 01, whose whole point is
# that no baseline exists yet. Use --no-reset only when deliberately chaining.
#
# ts handling: for okx, `ts` is the sequence id AND the event time. Files carry a fixed
# synthetic base so the timeline is readable and diffable, but that base is FUTURE-dated
# relative to real time — sending it verbatim would poison the consolidator's stored
# timestamp (it drops events older than stored) and every later real event would be dropped
# until wall-clock caught up. So each scenario's ts window is shifted onto now, aligned to the
# 300 ms cadence, preserving every delta within the scenario (the +300 steps, the deliberate
# gap, the duplicate, the backwards snapshot).
#
# Usage:
#   ./produce.sh 03-ex8-sequence-gap        # reset, then run this scenario
#   ./produce.sh 01-ex8-update-before-snapshot
#   ./produce.sh --list
#   ./produce.sh 02-ex8-happy-path --no-reset
#   DELAY=1 ./produce.sh 05-ex8-precision-dust
#
# Env overrides: KAFKA_CONTAINER, KAFKA_BOOTSTRAP, DELAY, SETTLE (see reset.sh)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
DELAY="${DELAY:-0}"

command -v jq >/dev/null || { echo "jq is required"; exit 1; }

list_scenarios() {
    echo "Scenarios (each runs standalone):"
    for d in "$SCRIPT_DIR"/[0-9][0-9]-ex*/; do echo "  $(basename "${d%/}")"; done
}

TARGET="${1:-}"
NO_RESET="${2:-}"

case "$TARGET" in
    ""|--list|-h|--help) list_scenarios; exit 0 ;;
    all)
        echo "There is no 'all' mode: scenarios are independent and each needs its own reset."
        echo "Run them one at a time."
        list_scenarios; exit 1 ;;
esac

DIR="$SCRIPT_DIR/$TARGET"
[ -d "$DIR" ] || { echo "no such scenario: $TARGET"; echo; list_scenarios; exit 1; }

EXID="$(basename "$DIR" | sed -n 's/^[0-9]*-ex\([0-9]*\)-.*/\1/p')"
[ -n "$EXID" ] || { echo "cannot derive exchange id from '$TARGET'"; exit 1; }
TOPIC="ex${EXID}-raw"

if [ "$NO_RESET" != "--no-reset" ]; then
    "$SCRIPT_DIR/reset.sh"
else
    echo "==> --no-reset: running against whatever state is already there."
fi

# ex8 carries `ts` (which is ALSO its sequence id, so it aligns to the 300 ms cadence); ex1
# carries `lastUpdate` (event time only — its sequence id is the independent `pub.offset`, left
# untouched, so no cadence alignment). ex3/wallex has no ordering field (job 1 stamps processing
# time). Both future-dated synthetic bases are shifted onto now so the consolidator's stored
# event_time is not poisoned (see README, "Why the script rewrites ts").
DELTA=0
MUT='.'
if [ "$EXID" = "8" ]; then
    MIN_TS="$(jq -r '.data[]?.ts // empty' "$DIR"/*.json | sort -n | head -1)"
    NOW_ALIGNED=$(( ($(date +%s) * 1000 / 300) * 300 ))
    DELTA=$(( NOW_ALIGNED - MIN_TS ))
    MUT='if (.data? | type) == "array" then (.data[].ts) |= ((tonumber + $d) | tostring) else . end'
elif [ "$EXID" = "1" ]; then
    MIN_TS="$(jq -r '(.lastUpdate // .push.pub.data.lastUpdate) // empty' "$DIR"/*.json | sort -n | head -1)"
    DELTA=$(( $(date +%s) * 1000 - MIN_TS ))
    MUT='if .lastUpdate then .lastUpdate += $d elif (.push.pub.data.lastUpdate) then .push.pub.data.lastUpdate += $d else . end'
fi

echo "=== $(basename "$DIR") -> $TOPIC (ts shift: ${DELTA} ms) ==="
for f in "$DIR"/*.json; do
    if [ "$DELTA" -ne 0 ]; then
        LINE="$(jq -c --argjson d "$DELTA" "$MUT" "$f")"
    else
        LINE="$(jq -c . "$f")"
    fi
    echo "  -> $(basename "$f")"
    printf '%s\n' "$LINE" \
        | docker exec -i "$KAFKA_CONTAINER" \
            kafka-console-producer --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$TOPIC" >/dev/null
    [ "$DELAY" != "0" ] && sleep "$DELAY"
done

echo "done."
