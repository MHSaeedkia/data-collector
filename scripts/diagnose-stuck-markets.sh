#!/usr/bin/env bash
# Finds markets whose stream went untrustworthy and never recovered.
#
# The fingerprint of the deadlock fixed on 2026-08-19: a burst of `sequence_gap` on a
# market's dead-letter topic, followed by unbroken `awaiting_snapshot`, with NO matching
# snapshot_request on `control-plane`. Job 2 asks once per episode, so a market that is
# still rejecting long after its single request never got a usable snapshot back.
#
# Read-only. Safe to run against a live stack.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:29092}"
REGISTRY="${REGISTRY:-http://schema-registry:8082}"
WINDOW_MS="${WINDOW_MS:-600000}"   # how far back to read, default 10 min

kexec() { docker exec -i schema-registry "$@"; }

echo "== snapshot_request commands on control-plane =="
kexec kafka-avro-console-consumer \
  --bootstrap-server "$BOOTSTRAP" \
  --property schema.registry.url="$REGISTRY" \
  --topic control-plane --from-beginning \
  --timeout-ms 15000 2>/dev/null \
  | grep '^{' \
  | jq -r '"\(.exchange_id)|\(.pair_id)\t\(.action)\tsim=\(.simulation)"' \
  | sort | uniq -c | sort -rn || echo "  (none)"

echo
echo "== rejection reasons per market (dead-letter topics) =="
topics=$(docker exec kafka kafka-topics --bootstrap-server "$BOOTSTRAP" --list \
         | grep -E '^ex[0-9]+-p[0-9]+-rejected-flink$' || true)

if [ -z "$topics" ]; then echo "  (no dead-letter topics)"; exit 0; fi

for t in $topics; do
  market=$(echo "$t" | sed -E 's/^ex([0-9]+)-p([0-9]+)-rejected-flink$/\1|\2/')
  summary=$(kexec kafka-avro-console-consumer \
      --bootstrap-server "$BOOTSTRAP" \
      --property schema.registry.url="$REGISTRY" \
      --topic "$t" --from-beginning --timeout-ms 8000 2>/dev/null \
      | grep '^{' | jq -r '.reject_reason' | sort | uniq -c | sort -rn || true)
  [ -z "$summary" ] && continue

  # The tell: still holding updates, i.e. the last reason seen is awaiting_snapshot.
  last=$(kexec kafka-avro-console-consumer \
      --bootstrap-server "$BOOTSTRAP" \
      --property schema.registry.url="$REGISTRY" \
      --topic "$t" --from-beginning --timeout-ms 8000 2>/dev/null \
      | grep '^{' | jq -r '.reject_reason' | tail -1 || true)

  flag=""
  [ "$last" = "awaiting_snapshot" ] && flag="   <-- STUCK: still awaiting a snapshot"
  [ "$last" = "no_baseline" ] && flag="   <-- STUCK: never got a baseline"

  echo "market $market$flag"
  echo "$summary" | sed 's/^/    /'
done
