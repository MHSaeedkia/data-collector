#!/bin/bash

# ─────────────────────────────────────────────
# Config
BASE_URL="http://localhost:8081/control-plane"
MARKETS_FILE="$(dirname "${BASH_SOURCE[0]}")/markets.csv"
MAX_RETRIES=3
RETRY_DELAY=2  # seconds between retries
REVERSE=false

# ─────────────────────────────────────────────
# Parse flags
for arg in "$@"; do
  case $arg in
    --reverse|-r) REVERSE=true ;;
    *) echo -e "${RED}[ERROR]${NC} Unknown flag: $arg"; exit 1 ;;
  esac
done

# ─────────────────────────────────────────────
# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

ok()   { echo -e "${GREEN}[OK]${NC}     $1"; }
fail() { echo -e "${RED}[FAIL]${NC}   $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC}   $1"; }
log()  { echo -e "${BLUE}[INFO]${NC}   $1"; }

# ─────────────────────────────────────────────
# Validate
if [ ! -f "$MARKETS_FILE" ]; then
  echo -e "${RED}[ERROR]${NC} markets.csv not found at: $MARKETS_FILE"
  exit 1
fi

# ─────────────────────────────────────────────
# Counters
SUCCESS=0
FAILED=0
TOTAL=0

if [ "$REVERSE" = true ]; then
  log "Starting market sync from: $MARKETS_FILE  [REVERSE MODE]"
else
  log "Starting market sync from: $MARKETS_FILE"
fi
echo "────────────────────────────────────────────"

# ─────────────────────────────────────────────
# Process each line (skip header)
while IFS=',' read -r exchange market action; do
  # Skip header and empty lines
  [[ "$exchange" == "exchange" ]] && continue
  [[ -z "$exchange" || -z "$market" || -z "$action" ]] && continue

  # Reverse action if flag is set
  if [ "$REVERSE" = true ]; then
    if [ "$action" = "subscribe" ]; then
      action="unsubscribe"
    else
      action="subscribe"
    fi
  fi

  TOTAL=$((TOTAL + 1))
  ENDPOINT="$BASE_URL/$action"
  PAYLOAD="{\"exchange\": \"$exchange\", \"market\": \"$market\"}"
  ATTEMPT=0
  SENT=false

  while [ $ATTEMPT -lt $MAX_RETRIES ]; do
    ATTEMPT=$((ATTEMPT + 1))

    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
      --location "$ENDPOINT" \
      --header 'Content-Type: application/json' \
      --data "$PAYLOAD" \
      --max-time 10)

    if [[ "$HTTP_CODE" =~ ^2 ]]; then
      ok "[$action] $exchange / $market  (HTTP $HTTP_CODE)"
      SUCCESS=$((SUCCESS + 1))
      SENT=true
      break
    else
      if [ $ATTEMPT -lt $MAX_RETRIES ]; then
        warn "[$action] $exchange / $market  → HTTP $HTTP_CODE — retrying ($ATTEMPT/$MAX_RETRIES)..."
        sleep $RETRY_DELAY
      fi
    fi
  done

  if [ "$SENT" = false ]; then
    fail "[$action] $exchange / $market  → failed after $MAX_RETRIES attempts (last HTTP: $HTTP_CODE)"
    FAILED=$((FAILED + 1))
  fi

done < "$MARKETS_FILE"

# ─────────────────────────────────────────────
# Summary
echo "────────────────────────────────────────────"
echo -e "  Total:   $TOTAL"
echo -e "  ${GREEN}Success: $SUCCESS${NC}"
if [ $FAILED -gt 0 ]; then
  echo -e "  ${RED}Failed:  $FAILED${NC}"
else
  echo -e "  Failed:  $FAILED"
fi
echo "────────────────────────────────────────────"