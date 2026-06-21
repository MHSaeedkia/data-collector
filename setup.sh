#!/bin/bash

set -e

# ─────────────────────────────────────────────
# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log()    { echo -e "${BLUE}[INFO]${NC}  $1"; }
ok()     { echo -e "${GREEN}[OK]${NC}    $1"; }
warn()   { echo -e "${YELLOW}[WARN]${NC}  $1"; }
error()  { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

# ─────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_FILE="$SCRIPT_DIR/postgresql-42.7.11.jar"

# ─────────────────────────────────────────────
log "Checking requirements..."

command -v docker >/dev/null 2>&1 || error "Docker is not installed."
docker compose version >/dev/null 2>&1 || error "Docker Compose plugin not found."

if [ ! -f "$JAR_FILE" ]; then
  error "postgresql-42.7.11.jar not found next to setup.sh (expected: $JAR_FILE)"
fi

ok "All requirements met."

# ─────────────────────────────────────────────
log "Starting all services with Docker Compose..."
docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d

ok "Containers started."

# ─────────────────────────────────────────────
log "Waiting for Postgres to be healthy..."
RETRIES=30
until docker exec postgres pg_isready -U postgres >/dev/null 2>&1; do
  RETRIES=$((RETRIES - 1))
  if [ "$RETRIES" -le 0 ]; then
    error "Postgres did not become healthy in time."
  fi
  echo -n "."
  sleep 2
done
echo ""
ok "Postgres is ready."

# ─────────────────────────────────────────────
log "Running migration.sql in Postgres..."

if docker exec -i postgres psql -U postgres -d postgres < migration.sql; then
  ok "Migration executed successfully."
else
  fail "Migration failed!"
  exit 1
fi

# ─────────────────────────────────────────────
log "Waiting for NiFi container to be running..."
RETRIES=20
until [ "$(docker inspect -f '{{.State.Running}}' nifi-ecosystem 2>/dev/null)" = "true" ]; do
  RETRIES=$((RETRIES - 1))
  if [ "$RETRIES" -le 0 ]; then
    error "NiFi container did not start in time."
  fi
  echo -n "."
  sleep 3
done
echo ""
ok "NiFi container is running."

# ─────────────────────────────────────────────
log "Copying postgresql JAR into NiFi at /home/..."
docker cp "$JAR_FILE" nifi-ecosystem:/home/postgresql-42.7.11.jar
ok "JAR copied to nifi-ecosystem:/home/postgresql-42.7.11.jar"

# ─────────────────────────────────────────────
echo ""
echo -e "${GREEN}════════════════════════════════════════${NC}"
echo -e "${GREEN}  Setup completed successfully!${NC}"
echo -e "${GREEN}════════════════════════════════════════${NC}"
echo ""
echo -e "  ${BLUE}NiFi UI${NC}      →  https://localhost:8453/nifi"
echo -e "  ${BLUE}Kafka${NC}         →  kafka:9092  (inside docker network)"
echo -e "  ${BLUE}Postgres${NC}      →  localhost:5432  (user: postgres / pass: 123)"
echo -e "  ${BLUE}JAR in NiFi${NC}  →  /home/postgresql-42.7.11.jar"
echo ""
