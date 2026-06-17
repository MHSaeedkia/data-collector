# NiFi Ecosystem Setup

Automated setup for **Apache NiFi** + **Apache Kafka** + **PostgreSQL** on Docker.

---

## File Structure

```
project-folder/
├── docker-compose.yml         # Service definitions
├── setup.sh                   # Setup script
├── postgresql-42.7.11.jar     # JDBC driver (must be next to setup.sh)
└── README.md
```

---

## Prerequisites

- Docker (v20+)
- Docker Compose Plugin (v2+)
- `postgresql-42.7.11.jar` placed next to `setup.sh`

---

## Getting Started

```bash
chmod +x setup.sh
./setup.sh
```

The script performs the following steps in order:

1. Verifies Docker and the JAR file are present
2. Brings up all services with `docker compose up -d`
3. Waits until Postgres is fully ready
4. Creates the `market_status` enum and `markets` table in Postgres
5. Copies the JAR file to `/home/` inside the NiFi container

---

## Services

| Service | Port | Access |
|---|---|---|
| NiFi UI | `8453` | https://localhost:8453/nifi |
| Kafka | `9092` | Inside Docker network only (`kafka:9092`) |
| PostgreSQL | `5432` | localhost:5432 |

### PostgreSQL Connection Details

| Parameter | Value |
|---|---|
| Host | `localhost` |
| Port | `5432` |
| Database | `postgres` |
| User | `postgres` |
| Password | `123` |

### Kafka Connection Details (inside Docker)

| Parameter | Value |
|---|---|
| Bootstrap Server | `kafka:9092` |
| Cluster ID | `1cPr_gg8R0iGWbVpNAefFA` |

---

## Docker Network

All services are connected to the `nifi-net` network and can reach each other by hostname:

- NiFi → Kafka: `kafka:9092`
- NiFi → Postgres: `postgres:5432`

---

## Database Schema

### Enum: `market_status`

```sql
'subscribe' | 'unsubscribe' | 'pending-subscribe' | 'pending-unsubscribe'
```

### Table: `markets`

| Column | Type | Description |
|---|---|---|
| `id` | SERIAL PRIMARY KEY | Auto-increment ID |
| `exchange` | VARCHAR(100) NOT NULL | Exchange name |
| `market` | VARCHAR(100) NOT NULL | Market name |
| `status` | market_status NOT NULL | Status (default: `pending-subscribe`) |

**Constraint:** The combination of `(exchange, market)` must be unique.

---

## JAR in NiFi

After running the setup script, the JDBC driver is available at this path inside the NiFi container:

```
/home/postgresql-42.7.11.jar
```

When configuring a `DBCPConnectionPool` controller service in NiFi, use this path as the **Database Driver Location**.

---

## Volumes

Data is stored in Docker volumes and persists across container restarts:

| Volume | Contents |
|---|---|
| `postgres_data` | PostgreSQL data |
| `nifi_conf` | NiFi configuration |
| `nifi_state` | NiFi internal state |
| `nifi_logs` | NiFi logs |
| `nifi_data` | NiFi data |

---

## Useful Commands

```bash
# Check service status
docker compose ps

# Follow logs for a service
docker compose logs -f nifi
docker compose logs -f kafka
docker compose logs -f postgres

# Stop all services
docker compose down

# Stop and remove all data (volumes)
docker compose down -v

# Connect to Postgres
docker exec -it postgres psql -U postgres
```

---

## Notes

- The script is **idempotent** — running it multiple times will not cause errors.
- Kafka is only accessible from inside the Docker network. To expose it to the host, an `EXTERNAL` listener would need to be configured.
- NiFi may take 1–2 minutes to fully initialize before the UI becomes available.
# Nifi-Ecosystem-Setup
