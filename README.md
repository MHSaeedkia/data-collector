# data-collector

A real-time crypto market data pipeline that ingests order book data from multiple exchanges, normalizes it through NiFi, streams it via Kafka, and produces a cleaned order book using Flink.

## Architecture

```
Exchanges (nobitex, bitpin, wallex)
        │
        ▼
    Apache NiFi          ← fetch data, normalize & transform raw market data
        │
        ▼
  Kafka + Schema Registry  ← stream normalized Avro events
        │
        ▼
  Apache Flink           ← generate cleaned order book

```

## Services

| Service          | Port (host)     | Purpose                         |
| ---------------- | --------------- | ------------------------------- |
| NiFi             | 8443 (HTTPS UI) | Data ingestion & normalization  |
| Flink JobManager | 7070            | Stream processing UI & REST API |
| Flink History    | 7071            | Archived jobs from past runs    |
| Kafka            | 9092            | Message broker (KRaft mode)     |
| Schema Registry  | 8082            | Avro schema management          |
| Kafka UI         | 8080            | Kafka web UI                    |
| PostgreSQL       | 5432            | Market metadata store           |
| Prometheus       | 9090            | Scrapes Flink + exporters       |
| Alertmanager     | 9093            | Alert routing (no receiver yet) |
| Grafana          | 3001            | Dashboards (3000 is `web`)      |

## Markets

Tracked pairs across exchanges:

| Exchange | Markets                                                    |
| -------- | ---------------------------------------------------------- |
| nobitex  | BTCUSDT, ETHUSDT, XRPUSDT, BNBUSDT, ARBUSDT, SOLUSDT       |
| bitpin   | BTC_USDT, ETH_USDT, XRP_USDT, BNB_USDT, ARB_USDT, SOL_USDT |
| wallex   | BTCUSDT, ETHUSDT, XRPUSDT, BNBUSDT, ARBUSDT, SOLUSDT       |

## Getting Started

### Prerequisites

- Docker & Docker Compose

### Start the stack

```bash
docker compose -f docker-compose.yml up -d
```

Services start in dependency order. NiFi takes ~90 seconds to become healthy.

### NiFi credentials

```
Username: admin
Password: admin123456789
```

Access the UI at `https://localhost:8443/nifi`

## Market Sync

The `markets/market-sync.sh` script registers or unregisters markets against the control-plane API.

```bash
# Subscribe all markets
./markets/market-sync.sh

# Unsubscribe all markets
./markets/market-sync.sh --reverse
```

Markets are defined in [markets/markets.csv](markets/markets.csv).

## Database

PostgreSQL is initialized with a `markets` database containing two tables:

- **markets** — canonical base/quote pairs (e.g. BTC/USDT)
- **exchange_markets** — per-exchange market instances with subscription state and precision metadata

## Project Structure

```
.
├── docker-compose.yml         # full stack; Flink cluster builds flink/normalizer
├── flink/
│   ├── run-job.sh             # builds + submits any job below; run with no arg to list them
│   ├── run-local.sh           # runs one job in-process instead — no Flink cluster at all
│   ├── normalizer/            # raw-normalization pipeline (6 chained Flink jobs + common/)
│   └── merger/                # sums the aggregated book's levels per price (p{id}-{side}-merged)
├── monitoring/                # Prometheus scrape config + alert rules, Alertmanager, Grafana
├── nifi/
│   └── Dockerfile              # NiFi + PostgreSQL JDBC driver
├── postgres/
│   └── init.sql                # Database schema
└── markets/
    ├── markets.csv             # Exchange/market subscription list
    └── market-sync.sh          # Subscription management script
```

# Run Flink

```
./scripts/warmup.sh

# One script builds and submits any job, from any project under flink/.
# Run it with no argument to list them. Order matters: downstream-first, because
# every source reads from `latest`. See `make run-all-jobs`.
cd flink
./run-job.sh merger
./run-job.sh job-aggregator
./run-job.sh job-book-builder
./run-job.sh job-precision
./run-job.sh job-rebaser
./run-job.sh job-type-validator
./run-job.sh job-pair-extractor

# Or run a single job on your own machine, with no Flink cluster and no Flink image:
# an in-process MiniCluster, pointed at the stack's published ports. Ctrl-C stops it.
# Cancel the cluster's copy first (../scripts/cancel-flink-jobs.sh) or both write the
# same downstream topics.
./run-local.sh job-rebaser

cd ../web
npm i && npm start
```
