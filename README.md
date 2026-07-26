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
| Kafka            | 9092            | Message broker (KRaft mode)     |
| Schema Registry  | 8082            | Avro schema management          |
| Kafka UI         | 8080            | Kafka web UI                    |
| PostgreSQL       | 5432            | Market metadata store           |

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
│   └── normalizer/            # raw-normalization pipeline (6 chained Flink jobs + common/)
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

# Submit the 6 pipeline jobs downstream-first (aggregator first). See `make refresh-normalizer`.
cd flink/normalizer
./run-job.sh job-aggregator
./run-job.sh job-book-builder
./run-job.sh job-precision
./run-job.sh job-rebaser
./run-job.sh job-type-validator
./run-job.sh job-pair-extractor

cd ../../web
npm i && npm start
```
