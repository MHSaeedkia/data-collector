# warmup

Provisions the pipeline before the Flink jobs start:

1. registers every `../schemas/*.avsc` with the schema registry, and
2. makes the Kafka topics for every `exchange_markets` row exist with the
   retention configured in `.env`.

Safe to re-run. Existing topics are kept; only their retention is rewritten when
`.env` no longer matches the broker.

## Run it

```sh
make warmup          # from the repo root
make run             # from here — same thing
```

The first run copies `.env.example` to `.env`. Edit `.env` to change retention,
then run again. A real environment variable beats the file, so a one-off change
needs no edit:

```sh
RETENTION_INPUT_MS=600000 make warmup
```

## Why it is a Go program

It replaces `scripts/warmup.sh`, which ran one `docker exec kafka kafka-topics
--create` — and with it a whole JVM — per topic. At 9 exchanges × 50 markets
that is ~3000 topics and about an hour. Every broker call here carries a batch
of topics instead, so the same work is a handful of requests.

It also talks to `localhost:9092` and `localhost:5432` directly (both compose
files publish them), so it needs no docker CLI and no container names.

## Layout

| Path                 | What it does                                             |
| -------------------- | -------------------------------------------------------- |
| `main.go`            | wires the three steps together                           |
| `internal/config`    | `.env` + environment + defaults                          |
| `internal/domain`    | the two types the steps pass around                      |
| `internal/postgres`  | reads `exchange_markets`                                 |
| `internal/topics`    | builds the topic plan, diffs it against the broker       |
| `internal/kafka`     | creates and retunes topics, in batches                   |
| `internal/registry`  | registers the Avro schemas                               |

## Things worth knowing

- **Lowering a retention deletes data** already past the new limit. The topics
  are retuned, not recreated, so this happens as soon as you re-run.
- The whole `schemas/` directory is registered, subject = file name with dashes
  (`raw_order_book_event.avsc` → `raw-order-book-event`). A hand-written list is
  what once left `control-command` unregistered for two days without any test
  noticing.
- `exchange_markets` is unique on `(exchange_id, market)` — the exchange's own
  symbol string — **not** on `(exchange_id, market_id)`, so two rows can name
  the same exchange and pair. The plan de-duplicates: a name appearing twice in
  one create batch is rejected by the broker.
- The subscription query does **not** filter on `status`, so topics exist for
  unsubscribed rows too. That is what the shell version did, kept on purpose.
