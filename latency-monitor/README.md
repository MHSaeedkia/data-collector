# latency-monitor

Tails one Kafka topic and prints, per record, where its time went: how long each
of jobs 1–5 held it, how long it waited in Kafka between them, and how the whole
journey compares against the exchange's own clock and the broker's write time.

Replaces `scripts/watch-topic.sh`. That script could print a record's timestamp
and offset but never its payload — the timings live inside the Avro value, and
decoding Confluent-wire-format Avro in shell was not going to happen.

## Run

```bash
cd latency-monitor
go run . ex1-p1-orderbook-snapshot-flink

# stop the moment an offset does not increase (exit code 2)
go run . --strict-order p1-asks

# or build it
make build && ./latency-monitor ex1-p1-orderbook-snapshot-flink
```

It reads from the END of the topic, like the script did, so it shows what is
arriving now rather than replaying history.

| flag | |
|---|---|
| `<topic>` | required positional argument |
| `--strict-order` | exit 2 on the first non-increasing offset. Without it, an out-of-order offset is a warning on stderr and the tail continues |
| `--env` | path to the .env file, default `.env` |
| `--brokers` | overrides `KAFKA_BOOTSTRAP` |
| `--schema-registry` | overrides `SCHEMA_REGISTRY_URL` |

## Configuration

The two endpoints come from a `.env` file. `make run` creates it from
`.env.example` on the first run and never overwrites an existing one, so edit
`.env`, not `.env.example`.

| setting | default |
|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8082` |

The defaults are the **host-mapped** ports — `docker-compose.yml` and
`docker-compose.prod.yml` both publish 9092 and 8082. Running inside the docker
network instead means `kafka:29092` and `http://schema-registry:8082`.

Precedence, weakest first:

1. the built-in defaults above
2. `.env`
3. a real environment variable
4. a command-line flag

So a one-off run against another broker needs no edit:

```bash
KAFKA_BOOTSTRAP=kafka:29092 go run . p1-asks
go run . --brokers kafka:29092 p1-asks
```

The variable names match `warmup/.env.example`, so one `.env` copies between the
two tools.

Timestamps print in the **host's** timezone. Override with `TZ=`:

```bash
TZ=UTC go run . ex1-p1-orderbook-snapshot-flink
```

## Output

```
──────────────────────────────────────────────────────────────────────────────
ex1-p1-orderbook-snapshot-flink  partition=0  offset=1505638  ex1-p1  (OrderBookSnapshot)

  kafka write         2026-09-14 14:34:33.500 +0330
  exchange event      2026-09-14 14:34:26.146 +0330
  event_time          2026-09-14 14:34:26.146 +0330

  job              in             out                   job       wait
  1 pair-extract   14:34:26.971   14:34:26.972          1ms        n/a
  2 type-validate  14:34:28.569   14:34:28.569           0s     1.597s
  3 rebase         14:34:32.572   14:34:32.572           0s     4.003s
  4 precision      14:34:32.989   14:34:32.989           0s      417ms
  5 book-build     14:34:33.214   14:34:33.214           0s      225ms

  source     exchange → job 1 in          825ms
  pipeline   job 1 in → job 5 out        6.243s
  write      job 5 out → kafka            286ms
  end-to-end exchange → kafka            7.354s
```

- **job** — `out - in`: time inside that job.
- **wait** — this job's `in` minus the previous job's `out`: the delay between
  two adjacent jobs, which is time the record sat in Kafka between them. The
  first job has no predecessor, so `n/a`.
- **source** — the exchange's own clock to job 1's `in`: everything before
  Flink — the exchange's network, NiFi, and the raw topic.
- **pipeline** — job 1 `in` to job 5 `out`.
- **write** — job 5 `out` to the Kafka write time.
- **end-to-end** — the exchange's own clock to the Kafka write time.

The stage rows carry the time of day only; the date and zone are stated once in
the header, because every stamp in one block is within seconds of the others.

## What the numbers mean, and what they do not

**"Kafka write time" is the record's metadata timestamp**, never a field in the
payload. The cluster runs `LogAppendTime`, so it is the broker's append time for
*this* topic.

**`source` and `end-to-end` use `exchange_event_time`, not `event_time`.** Those
are different values on purpose. `event_time` is substituted with job 1's
processing time for the feeds that send no clock — ex3/wallex, ex4/ramzinex, and
ex7/ompfinex's updates — so measuring against it would report ~0 latency for
exactly the feeds you most want to measure. `exchange_event_time` is never
substituted: it is `null` there instead, and both numbers print as **`n/a`**.

**A negative duration is not a bug in this tool.** The stamps come from different
machines. A negative `wait` is clock skew between TaskManagers; a negative
`source` means the exchange's clock is ahead of the server's. Both are shown
rather than clamped, because clamping would turn a clock problem into a latency
mystery.

**`0s` means under half a millisecond**, not instant — every timestamp in the
pipeline is stored at millisecond resolution. Anything genuinely sub-millisecond
prints in microseconds (`450µs`). Past a second, durations always carry three
decimals (`5.600s`, not Go's `5.6s`), so a column of them stays scannable.

## Other topics

It works on any topic in the pipeline, not only job 5's:

- **jobs 1–4 stage topics** — the stages not yet reached are `n/a`, and the
  totals anchor on the stages that are present.
- **`p{id}-{side}` and the merged/adjusted topics** — these carry no
  `pipeline_timings` at all, so the block prints the header and says so. You
  still get the offset check, the write time, and `max_event_time` /
  `min_event_time`.

## Tests

```bash
make check    # fmt + vet + test
```

The decode tests encode against the **real** `../schemas/order_book_snapshot.avsc`
rather than a copy, so a schema change this decoder cannot follow fails here
rather than on the server.
