---
name: avro-schema-orderbook
description: Avro schema design for normalized order book events (OrderBookEvent, PriceLevelEvent, AggregatedOrderBookEvent) in schema registry
metadata:
    type: project
---

> **Relocated 2026-07-22:** `orderbook_event.*` and `price_level_event.*` moved to
> `schemas/deprecated/` (their only producers/consumers are the deprecated orderbook-job,
> orderbook-consolidator, and job 6 level-emitter). `scripts/warmup.sh` still registers both
> (paths repointed into `deprecated/`). The four active schemas stay in `schemas/`:
> `raw_order_book_event`, `order_book_snapshot`, `rejected_order_book_event`, and
> `aggregated_order_book_event`.
>
> **Renamed 2026-07-22 (Part D):** `consolidated_order_book_event.*` → `aggregated_order_book_event.*`,
> record `ConsolidatedOrderBookEvent`→`AggregatedOrderBookEvent`, inner `ConsolidatedLevel`→`AggregatedLevel`,
> subject `consolidated-order-book-event`→`aggregated-order-book-event`. The wire FIELDS are
> unchanged (`pair_id`, `side`, `event_time`, `levels[exchange_id,price,quantity]`), so the Go web
> decoder — which resolves the schema by Confluent wire-header **id** and maps by field name, never
> by subject/record name — needed only comment/test-schema touch-ups. The producer is now the
> terminal `job-aggregator` (`AggregatedOrderBook`/`AggregatedLevel`/`CrossExchangeAggregator`), not
> the deprecated consolidator. Namespace left as `io.tibobit.orderbook` (changing it is cosmetic and
> would alter the schema fingerprint for zero benefit).

## Schema: OrderBookEvent

File: `schemas/deprecated/orderbook_event.avsc`
Namespace: `io.tibobit.orderbook`
Registered in schema registry; NiFi and Flink both reference it by name.
Example payload: `schemas/deprecated/orderbook_event_example.json`.

## Field summary (in schema order)

| Field           | Avro type                 | Notes                                                         |
| --------------- | ------------------------- | ------------------------------------------------------------- |
| `exchange_id`   | int (required)            | DB `exchanges.id`, e.g. 1 — the exchange identity            |
| `exchange_name` | `["null","string"]`=null  | **Display only, no logic** — e.g. `bitpin`, `nobitex`        |
| `base`          | `["null","string"]`=null  | **Display only, no logic** — base asset, e.g. `BTC`          |
| `quote`         | `["null","string"]`=null  | **Display only, no logic** — quote asset, e.g. `USDT`        |
| `pair_id`       | int (required)            | DB `markets.id`, e.g. 2 — the pair identity, used in topics  |
| `side`          | enum `asks`\|`bids` (req)  | Mirrors topic prefix; included for self-describing messages   |
| `type`          | enum `snapshot`\|`update` (req) | Event type. Flink honours it (Phase 3, 2026-06-29): `snapshot` replaces the exchange book, `update` mutates it — see [[orderbook-aggregation]] |
| `event_time`    | long timestamp-millis (req) | Exchange-reported UTC timestamp in ms                       |
| `sequence_id`   | long (required)            | Monotonic per `(pair_id, exchange_id)` stream; ordering/dedup token — see [[orderbook-aggregation]] |
| `sequence_jump` | long (required)            | Expected delta from the previous message's `sequence_id` (increments are NOT +1 — some exchanges jump ~300). In-order iff `seq == lastSeq + sequence_jump`; anything else is a gap — see [[orderbook-aggregation]] |
| `levels`        | array of PriceLevel (req)  | Price + quantity both as string to preserve decimal precision |

Required vs optional: `exchange_id`, `pair_id`, `side`, `type`, `event_time`, `sequence_id`, `sequence_jump`, `levels` are **required** (non-nullable, no default). The three display-only fields `exchange_name`/`base`/`quote` are **optional** — `["null","string"]` with `default: null` — since no logic depends on them.

## `base`, `quote`, `exchange_name` are display-only — NO logic depends on them

These three fields are **purely human-readable labels with no behavioural value**. Nothing in the
pipeline routes, keys, joins, filters, or branches on them — identity is always `pair_id` and
`exchange_id`. Treat them as carry-along display strings; **never add logic that reads them**. If a
future need to map id → name arises, look it up from postgres (`markets.base_id`/`quote_id` joined to
`currencies`, and `exchanges`; see [[db-schema]]), don't build logic on the event field.

Consequence in code: the Flink job ignores them — `OrderBookEvent` deserializes only `exchange_id`,
`pair_id`, `side`, `type`, `event_time`, `levels` and is `@JsonIgnoreProperties(ignoreUnknown=true)`.
The wire schema still includes them (unchanged) for consumers that want to display them (NiFi/web).
See [[kafka-topic-strategy]] and [[orderbook-aggregation]].

## NiFi responsibility before publishing

NiFi is handled by a separate team and is not implemented in this repo. Documented here for readers to understand the contract this schema depends on.

- Provide `base` and `quote` separately (e.g. raw `BTCUSDT` → `base=BTC`, `quote=USDT`) plus `pair_id` (DB `markets.id`) and `exchange_id`/`exchange_name`
- Split raw exchange message (which contains both sides) into two separate events
- Route each event to the correct input topic: `ex{exchange_id}-p{pair_id}-{side}` (e.g. `ex1-p2-asks`) — see [[kafka-topic-strategy]]
- Kafka message key is null (one exchange per topic already guarantees ordering)
- Populate `sequence_id`/`sequence_jump` per `(pair_id, exchange_id)` stream so Flink can drop stale/duplicate events and detect gaps

**NOTE:** this whole NiFi-normalizes contract is being replaced by [[raw-pipeline-decision]]
(NiFi will publish verbatim raw payloads to `ex{id}-raw`; a new Flink pipeline reproduces the
normalization). Kept until cutover.

## Why price/qty are strings

Exchange APIs return price and quantity as strings to avoid floating-point precision loss. Keeping them as strings in Avro preserves this exactly. Flink converts to `BigDecimal` at processing time.

**Why:** Schema registry contract between NiFi and Flink for the order book pipeline.
**How to apply:** Any new exchange integration must produce events conforming to this schema after NiFi normalization.

## Schema: PriceLevelEvent

File: `schemas/deprecated/price_level_event.avsc` (+ `_example.json`), record `PriceLevelEvent`, namespace
`io.tibobit.orderbook`, registry subject `price-level-event`. The consolidator's **input** wire
shape on `ex{exchange_id}-p{pair_id}-{side}` topics — one flat price level per message:
`exchange_id:int`, `pair_id:int`, `side:enum(asks|bids)`, `event_time:timestamp-millis`,
`price:string`, `quantity:string`. No `type`/`sequence_*`/`levels[]`/display fields — see
[[orderbook-consolidator-decision]] for the semantics.

## Schema: AggregatedOrderBookEvent (was ConsolidatedOrderBookEvent — renamed 2026-07-22)

File: `schemas/aggregated_order_book_event.avsc`, record name `AggregatedOrderBookEvent`,
namespace `io.tibobit.orderbook`. Example payload: `schemas/aggregated_order_book_event_example.json`.
Registered in `scripts/warmup.sh` as subject `aggregated-order-book-event`.

This documents the **output** wire shape produced by the terminal `flink/normalizer/job-aggregator`'s
`AggregatedOrderBook`/`AggregatedLevel` model, published to `p{pair_id}-{side}` and consumed by `web/`.
(The identical shape was previously produced by the now-deprecated `flink/DEPRECATED-orderbook-consolidator/` —
see [[orderbook-consolidator-decision]].)
That output shape is fixed/frozen — do not change it; this schema is a documentation/contract
mirror of it, not a driver of it.

| Field        | Avro type                          | Notes                                              |
| ------------ | ----------------------------------- | --------------------------------------------------- |
| `pair_id`    | int (required)                      | DB `markets.id`                                     |
| `side`       | enum `asks`\|`bids` (required)      | Matches topic suffix                                 |
| `event_time` | long timestamp-millis (required)    | Max `event_time` across contributing exchange books  |
| `levels`     | array of record (required)          | Each: `exchange_id:int`, `price:string`, `quantity:string` — union across exchanges, never summed; equal prices from different exchanges stay as separate adjacent entries |

## Raw-pipeline schemas (added 2026-07-14, M0 of [[raw-pipeline-decision]])

Three schemas for the new raw pipeline, namespace `io.tibobit.orderbook`, each with an
`_example.json`, registered by `scripts/warmup.sh` as canonical fixed-name subjects (no
per-topic subjects). Intended as TRUE Confluent Avro wire format once the jobs exist.

**`raw_order_book_event.avsc`** — record `RawOrderBookEvent`, subject `raw-order-book-event`.
The ONE shared event on all job 1–4 topics (`ex{id}-p{id}-raw-flink` etc. — no side segment;
side split happens in job 6). Fields: `exchange_id:int`, `pair_id:int`,
`type:enum Type(snapshot|update)`, `sequence_id:["null","long"]`, `sequence_jump:long`,
`event_time:timestamp-millis`, `asks`/`bids`: nullable arrays of `PriceLevel{price:string,
quantity:string}`. Design decisions (mine, 2026-07-14 — driven by the captured wire formats
in `sample-raw-data.md`):

- **`asks`/`bids` nullable, default null**: null = "this side is not part of this event" —
  required for ex3 wallex per-SIDE snapshots. An EMPTY array is different: the exchange
  reported that side empty (for a snapshot: clear the side). Never conflate the two.
- **`sequence_id` nullable**: null = the feed has no ordering field at all (only ex3) —
  job 2 must pass such events through unchecked. For everyone else job 1 fills it from the
  per-exchange ordering field (ex1/2/4 `pub.offset`, ex5 `seq`, ex6 `u`, ex8 `ts` as long).
- **`sequence_jump` semantics**: >0 = delta feed, job-2 gap rule `seq == last + jump`
  (ex6=1, ex8=300); **0 = snapshot feed** — no gap rule, only the out-of-order check
  (drop if not strictly greater than last seen). Differs from the old `orderbook_event.avsc`
  where jump was always a real increment.
- **`event_time` required**: exchange-reported ms where available; **ex3 has no timestamp on
  the wire, so job 1 stamps processing time there** (judgment call — flag at job-1 impl).

**`order_book_snapshot.avsc`** — record `OrderBookSnapshot`, subject `order-book-snapshot`.
Job-5 output (full maintained book per (exchange, pair)): `exchange_id`, `pair_id`,
`event_time`, `last_sequence_id:["null","long"]` (null for ex3), required `asks[]`/`bids[]`
of `PriceLevel`.

**`rejected_order_book_event.avsc`** — record `RejectedOrderBookEvent`, subject
`rejected-order-book-event`. Dead-letter envelope: `event:RawOrderBookEvent` (full inline
definition — kept field-for-field identical to `raw_order_book_event.avsc`; update BOTH if
one changes), `reject_reason:string` (human-readable, e.g. "sequence gap: expected X, got Y"),
`rejected_at:timestamp-millis` (job-2 processing time).

`PriceLevel`/`Type` are duplicated across these files with IDENTICAL definitions on purpose
(Avro codegen tolerates identical redefinitions; divergent ones break the build).

## Per-step latency timings (added 2026-07-15, IMPLEMENTED, see [[raw-pipeline-decision]])

Every event carries ONE `pipeline_timings` field — wire type `["null", PipelineTimings]`,
`default: null` (nullable union for a one-token backward-compat default; writers always emit a
non-null record). NOT an array (user rejected the array as ambiguous/query-hostile; the
pipeline is a fixed 6 steps, so name them). `PipelineTimings` has one nullable
`timestamp-millis` field per step×phase, all
`default: null`: `pair_extract_in/out`, `type_validate_in/out`, `rebase_in/out`,
`precision_in/out`, `book_build_in/out`, `level_emit_in/out` (`_in` = read off input topic,
`_out` = written to output — separates in-job compute from Kafka transit). Each job fills ONLY
its own two fields; `null` means "not yet reached this stage". Anchor = existing `event_time`;
`pair_extract_in` doubles as "came from the raw topic", so NO separate top-level ingest field.
Total end-to-end = `level_emit_out − event_time`; other derived deltas (all direct field paths)
in [[raw-pipeline-decision]].

Carriers: `raw_order_book_event` (jobs 1–4), `order_book_snapshot` (job 5), the inlined event
in `rejected_order_book_event` (keep field-for-field identical), and — deliberately —
**`price_level_event.avsc`** (job 6, the otherwise-frozen consolidator input): the added field
is nullable/defaulted, backward-compatible so the consolidator/`web/` keep decoding unchanged
(one nested field, not 12 flat ones). `PipelineTimings` is duplicated field-for-field across
the avsc files, same identical-redefinition rule as `PriceLevel`/`Type`. Adding/removing a
stage later = explicit optional-field schema evolution, not a silent array index shift.
Schema-file + Java-model edits are the implementation step following this doc update.

## Which schemas are real wire encoding vs documentation-only

`price_level_event.avsc` and `consolidated_order_book_event.avsc` are **TRUE Confluent Avro wire
encoding** (magic byte + schema id + Avro payload): the consolidator reads/writes them via the
Confluent registry classes, and `web/` decodes the output via `hamba/avro/v2` + the registry (see
[[orderbook-consolidator-decision]] and [[orderbook-web]]). `orderbook_event.avsc` is still
**documentation-only** — the old `flink/orderbook-job` speaks plain JSON matching it. The one
open risk: NiFi's actual producer format on the consolidator's input topics is unverified
(moot after [[raw-pipeline-decision]] cutover).

[[kafka-topic-strategy]]
