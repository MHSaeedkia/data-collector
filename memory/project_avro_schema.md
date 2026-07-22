---
name: avro-schema-orderbook
description: Avro schema design for the normalizer pipeline (RawOrderBookEvent, OrderBookSnapshot, RejectedOrderBookEvent, AggregatedOrderBookEvent) in schema registry
metadata:
    type: project
---

# Avro schemas — normalizer pipeline

Four active schemas live in `schemas/` and are registered by `scripts/warmup.sh` as canonical
fixed-name subjects (no per-topic subjects), namespace `io.tibobit.orderbook`, each with an
`_example.json`. All are TRUE Confluent Avro wire format (magic byte + schema-registry id + Avro
payload):

- `raw_order_book_event.avsc` — subject `raw-order-book-event` (jobs 1–5 intermediate stream)
- `order_book_snapshot.avsc` — subject `order-book-snapshot` (job 5 full-book output)
- `rejected_order_book_event.avsc` — subject `rejected-order-book-event` (job 2 dead-letter)
- `aggregated_order_book_event.avsc` — subject `aggregated-order-book-event` (aggregator → web)

## Why price/qty are strings

Exchange APIs return price and quantity as strings to avoid floating-point precision loss. Keeping
them as strings in Avro preserves this exactly; Flink converts to `BigDecimal` at processing time
([[bigdecimal-rules]]). Any new exchange integration must produce events conforming to these schemas.

## Raw-pipeline schemas (M0 of [[raw-pipeline-decision]])

**`raw_order_book_event.avsc`** — record `RawOrderBookEvent`, subject `raw-order-book-event`.
The ONE shared event on all job 1–4 topics (`ex{id}-p{id}-raw-flink` etc. — no side segment; both
sides ride in one event, the per-side split happens in the aggregator's `SnapshotSplitter`). Fields:
`exchange_id:int`, `pair_id:int`, `type:enum Type(snapshot|update|reset)`,
`sequence_id:["null","long"]`, `sequence_jump:long`, `event_time:timestamp-millis`, `asks`/`bids`:
nullable arrays of `PriceLevel{price:string, quantity:string}`. Design decisions (driven by the
captured wire formats in `sample-raw-data.md`):

- **`asks`/`bids` nullable, default null**: null = "this side is not part of this event" — required
  for ex3 wallex per-SIDE snapshots. An EMPTY array is different: the exchange reported that side
  empty (for a snapshot: clear the side). Never conflate the two.
- **`sequence_id` nullable**: null = the feed has no ordering field at all (only ex3) — job 2 must
  pass such events through unchecked. For everyone else job 1 fills it from the per-exchange ordering
  field (ex1/2/4 `pub.offset`, ex5 `seq`, ex6 `u`, ex8 `ts` as long).
- **`sequence_jump` semantics**: >0 = delta feed, job-2 gap rule `seq == last + jump` (ex6=1,
  ex8=300); **0 = snapshot feed** — no gap rule, only the out-of-order check (drop if not strictly
  greater than last seen).
- **`type=reset`** is a synthetic marker job 2 emits on a true gap ([[type-validator]]); job 5 turns
  it into an emptied book so the [[aggregator]] drops that exchange from the union. `Type` is an Avro
  **enum**, so `reset` had to be added as a symbol (v2/id 7) or the serializer NPEs.
- **`event_time` required**: exchange-reported ms where available; **ex3 has no timestamp on the
  wire, so job 1 stamps processing time there**.

**`order_book_snapshot.avsc`** — record `OrderBookSnapshot`, subject `order-book-snapshot`. Job-5
output (full maintained book per (exchange, pair)): `exchange_id`, `pair_id`, `event_time`,
`last_sequence_id:["null","long"]` (null for ex3), required `asks[]`/`bids[]` of `PriceLevel`.

**`rejected_order_book_event.avsc`** — record `RejectedOrderBookEvent`, subject
`rejected-order-book-event`. Dead-letter envelope: `event:RawOrderBookEvent` (full inline definition —
kept field-for-field identical to `raw_order_book_event.avsc`; update BOTH if one changes),
`reject_reason:string`, `rejected_at:timestamp-millis` (job-2 processing time).

`PriceLevel`/`Type` are duplicated across these files with IDENTICAL definitions on purpose (Avro
codegen tolerates identical redefinitions; divergent ones break the build).

## Schema: AggregatedOrderBookEvent

File: `schemas/aggregated_order_book_event.avsc`, record `AggregatedOrderBookEvent`, subject
`aggregated-order-book-event`. The **output** wire shape produced by the terminal
`flink/normalizer/job-aggregator`'s `AggregatedOrderBook`/`AggregatedLevel` model, published to
`p{pair_id}-{side}` and consumed by `web/`. This shape is **frozen** — do not change it; the schema
is a documentation/contract mirror of the code, not a driver of it. The Go web decoder resolves the
schema by Confluent wire-header **id** and maps by field name (never by subject/record name).

| Field        | Avro type                          | Notes                                              |
| ------------ | ----------------------------------- | --------------------------------------------------- |
| `pair_id`    | int (required)                      | DB `markets.id`                                     |
| `side`       | enum `asks`\|`bids` (required)      | Matches topic suffix                                 |
| `event_time` | long timestamp-millis (required)    | Max `event_time` across contributing exchange books  |
| `levels`     | array of record (required)          | Each: `exchange_id:int`, `price:string`, `quantity:string` — union across exchanges, never summed; equal prices from different exchanges stay as separate adjacent entries |

## Per-step latency timings (see [[raw-pipeline-decision]])

Every raw-pipeline event carries ONE `pipeline_timings` field — wire type `["null", PipelineTimings]`,
`default: null` (writers always emit a non-null record). NOT an array (the pipeline is a fixed set of
steps, so name them). `PipelineTimings` has one nullable `timestamp-millis` field per step×phase, all
`default: null`: `pair_extract_in/out`, `type_validate_in/out`, `rebase_in/out`, `precision_in/out`,
`book_build_in/out` (`_in` = read off input topic, `_out` = written to output — separates in-job
compute from Kafka transit). Each job fills ONLY its own two fields; `null` means "not yet reached
this stage". Anchor = existing `event_time`; `pair_extract_in` doubles as "came from the raw topic".
Total end-to-end = `book_build_out − event_time`. The aggregated web output carries NO
`pipeline_timings` (the frozen web shape drops it). Carriers: `raw_order_book_event` (jobs 1–4),
`order_book_snapshot` (job 5), and the inlined event in `rejected_order_book_event` (keep
field-for-field identical). `PipelineTimings` is duplicated field-for-field across the avsc files,
same identical-redefinition rule as `PriceLevel`/`Type`.

[[kafka-topic-strategy]]
