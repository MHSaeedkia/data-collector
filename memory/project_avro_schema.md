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

⚠ **The `_example.json` files are hand-maintained docs and go stale silently** — nothing reads them,
no test validates them. Verified field-for-field and in schema order against all four avsc on
2026-08-08 (the snapshot example had missed the per-level `source_id`). They are written in a
HUMAN-readable shape, not goavro's textual encoding: ISO-8601 timestamps and plain nested records,
with no union-branch wrappers. Keep that convention — and update the example in the same commit as
the schema.

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
  field (ex1/2/4 `pub.offset`, ex6 `u`, ex8 + **ex5 (since 2026-08-22)** `ts` as long).
- **`sequence_jump` semantics**: >0 = delta feed, job-2 gap rule `seq == last + jump` (ex6=1,
  ex8=300, **ex5=600 since 2026-08-22**); **0 = snapshot feed** — no gap rule, only the
  out-of-order check (drop if not strictly greater than last seen).
- **`sequence_jump_tolerance` (added 2026-08-22, `default: 0` so it is a BACKWARD-compatible
  evolution)**: half-width of the accepted window, making the real rule
  `last + jump - tol <= seq <= last + jump + tol`. At 0 — every exchange but ex5 — it collapses to
  the exact check that was there before, so nothing else changed behaviour. **ex5 bitget stamps 10**
  because its sequence is a millisecond CLOCK on a nominal 600 ms cadence, not a counter, so it
  never lands on an exact multiple. Lives on `raw_order_book_event` and its nested copy inside
  `rejected_order_book_event`; it does NOT flow to `order_book_snapshot` or the aggregated topics,
  and the rejected SERIALIZER needed no change because it delegates to the raw one. ⚠ re-register
  both subjects — same trap as `pipeline_timings` / `simulation` / `reason`.
- **`type=reset`** is a synthetic marker job 2 emits on a true gap ([[type-validator]]); job 5 turns
  it into an emptied book so the [[aggregator]] drops that exchange from the union. `Type` is an Avro
  **enum**, so `reset` had to be added as a symbol (v2/id 7) or the serializer NPEs.
- **`event_time` required**: exchange-reported ms where available; **ex3 has no timestamp on the
  wire, so job 1 stamps processing time there**.

**`order_book_snapshot.avsc`** — record `OrderBookSnapshot`, subject `order-book-snapshot`. Job-5
output (full maintained book per (exchange, pair)): `exchange_id`, `pair_id`, `event_time`,
`last_sequence_id:["null","long"]` (null for ex3), required `asks[]`/`bids[]` of `PriceLevel` —
**the one `PriceLevel` that is NOT identical to the others: it alone carries `source_id`** (2026-08-08,
see [[record-lineage]]).

**`rejected_order_book_event.avsc`** — record `RejectedOrderBookEvent`, subject
`rejected-order-book-event`. Dead-letter envelope: `event:RawOrderBookEvent` (full inline definition —
kept field-for-field identical to `raw_order_book_event.avsc`; update BOTH if one changes),
`reject_reason:string`, `rejected_at:timestamp-millis` (job-2 processing time).

`PriceLevel`/`Type` are duplicated across these files. `Type`, and `PriceLevel` in the raw and
rejected schemas, are identical on purpose — **within one file** Avro tolerates an identical
redefinition and breaks on a divergent one, which is what the rule protects (`rejected_…` inlines
the raw event, so those two must match field-for-field).

**Divergence ACROSS files is fine and is now real**: `order_book_snapshot.avsc`'s `PriceLevel` has a
third field, `source_id`. Nothing parses these files together — there is no avro-maven-plugin
anywhere, each avsc is loaded on its own at runtime and read through `GenericRecord` — so the old
blanket "all identical" rule was stronger than the constraint actually is. Do not "restore
consistency" by adding the field to the other two: on a raw or rejected event every level belongs to
the event carrying it, so it would be permanently blank. `serde/PriceLevels` decides from the SCHEMA
(`getField("source_id") != null`), never from the value — a `GenericRecordBuilder` throws on a field
its schema lacks, so a value-driven check would break the raw and rejected sinks.

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

**2026-08-03 — `simulation` (int, default 0) added to ALL FOUR schemas** (`AggregatedLevel` gets it per level, not per record). Same re-registration trap as `pipeline_timings`: a stale registered subject makes the Avro sink throw. See [[simulation-flag]].
