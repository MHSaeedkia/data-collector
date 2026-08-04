---
name: record-lineage
description: id / source_ids record lineage across the whole normalizer pipeline — the model, the four user decisions behind it, and the traps
metadata:
    type: project
---

# Record lineage (`id` / `source_ids`)

**Added 2026-08-03 (user request). Field renamed `sink_id` → `id` on 2026-08-04**, also by user
request — a pure rename, no logic touched. Two fields that answer "where did this record come from".

- **`id`** — a fresh UUID minted by whichever step WROTE the record to its topic. Unique per
  record, never reused. NiFi mints the first one; each job mints its own on emit. A record crossing
  the whole pipeline therefore carries **seven different ids** in turn — NiFi's, then one per job.
- **`source_ids`** — the ids of the records read **on that hop**.

The critical thing to hold onto: `id` is not a correlation id and does **not** survive a hop.
It is re-stamped every time. To follow a record you walk the chain hop by hop; nothing carries an
end-to-end identity. This is the opposite of how `simulation` behaves ([[simulation-flag]]), and
mixing the two mental models is the easiest mistake to make here.

⚠ **The name now works against that.** A field called `id` reads like the record's stable identity,
and it is exactly the opposite: it identifies the WRITE, not the record's journey. `source_ids` (the
plural, and the word "source") is what keeps the model legible — do not "tidy" it to `parent_id` or
fold it into the `id`. The old name `sink_id` said this out loud; the current one does not, so this
paragraph is the only thing left that does.

## The four decisions the user made (2026-08-03)

These were explicitly chosen from alternatives — do not "simplify" them back.

1. **Job 5's `source_ids` = every event still holding a resting level**, not just the triggering
   event. This is why the book builder's MapState value had to change shape.
2. **The aggregated record gets a per-record `id` and a per-LEVEL `source_id`** — not a
   record-level `source_ids` array.
3. **Immediate parents only**, never the accumulated chain.
4. **A payload with no `id` is DROPPED** by job 1, not passed through and not given a
   substitute.

## Decision 4 is an operational landmine — read this before deploying

Job 1 drops any payload whose `id` is missing or blank (`dropped-no-id` counter, WARN
log). **A NiFi processor that does not inject `id` loses 100% of its data**, silently apart
from the counter. **Deploy NiFi's change BEFORE these jars, never after.** If a feed goes dead after
a deploy, check that counter first.

## Where it rides on the wire — the same two carriers as `simulation`

- **ex1, ex2, ex4, ex5, ex6, ex8** — object payload root, so NiFi injects `"id": "<uuid>"` as a
  **root field**.
- **ex3/wallex** — array root, so it goes in the **same trailing object as `simulation`**:
  `["{market}@{side}", [levels…], {"simulation":1,"id":"<uuid>"}]`. **Not a fourth element** —
  `WallexParser` drops any envelope longer than 3. The parser reads it from `root.path(2)`, so
  "index 2" is the rule, not "the last element" (this matters — see the e2e note below).

`Json.sourceIds(carrier)` mirrors `Json.simulation(carrier)`: same carrier node, one field.

**`sample-raw-data.md` is the NiFi-facing contract for this** (2026-08-04): all 12 per-exchange
samples now show `id` and `simulation` in the right carrier, under a "NiFi-injected fields" section
that states the drop rule. It is what the NiFi team reads, so a change to either field's placement
has to land there too or the producer side will not hear about it.

## Per-job behaviour

| Job | What it does |
| --- | --- |
| 1 pair-extract | Reads NiFi's id into `source_ids`, mints its own. **Fan-out**: one payload → N events all sharing the one source, each with its OWN id. Drops the payload if there is no source |
| 2 type-validate | Re-stamps on the main stream. The gap case is a lineage FAN-OUT: the `reset` marker and the dead-letter record are two children of the one gap event, each with its own id |
| 3 rebase / 4 precision | **No longer no-ops** (unlike `simulation`): they write to a topic, so they re-stamp. Job 3's `no_rebase_row` dead-letter gets its own id too |
| 5 book-build | The genuine FAN-IN. See below |
| 6 aggregate | Mints a record `id`; `SnapshotSplitter` stamps each level's `source_id` from the snapshot it came from |

### Dead-letter records have TWO lineages, and they are not the same thing

The envelope's `id` is the dead-letter record's own; its `source_ids` is the rejected event.
The **nested** event keeps the id it arrived with and is deliberately NOT re-stamped — it is being
*recorded*, not *forwarded*, and that id is what links the dead-letter back to the raw stream.

### Job 5 — the only real fan-in

`source_ids` = the triggering event, **then** the distinct events still holding a resting level
(asks order, then bids, deduplicated). So it grows with **book depth**, not pipeline length.

This is why `MapState` went from `price → quantity` to `price → RestingLevel{price, quantity,
id}`. A level updated by a later event transfers to that event, so an older event drops out of
the lineage once nothing it set is still resting — otherwise the list would only ever grow.

**The triggering event is included unconditionally, and that is load-bearing.** An event that only
DELETES levels, or a `reset` that empties the book, leaves nothing resting — a strict "only resting
levels" reading would emit a record whose sources do not mention the event that produced it,
breaking the chain at exactly the moments worth tracing.

⚠ This changes the value type of the existing `asks`/`bids` state. No checkpointing is configured
platform-wide, so there is nothing to migrate today; if that ever changes, this is a breaking
state-schema change.

### Job 6 — why per level, and what it costs

The union mixes exchanges, so a record-level parent list would flatten away which exchange each
parent belongs to — the same argument that already put `exchange_id` and `simulation` on the level.
`source_id` is singular because a level comes from exactly one snapshot.

**Known consequence:** an aggregated record with **no levels** (every exchange reset) carries no
parent information at all. Accepted, by construction of decision 2.

## Traps

- **Avro hands back `Utf8`, not `String`.** An unconverted id compares unequal to every String it
  should match while printing identically in a log. `serde/LineageRecords` converts at the boundary
  and `RawOrderBookEventDeserializerTest.convertsUtf8LineageToString` is the guard — an in-memory
  `GenericRecordBuilder` round trip does NOT reach this, because it stores back whatever object it
  was given.
- **Order matters when re-stamping.** `sourceIds = [id]` must happen BEFORE minting the new
  id, or the record becomes its own parent — and that failure is invisible, since both fields
  still hold well-formed uuids. `model/Lineage.restamp()` exists solely to make this unmissable.
- **RE-REGISTER ALL FOUR SUBJECTS.** Same trap as `pipeline_timings` and `simulation`: a stale
  registered subject makes the Avro sink throw on the unknown field. Every field defaults
  (`""` / `[]`), so old records still read.

## Coverage

- `RecordIdTest` (job 1) — the cross-parser rule, mirroring `SimulationFlagTest`. 21 cases.
- Per job: fan-out + drop (1), re-stamp/reject/gap-fan-out (2), re-stamp + reject (3), re-stamp (4),
  fan-in incl. delete-only, reset, overwrite-transfers-owner and dedup (5), per-level through the
  union + reset-drops-out (6). Plus Avro round trips and the Utf8 guard in common.
- **202 Java tests green** (surefire total across the 7 modules). `AggregatedOrderBookSerializer`
  has no unit test (the aggregator pom does not copy the avsc onto its test classpath) — its two
  new fields are covered by e2e only.
- e2e: `scenario/lineage.go` + `lineage_test.go`; **verified live** on 5 scenarios (ex1 object root,
  ex3 array root incl. noise frames, ex5 fan-out, ex6 delta feed, ex1 gap→reset) — **those live runs
  predate the rename**; nothing has been run against a stack since, only the test suites.

## What e2e can and cannot assert

The ids are random per run, so **no scenario can declare them**. The harness therefore:

1. **Injects** a `id` into every source at produce time, exactly as NiFi does — the 177
   fixtures are not edited. Job 1 drops unstamped payloads, so a stamping bug does not fail loudly:
   it empties every snapshot stream in the suite and reads as a broken pipeline. `lineage_test.go`
   stamps all 177 sources offline to catch that cheaply.
2. **Checks structurally** — present, well-formed uuid, unique per topic, sources non-empty and
   deduplicated.
3. **Strips the fields** before the literal comparison, so the existing per-scenario expectations
   still work untouched.

**The one EXACT cross-job assertion**: every level of the final aggregated record must carry the
`id` of a snapshot job 5 really emitted, and specifically the LAST one (a scenario feeds one
exchange). This is what makes decision 2 testable end to end — a record-level `source_ids` could
only ever have been checked for shape.

⚠ The **NiFi → job 1 link is NOT covered by e2e**: the harness reads the snapshot and aggregated
topics, never the raw topic, so the id it injects never appears in anything it reads back. That link
is covered by unit tests only.

Two harness details worth keeping:

- The stamper edits payloads **textually** (object roots) or via `json.RawMessage` (array roots).
  It must never unmarshal/re-marshal a payload wholesale: ex3 and ex5 carry prices as JSON NUMBERS
  and a round trip through `float64` would silently reformat them ([[bigdecimal-rules]]).
- It stamps array element **index 2**, not the last element. `18-ex3-noise-frames` feeds a
  deliberately malformed 4-element envelope ending in a bare string; stamping "the last element"
  fails on it, while stamping slot 2 preserves the malformed shape and the test's whole point.

## Web

`wireLevel.source_id`/`wireEvent.id` → `domain.RawLevel`/`RawBook` → `domain.Level`/`Book` →
the WebSocket JSON. **The browser UI does not render them** — plumbed only, exactly like
`simulation`.

Related: [[avro-schema]], [[simulation-flag]], [[pair-extractor]], [[type-validator]], [[rebaser]],
[[precision]], [[book-builder]], [[aggregator]], [[e2e-harness]], [[orderbook-web]],
[[bigdecimal-rules]].
