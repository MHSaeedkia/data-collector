---
name: record-lineage
description: id / source_ids / trigger_id record lineage across the whole normalizer pipeline — the model, the user decisions behind it, and the traps
metadata:
    type: project
---

# Record lineage (`id` / `source_ids` / `trigger_id`)

**Added 2026-08-03 (user request). Field renamed `sink_id` → `id` on 2026-08-04**, also by user
request — a pure rename, no logic touched. Two fields that answer "where did this record come from".

- **`id`** — a fresh UUID minted by whichever step WROTE the record to its topic. Unique per
  record, never reused. NiFi mints the first one; each job mints its own on emit. A record crossing
  the whole pipeline therefore carries **seven different ids** in turn — NiFi's, then one per job.
- **`source_ids`** — the ids of the records read **on that hop**. On jobs 1–4 and the dead letters.
- **`trigger_id`** — job 5's snapshot only, and its ONLY record-level parent: the event that caused
  the emit. Not an array, and **not necessarily one of the level ids** (see below).
- **`source_id` on a LEVEL** — job 5's snapshot levels and job 6's aggregated levels each name one
  parent per price rather than per record. Different questions: job 6's names the snapshot,
  job 5's names the event that set that price. See "Tracing ONE price" below.

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
   **SUPERSEDED 2026-08-08** (same user, after the per-level `source_id` landed): the snapshot's
   record-level array is gone, replaced by a single **`trigger_id`**. The array had become the union
   of the level ids plus the trigger, so all it still added was the trigger — and only
   *positionally*, as its first element. The MapState change this decision forced is unaffected and
   is now load-bearing for the per-level ids instead.
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
| 5 book-build | The genuine FAN-IN, at two granularities: a per-level `source_id` **and** the record's `trigger_id`. No `source_ids` on this record. See below |
| 6 aggregate | Mints a record `id`; `SnapshotSplitter` stamps each level's `source_id` from the snapshot it came from |

### Dead-letter records have TWO lineages, and they are not the same thing

The envelope's `id` is the dead-letter record's own; its `source_ids` is the rejected event.
The **nested** event keeps the id it arrived with and is deliberately NOT re-stamped — it is being
*recorded*, not *forwarded*, and that id is what links the dead-letter back to the raw stream.

### Job 5 — the only real fan-in, and the only record with lineage at TWO granularities

**Each emitted level carries its own `source_id`** — the single job-4 event that last SET that price
(2026-08-08, user request). The data was already in `RestingLevel.getId()`; `priceLevels()` was
dropping it on the way out, so the fix was one schema field and one constructor argument. Added
because a record-level set cannot answer the question the whole feature exists for: it has no
mapping back to prices, so tracing one price stopped at "it came from one of these N events". The
trace **widened** at job 5 while every other hop narrows to one parent.

**The record itself carries only `trigger_id`** — the event that caused this emit. Once the levels
name their own owners, an array of every contributing event is exactly `{trigger} ∪ {level owners}`,
so it adds nothing but the trigger. A single field says the same thing and says WHICH one it is,
instead of relying on "first element by contract".

⚠ **`trigger_id` is not always among the level ids, and that is the entire reason it is a separate
field.** An event that only DELETES levels owns none of them; a **reset** empties the book, so there
are no levels at all. Fold the trigger in with the level owners and those records name no parent
that has anything to do with why they exist — the chain breaks at exactly the moments worth tracing.
`deleteOnlyEventIsStillTheTrigger` and `resetKeepsItsOwnTrigger` are the guards.

**A level's id is the event that last SET it, not the event that triggered the emit.** An update
touching one price leaves every other level naming whichever event put it there, however long ago —
the lineage follows the price, not the message. That is exactly the property that makes it useful,
and it is what `untouchedLevelKeepsItsOriginalEvent` pins down.

⚠ The two halves still cross-check, just differently now that the record no longer restates the
level ids. Job 5 emits one book per accepted event, so **trigger ids are distinct across the
stream**, and a level can only have been set by an event that already arrived, so **every level's
`source_id` must be the trigger of that record or of an earlier one**. e2e asserts both; the second
is strictly stronger than the membership check it replaced.

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

**The snapshot's per-level `source_id` is deliberately NOT copied through** (2026-08-08, user
decision, made explicitly after the alternatives were laid out). An aggregated level names the
job-5 snapshot and nothing else. Copying job 5's per-level id would shorten a trace by one hop, but
it names a job-4 event this record never read: the aggregated record would point straight past job 5,
and since it has no record-level parent either, the job-5 hop would vanish from the lineage
entirely. Each hop, one step — the rule holds at level granularity too.

## Tracing ONE price back to its raw event

This is the question the whole feature exists to answer, and the walk is worth writing down:

1. Aggregated level `(exchange 1, price 10.00)` → its `source_id` is a **job-5 snapshot** id.
2. Open that snapshot → **the level with the same price** → its `source_id` is a **job-4 event** id.
   (Price is a safe join key: job 4 has already merged levels that share a price, so it is unique
   within a book.)
3. Job 4 → 3 → 2 → 1, each record's `source_ids` naming the record before it.
4. Job 1's `source_ids` is NiFi's id → the message on `ex{n}-raw` carrying that `"id"`.

Six lookups, and that is inherent: decision 3 stores immediate parents only, so reaching raw always
means walking every hop. Step 2 is the one that only became possible on 2026-08-08 — before the
per-level `source_id`, it dead-ended at "one of these N events".

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
  (`""` / `[]`), so old records still read. **`order-book-snapshot` changed again on 2026-08-08**
  (per-level `source_id`) — if it was already re-registered for the first lineage change, it is
  stale AGAIN.

## Coverage

- `RecordIdTest` (job 1) — the cross-parser rule, mirroring `SimulationFlagTest`. 21 cases.
- Per job: fan-out + drop (1), re-stamp/reject/gap-fan-out (2), re-stamp + reject (3), re-stamp (4),
  fan-in (5), per-level through the union + reset-drops-out (6). Plus Avro round trips and the Utf8
  guard in common.
- Job 5's per-level ids: `stampsEachLevelWithItsOwningEvent`, `untouchedLevelKeepsItsOriginalEvent`
  (the 4-event case — a level naming an event three messages back), `snapshotOwnsEveryLevelItReports`.
  Its `trigger_id`: `namesTheTriggerAndAttributesEachLevel`, plus the two that justify the field
  existing at all — `deleteOnlyEventIsStillTheTrigger` and `resetKeepsItsOwnTrigger`.
  Serde: per-level round trip, unstamped-level-becomes-`""`, `roundTripsRecordLineage`, and — in the
  RAW serializer test — `ignoresSourceIdNotInThisSchema`, the guard on the schema-driven write.
- **207 Java tests green** (surefire total across the 7 modules). `AggregatedOrderBookSerializer`
  has no unit test (the aggregator pom does not copy the avsc onto its test classpath) — its two
  new fields are covered by e2e only.
- e2e: `scenario/lineage.go` + `lineage_test.go`; **verified live** on 5 scenarios (ex1 object root,
  ex3 array root incl. noise frames, ex5 fan-out, ex6 delta feed, ex1 gap→reset) — **those live runs
  predate the rename**; nothing has been run against a stack since, only the test suites.

## What e2e can and cannot assert

**The asymmetry that decides everything here:** a SOURCE id is an INPUT, so it can be a literal;
every id further down is minted by a job at run time and is random per run, so **no scenario can
declare one**. So:

1. **The 177 source payloads each spell their own `id` out** (2026-08-04, user request), the same
   way they spell `simulation` out — a distinct uuid per payload, in the carrier its parser reads.
   `stampID` **no longer overwrites an id that is already there**; it only fills one in, which is
   what keeps a scenario POSTed to the HTTP endpoint working when its author did not think about
   lineage. So what the fixture shows is what reaches the raw topic and can be matched by eye.
   ⚠ A **present-but-blank** `id` is a hard error, not something to fill in: splicing into an object
   root would leave the key twice and both Go and Jackson take the LAST one, so the blank would win
   and job 1 would drop the record — the exact silent failure this path exists to prevent.
2. **Checks structurally** — present, well-formed uuid, unique per topic, sources non-empty and
   deduplicated. `TestStampIDOnEverySource` also asserts all 177 fixture ids are unique across the
   whole suite and that each fixture really declares its own (so a lost one cannot be papered over
   by the fallback injection). For job 5's per-level ids the check is stronger than shape: every
   trigger id **must be distinct across the stream** (job 5 emits one book per event) and every
   level's `source_id` **must be the trigger of that record or of an earlier one** (a level cannot
   predate the event that set it). Both are exact, and the second replaced — and strengthens — the
   membership check that the old record-level array allowed.
3. **Strips the fields** before the literal comparison, so the existing per-scenario expectations
   still work untouched — including the per-level `source_id`. ⚠ `verify` keeps a **shallow** copy
   of the snapshots for the aggregated check, so the level slices are shared and stripping clears
   them in both. Harmless while an aggregated level names the snapshot RECORD; it would not be if
   that ever changed.

**The one EXACT cross-job assertion**: every level of the final aggregated record must carry the
`id` of a snapshot job 5 really emitted, and specifically the LAST one (a scenario feeds one
exchange). This is what makes decision 2 testable end to end — a record-level `source_ids` could
only ever have been checked for shape.

⚠ The **NiFi → job 1 link is NOT covered by e2e**: the harness reads the snapshot and aggregated
topics, never the raw topic, so the id it injects never appears in anything it reads back. That link
is covered by unit tests only.

Two harness details worth keeping (they still govern the fallback injection, and the ex3 one also
governs where a fixture must put its literal id):

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
