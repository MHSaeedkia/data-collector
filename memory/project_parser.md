---
name: parser
description: 2026-09-19 — job-parser split out of job-pair-extractor (raw pipeline job 1): the parsed-book-event wire type, why the topic is per-exchange, which drop rules moved, and the job renumbering it forced everywhere
metadata:
    type: project
---

# Job 1 — parser (split out of the pair extractor, 2026-09-19)

`flink/normalizer/job-parser/` (package `io.tibobit.normalizer.parse`, [[normalizer-scaffold]]
conventions). Consumes `^ex[0-9]+-raw$` → emits `ex{exchange_id}-parsed-flink` (subject
`parsed-book-event`). Stateless `RichFlatMapFunction`, no keying, no DB.

Started by a teammate on `refactor/standalone-parser-job` (`577f2f8`) and finished on the same
branch 2026-09-19. **The Java, the schema, the topics and the pipeline wiring were the teammate's
work and were already complete and green**; what was finished afterwards was the sweep the split
left behind — see § "What the first commit left" at the end.

## Why it exists

[[pair-extractor]] did two unrelated things: turn a verbatim exchange payload into a structured
book event, and resolve the exchange's own market string to `pair_id`. The second needs postgres,
the first needs nine hand-written wire-format parsers; a change to either forced a redeploy of
both. Splitting them is the single-responsibility argument, and it also means **job 1 ships without
the postgres driver at all** — the jar carries no `org/postgresql` entries, so the 2026-08-25
`ClassNotFoundException: org.postgresql.Driver` class of failure cannot happen to it.

## Decisions

- **`ParsedBookEvent` WRAPS a `RawOrderBookEvent` rather than redeclaring its fields.** It lives in
  `common/` because it is a WIRE type now, not a parser-internal struct. Wrapping is what keeps the
  two schemas from drifting: a field added to the raw event is carried here for free, and the only
  thing `parsed_book_event.avsc` says differently is `market` (a string) in place of `pair_id`.
- **The output topic is per EXCHANGE, not per (exchange, pair)** — `ex{id}-parsed-flink`. `pair_id`
  is exactly the thing this job does not know, so there is no `p{id}` segment to put in the name.
  The ordering guarantee is unchanged, because one partition per exchange is already what
  `ex{id}-raw` gives.
- **The output name deliberately does NOT match `RAW_TOPIC_PATTERN` (`ex[0-9]+-raw`)**, so the job
  cannot consume its own output. Worth keeping in mind before renaming either topic family.
- **The `parsed-book-event` schema re-declares the FULL `pipeline_timings` record**, not just
  `parse_in`/`parse_out`. Only the parse pair is ever set on this stream; the rest exist so the
  record is the same shape as the one `raw-order-book-event` carries.
- **`parse_in`/`parse_out` are their own field pair, not shared with `pair_extract_*`.** The whole
  point is that the wait rendered in front of pair-extract is now measurable — it is the Kafka hop
  the split introduced. Sharing one pair would have hidden exactly the cost the split added.

## Which drop rules moved, and which did not

Job 1 (all counted, none dead-lettered — dead-letter is [[type-validator]]'s concern):

| counter               | rule                                         |
| --------------------- | -------------------------------------------- |
| `dropped-no-parser`   | no parser for the exchange                    |
| `dropped-unparseable` | unrecognized/malformed frame (whitelist rule) |
| `dropped-no-id`       | no `id` on the payload — WARN + drop          |

Job 2 keeps exactly one: `dropped-unknown-market`.

**The no-id check HAD to stay in job 1** and this is not a style call: it reads the id the parser
lifted off NiFi's payload, and the operator overwrites that field with its own minted id on the very
next line. By the time job 2 sees the record there is no missing id left to detect. The
"NiFi is a hard dependency — deploy NiFi's change BEFORE this jar" decision (2026-08-03) therefore
now belongs to job 1's javadoc, not job 2's.

## Lineage gained a hop

`NiFi id` → job 1 mints its own id, `source_ids = [NiFi id]` → job 2 mints a fresh id,
`source_ids = [job 1's id]`. One element per hop, the [[record-lineage]] convention, unchanged in
kind — just one link longer. A payload fanning out to several events gives each fanned-out event
its own id while they all share the one NiFi parent, so the no-id rule drops all or none of them.

## ⚠ Every downstream job's NUMBER changed

This is the part that bites when reading anything written before 2026-09-19:

| stage         | was   | now   |
| ------------- | ----- | ----- |
| parse         | —     | job 1 |
| pair-extract  | job 1 | job 2 |
| type-validate | job 2 | job 3 |
| rebase        | job 3 | job 4 |
| precision     | job 4 | job 5 |
| book-build    | job 5 | job 6 |
| aggregate     | job 6 | job 7 |

**Dated sections in `memory/` and `todo.md` were deliberately NOT renumbered** — they are records of
what was decided on a date, and rewriting their numbers would falsify them. Read any pre-2026-09-19
entry with the table above in hand. Source comments, READMEs, `sample-raw-data.md`, the swagger
annotations and `exporter.py` WERE swept, so code and docs are on the new numbering.

Note that `sample-raw-data.md`'s many "Parsing notes (job 1)" headings stayed job 1: parsing IS job
1 after the split, so those were correct by accident. The same is true of "`event_time` is job-1
processing time" everywhere — the parser is what substitutes it.

## Operational notes

- **Submission order**: `NORMALIZER_JOBS` is downstream-first, so `job-parser` is submitted LAST.
  Every source reads from `latest`, so a job started after its upstream misses whatever the upstream
  produced in between.
- **`warmup` must run first.** It creates `ex{id}-parsed-flink` (per distinct exchange, in the raw
  block so it exists before job 2's source starts) and registers `parsed-book-event` — the registry
  step is automatic, since `RegisterDir` globs `schemas/*.avsc`. `make refresh-normalizer` and
  `prod-deploy` do it; `run-normalizer-jobs` and `run-all-jobs` do NOT.
- **Slots**: 9 jobs now need 15. Prod is 4 TaskManagers × 4 = 16, so **one spare slot**. Dev's
  `numberOfTaskSlots` is 50 and has been since before the split, even though the comment beside it
  reasons about 15 — pre-existing drift, not introduced here.
- **Staleness**: `ex{id}-parsed-flink` is listed BY HAND in `lpa-staleness-exporter/config.yaml`
  next to `ex{id}-raw`. It cannot be derived from a subscription row the way the per-pair stages are,
  because it has no `pair_id`. Without it a dead parser shows up only as every per-pair stage below
  it going stale at once, which points at nothing. See [[staleness-exporter]].
- `flink/job-discovery.sh` finds jobs by grepping poms for `<mainClass>`, so `run-job.sh`,
  `run-local.sh` and the `.vscode` launch config needed no hardcoded-list change.

## What the first commit left, and what closing it involved (2026-09-19)

The teammate's commit was functionally complete — 325 tests green, all Go modules clean, the shaded
jar packaging with the right `Main-Class` and carrying its parsers. The gaps were all in the
surrounding surface:

1. **`ex{id}-parsed-flink` was monitored by nothing** (the real one). Added to the exporter's manual
   `topics:` block for all nine exchanges at threshold 30, matching `ex{id}-raw`.
2. **The renumbering above was applied in ~8 files and missed ~100.** Swept mechanically (+1 for
   old job 2–6) with every `job 1` reference reviewed by hand, because "job 1" splits three ways:
   parsing stays 1, pair-resolution/raw-event-minting becomes 2, and the files the teammate already
   rewrote were on the new numbering and had to be left alone.
3. `README.md` did not list `./run-job.sh job-parser` and still said "6 chained Flink jobs".
4. `sample-raw-data.md` § ex9 pointed at `job-pair-extractor/src/test/resources/fixtures/`, which
   moved with the parsers.
5. The latency-monitor README's sample output still showed a 5-stage table; it renders 6 now.
6. `e2e/docs/` is generated — regenerated with `e2e/swagger-update.sh` rather than hand-edited.

## Live verification (2026-09-19, local dev stack)

**5 e2e scenarios run against the split, all PASS**: 01 (ex1 plain snapshot feed, whole chain),
34 (ex6 sequence gap → reset → control plane, across the new hop), 36 (ex6 noise frames — the drop
rules now span jobs 1 AND 2) and 39 (ex8 delta feed, run 3×). `/jobs/overview` showed 7 normalizer
jobs RUNNING including `normalizer-parser`.

The load-bearing observation: the latency monitor on `ex8-p1-orderbook-snapshot-flink` rendered

```
  1 parse          15:08:49.557   15:08:49.613         56ms        n/a
  2 pair-extract   15:08:49.803   15:08:49.803           0s      190ms
  3 type-validate  15:08:49.907   15:08:49.907           0s      104ms
```

— `parse_in`/`parse_out` stamped, and a real **37–190 ms** wait in front of pair-extract. That wait
IS the Kafka hop the split introduced, which is the whole reason the two got separate timing fields.

Two things worth knowing before running this yourself:

- **Run with `-provision-stack=false` against a stack you brought up yourself.** The harness's
  default `stack.Provision` does `docker compose down -v`, which destroys the kafka, postgres and
  **NiFi** volumes. `SCENARIO=<n> go run . -provision-stack=false` from `e2e/` gives the same
  coverage without that. `SCENARIO` takes the leading number or the full name.
- **Attaching the latency monitor mid-run is fiddly**: it tails from the END, and every scenario
  deletes and recreates its topics, so a tailer started too early dies `UNKNOWN_TOPIC_ID` and one
  started too late misses the records. Re-attach in a loop for the length of the run.
- `source` and `end-to-end` print as large NEGATIVE durations on replayed scenarios — the fixtures'
  exchange timestamps are ~117 days old. Fixture artifact, not a pipeline fault.

⚠ **NOT deployed to any server**, the other ~63 scenarios have not been run against the split, and
none of the five was mutation-checked — they are proven to pass, not proven to bite. Also note `.vscode/settings.json` picked up an unrelated `java.jdt.ls.vmargs` line in
the teammate's commit; left alone, since it is an IDE preference and not this refactor's business.
