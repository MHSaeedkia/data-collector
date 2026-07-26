---
name: e2e-harness
description: 2026-07-25 PLAN (not implemented) — Go end-to-end test harness replacing the 6 smoke-*.sh + manual-test-data/{produce,reset}.sh; raw in, aggregated book asserted out; design decisions, the sharp edges, and the phasing
metadata:
    type: project
---

# Go e2e test harness — PLAN, 2026-07-25

**Status: DESIGN ONLY. No code written.** Decided with the user 2026-07-25; implementation deferred.
Tasks live in `todo.md` M13.

## Why

Two disconnected test surfaces today, neither closes the loop:

- **`flink/normalizer/smoke-*.sh`** — 6 scripts, ~1200 lines, ~200 of which are byte-identical
  boilerplate copy-pasted five times. Each stops at its OWN job's topic. Coverage is ex8/OKX only
  (plus job 1's 12-fixture sweep).
- **`manual-test-data/`** ([[manual-test-data]]) — the valuable asset: 17 scenarios of real raw
  payloads across ex8/ex3/ex1/ex2. But **every expectation is prose** in a 629-line README, so the
  oracle is a human reading markdown and eyeballing the web UI. **Nothing in it has ever been run
  live**, and `reset.sh`'s Flink REST flow is unproven.

The user wants: raw in → real chain runs → assert the aggregated book on `p{pair}-{side}`. What
happens in the middle is explicitly not the priority right now. Language is **Go** — the user's
strong preference, and every dependency is already vendored in `web/`.

## User decisions (2026-07-25 Q&A — do not re-litigate)

1. **A `go test` package**, build-tagged. The free subtest/filter/report machinery IS the ~200 lines
   of duplicated bash boilerplate; that is the point, not a side benefit.
2. **All four oracles**: aggregated book, dead-letter records, **stage topics asserted ALWAYS**, and
   a full stage dump on failure.
3. **Expectations as a Go table in the test file**, not data files beside the payloads.
   Compile-time checked. (Rejected: `expect.json` per dir — drift risk; parsing the README — brittle.)
4. **All the bash is deleted** — `produce.sh`, `reset.sh`, all 6 `smoke-*.sh`.

Decision 2 is what makes decision 4 survivable: the per-stage contracts move into the harness rather
than vanishing. **I advised against deleting the smokes before the Go harness runs green; the user
reaffirmed. Sequenced last (M13 phase 5) so there is a working oracle during debugging.**

## Design decisions worth keeping

**New top-level `e2e/` module, NOT inside `web/`.** `web/Dockerfile` runs `go test -mod=vendor ./...`
during the image build — an e2e package there would break `docker compose build web`.

**No vendoring.** `web/vendor/` exists only because that Dockerfile must build offline
(proxy.golang.org 403s on this dev machine). The harness never builds in Docker. Verified
2026-07-25: `franz-go@v1.21.4` and `hamba/avro/v2@v2.31.0` are already in the local module cache;
`pkg/kadm` is not, but **fetching it over the network worked**, so the 403 risk did not materialize.
Pin to web/'s versions so the cache is shared.

**No `docker exec` anywhere.** Everything talks TCP to the host-published compose ports — kafka
`localhost:9092`, registry `8082`, Flink REST `7070`, postgres `5432`. This is the single biggest
win over the bash, and it also kills the `grep -m1 '^{'` needed to strip log4j noise from
`kafka-avro-console-consumer` stdout.

**Keep the bash's proven read pattern**: capture the topic's END offset before producing, then read
from exactly that offset. No consumer group, no `auto.offset.reset` race. `kadm.ListEndOffsets` is
the direct analogue of the `kafka-get-offsets` the scripts already shell out to; read via
`kgo.ConsumePartitions` at an explicit offset. Every topic is single-partition (`warmup.sh` creates
them `--partitions 1`), so partition 0 is always right.

**Do NOT reuse `web/internal/kafka/consumer.go`** — consumer-group + regex-subscribe, and it drops
key/offset/timestamp. Wrong shape. There is no producer anywhere in the repo to reuse.

**DO copy the approach in `web/internal/schema/decoder.go`** (magic byte `0x00`, 4-byte big-endian
schema id, `GET {registry}/schemas/ids/{id}`, `avro.Parse`, cache forever under a mutex). Its wire
structs and `magicByte` are package-private and it only decodes the aggregated shape, so the harness
needs its own generalized copy.

**Quiescence, not sleep.** Read until the expected count, OR a 2s quiet window after the first
record, OR a 25s hard cap (= the scripts' `CONSUME_TIMEOUT_MS`). Matters most on the aggregated
topic: the aggregator emits one event per side per input message, so the final book is the **last**
record per side — never a count.

## Sharp edges (the things that will silently break the port)

- **`encoding/json` must use `Decoder.UseNumber()`.** Without it every number becomes `float64` and
  `1800000000000` re-marshals as `1.8e+12` — job 1 sees a different wire shape. Easiest way to break
  this invisibly.
- **The timestamp field is a different Go type per exchange** (verified against the payloads
  2026-07-25): ex8 `.data[].ts` is a **string**; ex1 `lastUpdate` is a **number**; ex2 `event_time`
  is a **number on REST and an ISO-8601 string on WS** under the same name; ex3 has none and is a
  **top-level JSON array**, so the shifter must type-switch before touching it.
- Port all of `produce.sh`'s shift rules, including: ex8 aligns to the 300 ms grid (its `ts` is
  simultaneously event time AND sequence id); ex1 leaves `push.pub.offset` **untouched** (the
  sequence is independent of the timestamp); ex2 shifts both shapes by the same delta in different
  units (bases are whole seconds precisely so this stays exact); ex3 is never rewritten.
- **Scenario 16 file `05-rest-snapshot-string-event-time.json` must NOT be shifted** — its whole
  point is that job 1 drops it. Same `else . end` guard the jq has.
- The jq quirks in the scripts (`.pipeline_timings["io.tibobit.orderbook.PipelineTimings"]`,
  `.asks.array[0]` on raw vs bare `.asks[0]` on the snapshot schema) are
  `kafka-avro-console-consumer` JSON-encoding artifacts. Decoding into Go structs makes them
  disappear — **do not port them.**
- hamba/avro mapping: `timestamp-millis` → `time.Time`, `["null", T]` → **pointer**, enums →
  `string`. Prices/quantities stay `string` per [[bigdecimal-rules]] — never parsed to float.

## Isolation hazards

- **Scenarios cannot run in parallel.** All 17 target `pair_id` 1 and the aggregator's per-exchange
  state survives across runs. `t.Run` without `t.Parallel()`.
- **Every scenario resets first** — same three stateful jobs, same downstream-first order as
  [[manual-test-data]]'s `reset.sh`. No checkpointing on this platform ⇒ cancel+resubmit IS the reset.
- **Assertions scoped by `exchange_id`**, as `smoke-aggregator.sh` already does. Never assert "p1 has
  exactly N levels" — another exchange or a live NiFi feed may legitimately contribute.
- **A live NiFi feed on the exchange under test corrupts results** and exchange-scoping does not save
  it. Preflight: fail loudly if the raw topic's end offset advances while idle. Document stopping NiFi.
- **Preflight the reference data** as the scripts do: market 1 must be `price_precision 2` /
  `quantity_precision 8` and rebase `0/0`, or every expected digit is wrong.

## Honest scoping

The **dead-letter oracle is already tabulated** in the README's final table (01–07: 1/0/2/2/0/0/0;
08–12 and 13–17: 0/1/2/0/1 each) — encoding it is mechanical, which is why it is phase 2.
The **final-book expectations are NOT tabulated** and must be derived per scenario from prose plus
the payload files. That is the bulk of the work and the reason phase 3 is the long pole.

## Residual coverage the harness does NOT absorb

Two contracts die with the smokes unless separately ported — decide at phase 5:

- **`smoke-rebaser.sh`'s DB mutation** (UPDATE `exchange_markets`, wait out the 60s
  `RefreshingLookup`, restore on an EXIT trap). The seed is `0/0` = identity, so **nothing else in
  the repo proves job 3 works** ([[rebaser]]).
- **`smoke-pair-extractor.sh`'s 12-fixture sweep** across ex1–ex6+ex8. The scenarios only cover 4
  exchanges, so deleting it loses parser coverage for **ex4, ex5, ex6** ([[pair-extractor]]).

## Risk that dominates everything else

The expectations themselves have never been validated live. Early red tests are as likely to be the
README's fault or the harness's as the pipeline's — **budget for triage, not just implementation**.
Phase 1 (reset + produce one scenario, assert nothing) is deliberately shaped to retire the oldest
unknown first: whether `reset.sh`'s REST flow works at all.
