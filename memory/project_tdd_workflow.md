---
name: tdd-workflow
description: Team uses TDD for all dev; how we add test coverage to the existing Flink orderbook-job (harness, deps, spec-vs-impl stance)
metadata:
  type: feedback
---

## Practice: TDD for all software development

The team develops test-first (red → green → refactor) for **new** code: write a failing
test that states the desired behaviour, make it pass with the minimum code, then refactor.

**Existing code (e.g. the Flink `orderbook-job`) gets retroactive coverage written against
the documented contract, not the implementation.** Each test asserts what the Javadoc/spec
says the code *should* do (snapshot replaces, update upserts/deletes, stale/dup dropped, gap
→ await snapshot, sort order, etc.). If the implementation deviates from the spec the test
fails — that is a found bug to raise, **not** a reason to weaken the test to match the code.
Do not write pure characterization tests that just freeze current behaviour.

**Resolved (2026-06-30):** the update sequence contract is **strict** — an update is applied
iff `seq == lastSeq + sequence_jump`. Any other `seq > lastSeq` (forward gap *or* an
unexpected intermediate value) discards the book and awaits the next snapshot. The merger
had a bug: it applied the whole band `lastSeq < seq <= lastSeq + jump`. Fixed in
`OrderBookMerger.processElement` (condition is now `seq != lastSeq + jump → resync`) via
TDD red→green; `seqBelowExpectedIsTreatedAsGap` pins it. First spec-based test run found a
real MVP bug — the approach works.

## Test infrastructure for `flink/orderbook-job` (Maven, Java 21, Flink 2.2.0)

- **JUnit 5 (jupiter)** + **AssertJ** for assertions; **surefire** configured to run JUnit5.
- A `KeyedProcessFunction` like `OrderBookMerger` is tested with Flink's
  **`KeyedOneInputStreamOperatorTestHarness`** (from `flink-test-utils`, test scope) wrapping
  a `KeyedProcessOperator`. The harness builds real keyed `MapState` and calls
  `open(OpenContext)` — the Flink 2.x `open` signature this code uses — so state, sequence
  validation and the rebuild-from-state path are all exercised for real. Avoid hand-mocking
  `MapState`/`Collector`/`Context`; it tests a fake backend, not Flink's.
- **JaCoCo** (`jacoco-maven-plugin`) for coverage; report at `target/site/jacoco`.
- Flink runtime deps are `provided` scope in the main pom — they are still on the test
  classpath, but `flink-test-utils` (and the `flink-runtime`/`flink-streaming-java` test-jars
  it pulls) must be added at `test` scope for the harness.

Related: [[orderbook-aggregation]] (the behaviour under test), [[bigdecimal-rules]]
(price/qty are BigDecimal-from-string — assert on exact decimal strings, never doubles).
