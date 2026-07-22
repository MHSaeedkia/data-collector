---
name: tdd-workflow
description: Team uses TDD for all dev; spec-vs-impl stance, Flink operator-harness test infrastructure, JaCoCo-on-JDK26 note
metadata:
  type: feedback
---

## Practice: TDD for all software development

The team develops test-first (red → green → refactor) for **new** code: write a failing test that
states the desired behaviour, make it pass with the minimum code, then refactor.

**Existing code gets retroactive coverage written against the documented contract, not the
implementation.** Each test asserts what the Javadoc/spec says the code *should* do (snapshot
replaces, update upserts/deletes, stale/dup dropped, gap → await snapshot, sort order, etc.). If the
implementation deviates from the spec the test fails — that is a found bug to raise, **not** a reason
to weaken the test to match the code. Do not write pure characterization tests that just freeze
current behaviour. (The strict update-sequence rule `seq == lastSeq + sequence_jump → else resync`
now lives in [[type-validator]]; a spec-based test first found the original off-by-band bug — the
approach works.)

Rule of thumb: if the only possible unit assertion is "builder returned non-null", it is an
integration test, not a unit test — don't write the worthless non-null test. Plain getter/setter
POJOs with no logic are deliberately NOT unit-tested; the ser/deser tests exercise their bindings.

## Flink operator test infrastructure (Maven, Java 21, Flink 2.2.0)

- **JUnit 5 (jupiter)** + **AssertJ** for assertions; **surefire** configured to run JUnit5.
- A `KeyedProcessFunction` is tested with Flink's **`KeyedOneInputStreamOperatorTestHarness`** (from
  `flink-test-utils`, test scope) wrapping a `KeyedProcessOperator`. The harness builds real keyed
  `MapState` and calls `open(OpenContext)` — the Flink 2.x `open` signature this code uses — so
  state, sequence validation and the rebuild-from-state path are all exercised for real. Avoid
  hand-mocking `MapState`/`Collector`/`Context`; it tests a fake backend, not Flink's.
- Flink runtime deps are `provided` scope in the main pom — still on the test classpath, but
  `flink-test-utils` (and the `flink-runtime`/`flink-streaming-java` test-jars it pulls) must be
  added at `test` scope for the harness.
- Serde tests build `GenericRecord`s via `GenericRecordBuilder` and assert on the pure mapping
  functions — the Confluent registry encode/decode itself is library code, not re-tested.

## GOTCHA — JaCoCo 0.8.12 on JDK 26

`mvn test` under JDK 26 can print alarming `java.lang.instrument.IllegalClassFormatException ...
Unsupported class file major version 70` while JaCoCo tries to instrument bootstrap classes. Run
Maven with `-Djacoco.skip=true` to avoid the noise; the tests themselves run and pass regardless.

Related: [[orderbook-aggregation]] (the behaviour under test), [[bigdecimal-rules]] (price/qty are
BigDecimal-from-string — assert on exact decimal strings, never doubles).
