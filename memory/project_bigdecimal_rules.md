---
name: bigdecimal-rules
description: Rules for handling price/quantity decimals with BigDecimal (never double) across the pipeline to avoid float precision loss
metadata:
  type: project
---

## Rule: prices/quantities are BigDecimal-from-string, never double

Floating point (`double`/`float`) is base-2 and cannot represent decimals like `0.1`
exactly, so it produces wrong results: `0.1 + 0.2 == 0.30000000000000004`. `BigDecimal`
stores an arbitrary-precision *decimal* (unscaled integer + scale), so the same sum is
exactly `0.3`.

**Rules to follow in this repo:**
- **Construct from the original string, never from a double.** `new BigDecimal("0.1")` is
  exact; `new BigDecimal(0.1)` bakes in the float error *before* BigDecimal sees it
  (it becomes `0.1000000000000000055...`). Price/quantity arrive as decimal strings on
  the wire precisely so they never pass through a `double` — keep it that way. See
  [[avro-schema-orderbook]] ("Why price/qty are strings").
- **Never parse a price/quantity into `double`/`float` anywhere in the path.** No
  `Double.parseDouble`, no `parseFloat`, no implicit widening.
- **add / subtract / multiply / compareTo are always exact** — safe, never throw.
- **Division is the only trap:** a non-terminating result (e.g. `1/3`) throws
  `ArithmeticException` unless you pass a scale + `RoundingMode`. We currently do no
  division on prices/quantities; if that changes, specify rounding explicitly.

**Equality caveat (already relied on):** `BigDecimal.equals`/`hashCode` are
scale-sensitive — `"97240.50"` and `"97240.5"` are NOT `.equals`, but `.compareTo == 0`.
So never key price levels by raw string or in a `HashMap<BigDecimal,…>`. Two live patterns:
`flink/orderbook-job` keys levels in a `TreeMap<BigDecimal,…>` (compareTo-based); the
consolidator's hash-based Flink `MapState` keys by the **canonical string**
`new BigDecimal(price).stripTrailingZeros().toPlainString()`. See [[orderbook-aggregation]]
and [[orderbook-consolidator-decision]].

**Why:** This is an exchange/financial pipeline — silent precision drift corrupts prices
and quantities. The whole string→BigDecimal discipline exists to make decimal math exact.
**How to apply:** Any new code touching price/quantity uses `new BigDecimal(theString)`
and BigDecimal arithmetic/comparison; if you ever see a `double` in a price path, treat it
as a bug.
