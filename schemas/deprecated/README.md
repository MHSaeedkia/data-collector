# Deprecated schemas

Avro schemas whose only producers/consumers are deprecated modules. Kept (not deleted)
so `scripts/warmup.sh` can still register their subjects while the deprecated modules
exist, and for reference.

- `orderbook_event.avsc` — the monolithic `flink/DEPRECATED-orderbook-job/` (subject
  `order-book-event`). No live consumer.
- `price_level_event.avsc` — job 6 output (now `flink/normalizer/DEPRECATED-job-level-emitter/`,
  retired in the Part D cutover 2026-07-22) = the deprecated `flink/DEPRECATED-orderbook-consolidator/`
  input (per-level delta, subject `price-level-event`). Superseded by the aggregator reading job 5's
  full books directly (`order_book_snapshot.avsc` → `aggregated_order_book_event.avsc`). Its
  `warmup.sh` registration is **kept** (same as `orderbook_event` above) so the deprecated standalone
  consolidator stack stays runnable for reference; drop it only if that stack is deleted.
