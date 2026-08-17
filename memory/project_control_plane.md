# Control Plane — job 2 asks NiFi for a snapshot

Branch `feat/control-plane` (teammate's work, reviewed 2026-08-16; e2e coverage added
2026-08-17). The data plane carries order book events forward; the control plane carries one
command backward, to the collector.

## What it is

When job 2 decides a market's stream can no longer be trusted, it puts a command on a shared
Kafka topic `control-plane` for NiFi to consume, asking it to re-send a snapshot for that
`(exchange_id, pair_id)`. NiFi publishes the fresh snapshot to `ex{id}-raw` as usual, and the
pipeline recovers through the normal snapshot path. Nothing new downstream — job 2 is the only
producer, NiFi the only consumer.

This closes the loop that `type="reset"` opened: before it, a gap emptied the exchange's book
(see [[type-validator]], [[aggregator]]) and the market stayed dark until the exchange happened
to send a snapshot of its own. Delta-only feeds may never do that.

## Decisions

**Plain JSON, not Avro.** The only topic in the platform that is not Confluent-framed. NiFi
consumes it with a JSON processor and the payload is three fields, so a registered schema would
buy a version bump ritual and nothing else. The cost is real and accepted: the four Avro
subjects are enforced by the registry, this one is enforced by convention plus the e2e decoder.
Wire shape, fixed:

    {"action": "snapshot_request", "payload": {"pair_id": N, "exchange_id": N}}

**One shared topic, not one per market.** The target is in the record, so a per-pair topic
family would multiply topics for a stream that is nearly always empty. `scripts/warmup.sh`
creates it unconditionally rather than from the `pairs` query — NiFi needs it regardless of what
is subscribed. 1h retention: a command that old has been overtaken by whatever happened since.

**Keyed `{exchange_id}|{pair_id}`** even though the topic is single-partition, so ordering per
market survives if it is ever repartitioned.

**Once per EPISODE, not once per rejected event.** A `snapshotRequested` ValueState per key
guards it. After a gap every subsequent update also rejects (`awaiting_snapshot`), and one
request per rejected update would flood NiFi for as long as the feed keeps talking. The flag
clears on the three branches that actually resolve the condition — a sequenced snapshot, a
null-seq (ex1/ex2 REST) snapshot, and the first update that adopts a baseline after one — so the
NEXT gap is a new episode and does ask again.

**Triggers are `no_baseline` and `sequence_gap` only.** Not `stale_or_duplicate` (a replayed
sequence is a duplicate, not a hole — the book is intact) and not `out_of_order` (an old
snapshot arriving late; the newer book is already correct). Asking for a snapshot only makes
sense when the book is actually untrustworthy.

## Open, NOT resolved here

- **The NiFi side does not exist in this repo** (`nifi/` is a Dockerfile). Nothing consumes
  `control-plane` yet. Whether NiFi can produce an on-demand snapshot for the DELTA feeds where
  gaps actually happen — ex6/bybit, ex8/okx, which may need a WS resubscribe rather than a REST
  call — is unanswered and is the question that decides whether the feature works at all.
- **Cold start is a thundering herd.** No checkpointing, so after a job restart every delta-feed
  key hits `no_baseline` on its first update and asks at once: one command per subscribed
  (exchange, pair), all within seconds. NiFi needs to expect that.
- **`ControlCommand` carries no `simulation` flag and no `id`/`source_ids`**, unlike every
  record on the data plane ([[simulation-flag]], [[record-lineage]]). So a simulated gap asks
  for a real snapshot, and there is no way to trace which event caused a request. The e2e suite
  runs entirely on `simulation: 1` sources, so a harness run against a stack wired to a live
  NiFi would fire real snapshot requests.
- **Name collision**: `control-plane` is already the market subscribe/unsubscribe HTTP API
  (`markets/README.md`, `BASE_URL=http://localhost:8081/control-plane`). Same words, unrelated
  thing.

## e2e (2026-08-17)

`want_control_commands` on `Scenario`, asserted on every scenario. **Nil means "no command was
sent", never "skip"** — that is what makes a spurious request on a healthy feed fail, and it is
why 33 scenarios assert the control plane for free without declaring anything. Eight existing
scenarios ending on `no_baseline`/`sequence_gap` now declare the one command they produce.

Two new scenarios, `data_control.go`, grouped by feature rather than by exchange because what
they exercise is the episode rule, not an exchange's wire quirks — and a single episode cannot
show it. Both break the book, watch the request go out, feed back the snapshot NiFi would have
sent, and then break it again to prove the request re-arms. They differ in the resync path,
which is the branch that clears the flag: `42` re-syncs with a sequenced snapshot, `43` with a
null-seq REST snapshot whose offset the next delta adopts.

The Kafka key is checked structurally and then stripped before the literal comparison, the same
shape as the lineage checks — it is derived from the payload, so a scenario declaring it would
only restate its own ids. The decoder uses `DisallowUnknownFields`: with no registered schema,
a field renamed on the producing side has nothing else to fail against.

**Verified live 2026-08-17** over `-serve`, unlike most of the suite: 42 PASS (8 snapshots / 4
rejections / 2 commands), 43 PASS (5 / 3 / 2), and `30-ex6-snapshot-then-deltas` re-run as the
negative control read 0 commands. So the counts are real, not a vacuous match, and the
normalizer half of the feature does what it claims — one request per episode, re-armed by a
resync, silence on a healthy feed. What is still unverified is everything past the topic: no
NiFi flow consumes it.
