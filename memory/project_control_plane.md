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

**Avro on the Schema Registry, subject `control-command`** (schema `schemas/control_command.avsc`,
added 2026-08-18). This REVERSES the branch's original decision, which was plain JSON with the
shape `{"action":..., "payload":{pair_id, exchange_id}}` — the one topic in the platform outside
the registry, justified at the time by "NiFi reads it with a JSON processor and it is three
fields". Two things changed that: lineage and `simulation` went onto the command (below), so it
is no longer three fields, and a topic outside the registry has nothing but convention enforcing
it. `ControlCommandSerializer` is now the same shape as the four data-plane serializers — fetch
the write schema from the registry at first use, never a bundled copy, lazily on the task because
the Avro serializer is not Serializable. The payload nesting is gone; the record is flat.

**Two registration paths, and only one of them is the real one.** The e2e harness registers
every `schemas/*.avsc` and derives the subject from the filename, so a new schema works there the
moment the file exists. `scripts/warmup.sh` names each subject explicitly and does NOT, so a
schema can pass the whole e2e suite while job 2 dies at the first gap for anyone who set the
stack up from the README — which is what happened for two days after the Avro switch. **A green
e2e run says nothing about `make warmup`.** Add the `register_schema` line too.

The schema field is `simulation`, matching the other four subjects and [[simulation-flag]] — it
shipped once as `simulation_id`, which is both inconsistent and wrong (it is a 0/1 flag, not an
id). Worth catching before a subject has consumers, since renaming after that is an evolution
event.

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

**The command carries `simulation`, `id` and `source_ids`** (2026-08-18), closing what was the
loudest gap in the review: a simulated gap used to ask for a real snapshot, and no request could
be traced to the event that caused it. See the defect note below — the lineage is currently
inherited from the gap event rather than derived, which is not what [[record-lineage]] means.

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
- **Name collision**: `control-plane` is already the market subscribe/unsubscribe HTTP API
  (`markets/README.md`, `BASE_URL=http://localhost:8081/control-plane`). Same words, unrelated
  thing.

## Lineage on the command is DERIVED (fixed 2026-08-18)

The command lands on a topic, so it mints its own id and names the update that triggered it as
its single parent — `Lineage.newId()` / `List.of(event.getId())`, the same two lines `reject()`
and `emitReset` already use. It briefly shipped inheriting them instead (`id = event.getId()`,
`source_ids = event.getSourceIds()`), which is the failure `Lineage`'s own javadoc warns about:
every field still holds a well-formed uuid, so nothing that merely checks "is it set" notices.
What it actually cost was the trace — the command reused an id the dead-letter envelope already
carries, and named a grandparent, so no request could be tied to the event that caused it.

One gap event is therefore the parent of THREE records at once: the reset marker, the dead-letter
record, and the command. That fan-out is expected and is not a reason to inherit.

`simulation` is carried, not derived — it describes the data, not the record. A gap in simulated
data must not make NiFi call a real exchange, and the whole e2e suite feeds `simulation: 1`.

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
only restate its own ids. `id`/`source_ids` are stripped the same way and checked structurally
first — the parent is a raw event the harness never reads back, so only the shape is assertable:
a minted id, unique across the stream, exactly one parent, and the parent not equal to the
command's own id (which is the check that would have caught the inherited-lineage bug).

`simulation` IS declared and compared literally, like it is on a snapshot, and the server rejects
a declared command that does not say 1 — every fixture in the suite feeds simulated data, so a
command that dropped the flag would otherwise pass unnoticed.

The decoder used `DisallowUnknownFields` while the topic was plain JSON, since a field renamed on
the producing side had nothing else to fail against. The registry does that job now, so it goes
through `textual()` like the other four and the flattening is gone.

**Verified live 2026-08-17** over `-serve`, unlike most of the suite: 42 PASS (8 snapshots / 4
rejections / 2 commands), 43 PASS (5 / 3 / 2), and `30-ex6-snapshot-then-deltas` re-run as the
negative control read 0 commands. So the counts are real, not a vacuous match, and the
normalizer half of the feature does what it claims — one request per episode, re-armed by a
resync, silence on a healthy feed. What is still unverified is everything past the topic: no
NiFi flow consumes it.

**Re-verified live 2026-08-18** on a stack rebuilt from scratch (`docker compose up --build` +
`scripts/warmup.sh` + all six jobs), after the Avro switch and the four fixes: identical counts —
42 = 8/4/2, 43 = 5/3/2, 30 = 3/0/0. Identical is the point: the encoding changed, the episode
rule did not.

The topic was also read directly rather than only through the assertions, since a defaulted field
and a carried one both compare equal to zero when the fixture happens to be zero. The two commands
from run 42 carried `simulation: 1`, two distinct minted ids, and one distinct parent each — so
the values are really on the wire, not defaults the harness agreed with.
