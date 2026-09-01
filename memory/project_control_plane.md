# Control Plane — job 2 asks NiFi for a snapshot

Landed on `main` 2026-08-18 via `feat/control-plane` (teammate's work, reviewed 2026-08-16;
e2e coverage added 2026-08-17, Avro switch + four review fixes 2026-08-18). The data plane
carries order book events forward; the control plane carries one command backward, to the
collector.

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

**Once per EPISODE, not once per rejected event** (and then once per retry interval for as long
as the episode lasts — see the redesign below). A `resyncRequestedAt` ValueState per key guards
it. After a gap every subsequent update also rejects (`awaiting_snapshot`), and one
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

## The resync deadlock — a rejected snapshot silenced the key forever (fixed 2026-08-19)

**Symptom the user reported: "when we have a gap it does not get a snapshot, and sometimes no
command is sent at all."** Both halves had one cause.

The gap branch sets `awaitingSnapshot` and `snapshotRequested` together, and **neither ordering
guard cleared them on rejection** — both `reject(...); return;` before touching state. So if the
snapshot that came back to resolve the episode was rejected, the key wedged:

- every later update returned at the `awaitingSnapshot` reject;
- `requestSnapshotOnce` is gated *only* on `snapshotRequested`, still true → **no further command,
  ever**;
- worst of all, `lastEventTime` is written in exactly ONE place, inside `emit()`. A rejected event
  never reaches `emit()`, so the comparison the null-seq guard uses **can never advance** — every
  subsequent REST snapshot failed the identical stale check. Self-reinforcing: the guard that
  rejected the resync was the guard that could never afterwards be satisfied.

The market stayed dark until the job restarted, and no alert fired because everything looked
"normal" — a dead-letter topic filling with `awaiting_snapshot` is what a *working* held stream
looks like too.

**Why ex1/ex2 hit it hardest:** their resync snapshot is the null-seq REST one, whose `event_time`
comes from a different clock than the WS Centrifugo deltas that set `lastEventTime` (the same
field that already carries two different wire *types* within ex2 — see [[pair-extractor]]). Any
skew where REST trails the newest delta trips it. ex6/ex8 hit the same wall via `seq <= lastSeq`.

**Fix — the guards are suspended while a request is outstanding.** `resyncOutstanding()` returns
`snapshotRequested`, and both guards are `!resyncOutstanding() && <old condition>`. Rationale: the
guards exist to stop an OLD snapshot overwriting a GOOD book, and while a request is outstanding
there is no good book — the gap already emitted a `RESET` that emptied it downstream. **Accepting
a stale resync snapshot is strictly better than deadlocking**: an old book beats no book, and the
next update either re-anchors the baseline or gaps again and asks again. Gating on
`snapshotRequested` rather than `awaitingSnapshot` is deliberate — it also covers the `no_baseline`
episode, where a request is outstanding but `awaitingSnapshot` was never set.

**A "clear the flag on rejection" backstop was considered and deliberately NOT written.** Every
path that clears `awaitingSnapshot` also clears `snapshotRequested`, so once the exemption is in,
a snapshot can only be rejected when no request is outstanding — the clear would be provably
unreachable. Dead code, so it was dropped.

3 regression tests. TWO of them fail with the fix neutralised (`resyncOutstanding()` forced to
`false`); the third, `guardsStillApplyWithoutAnOutstandingRequest`, is a NEGATIVE control and
passes either way by design — it exists to prove the exemption did not disable the guards in
steady state. Re-checked 2026-08-22; the "each verified to FAIL" this note used to claim was
wrong. The 31 pre-existing tests all passed against the bug — they only ever covered a snapshot
that was ACCEPTED, never one rejected while a request was outstanding, which is exactly the hole.

## Re-asking, and the REDESIGN that replaced the timer (2026-08-22)

**The requirement:** one command per episode is not enough. If the command is lost, the collector
is down, or nothing is consuming `control-plane`, an episode that only ends on an ACCEPTED
snapshot means the market stays dark until the job restarts. The request has to repeat.

**The first implementation was a processing-time timer, and it was the wrong mechanism.** Worth
writing down WHY, because "re-ask periodically" reads like the textbook case for a timer:

- `onTimer` is handed no event, so everything the command needs had to be smuggled into state —
  `pendingSimulation`, `pendingSourceId` — or parsed back out of the key string, which coupled the
  function to `ExchangePairKey`'s format and made a wrong key selector a job crash inside a timer
  callback rather than a rejected record.
- Timers were registered per episode and cancelled nowhere. A stale timer only dies if it happens
  to fire while nothing is outstanding, so **two episodes inside one retry interval left two live
  chains, each re-arming forever** — measured at a steady 2 commands per interval instead of 1,
  growing by one chain per overlapping episode. Flappy delta feeds (ex6/ex8) are exactly what
  produces close-together episodes. It silently broke the once-per-episode invariant the whole
  feature rests on.
- The fallback `sourceId == null ? ""` put `source_ids: [""]` on the topic — an untraceable
  command that passes every "is the field set" check.

**What replaced it: the ask is driven by the rejections themselves.** Both untrustworthy branches
call `askForSnapshot(event, ctx)` on EVERY event they turn away, and the ask is suppressed unless
`snapshotRetryMs` has elapsed since the last one. The first ask and the hundredth are the same line
of code — the only question either time is "have we asked recently?".

That deletes the timer, `onTimer`, `scheduleRetry`, `requestSnapshotOnce`, the key parse and both
pending fields. A rejection arrives exactly when a retry is worth sending and carries the exchange,
pair, simulation flag and parent id already. Each command now names the update dead-lettered
alongside it (retries included) instead of re-naming the original gap, so every command points at
a record that is really on the rejected topic.

**The trade-off, taken deliberately:** a market that goes SILENT after the gap gets no retries,
where a timer would keep asking. Accepted — a feed sending nothing cannot be re-synced by anything
we put on the topic, and the moment it speaks its first update is rejected and asks. Both triggers
are themselves updates, so the feed is alive by definition when an episode opens. If the collector
ever needs the silent case, a timer can be added ON TOP — but it must then cancel.

## `reason` on the command (2026-08-22, user request)

The command now says WHY, not just what: `reason` is `no_baseline` or `sequence_gap`, the same
vocabulary as `reject_reason` on the dead-letter topic. Avro field added with `default: ""`, so
registering it is a BACKWARD-compatible evolution — checked against the live registry before
registering (`/compatibility/subjects/control-command/versions/latest` → `is_compatible: true`,
then v2 = schema id 7).

**It costs no state either, for the same reason the rest of the feature does not.** `resyncReason()`
is `lastSeq == null ? NO_BASELINE : SEQUENCE_GAP` — the identical discriminator the reject reasons
already use one line apart. `lastSeq` only moves inside `emit()`, and while a resync is pending
every event is rejected instead, so it is FROZEN for the whole episode. That is what makes the
value stable across retries without storing it.

**A retry carries the reason that OPENED the episode, not `awaiting_snapshot`.** The update that
prompts a re-ask is dead-lettered `awaiting_snapshot`, but that is bookkeeping about a request we
already sent — it is not a reason to want a snapshot. The reason describes what the collector is
being asked to fix. Consequence for anyone reading a scenario: a declared reason lines up with the
FIRST reject of each episode in `WantRejects`, never with the holds after it, and
`awaiting_snapshot` can never appear on the topic (the e2e `validate` now 400s a scenario that
declares it).

Two mutations, each killing a different set of the 43 unit tests: reason hardcoded to
`sequence_gap` kills 3 (`noBaselineRequestsSnapshot`, `reasonDistinguishesTheTwoTriggers`,
`noBaselineEpisodeIsRetried`); a retry reporting its trigger's own reject reason kills 3
(`retryKeepsTheEpisodeReason`, `sequenceGapRequestsSnapshot`, `reasonDistinguishesTheTwoTriggers`).
The second mutation catching `sequenceGapRequestsSnapshot` is not a mistake in the test — inside
`askForSnapshot` the timestamp is written BEFORE the command is built, so `resyncPending()` is
already true even on the first ask of an episode. Worth knowing before adding anything else that
reads state in there.

e2e: `Reason` is DECLARED and compared literally, like `simulation` and unlike the lineage. It is
the first field that distinguishes two commands a scenario could otherwise not tell apart — 43's
two wanted commands were byte-identical before it. Verified live 2026-08-22 on 02, 03, 10, 11, 30
(negative control), 32, 33, 36, 38, 42, 43, 44, 45, and mutation-checked live by declaring 43's
second command `no_baseline`, which failed on `control-plane record 1`. The topic was also read
directly rather than only through the assertions — `{"action":"snapshot_request","reason":
"sequence_gap","exchange_id":8,...}` — since a defaulted `""` and a carried value both compare
equal when the fixture agrees.

**Deploying it needs the schema re-registered AND job 2 resubmitted, in that order.**
`ControlCommandSerializer` fetches the write schema lazily on first use and holds it, so a
long-running job 2 keeps v1 and `GenericRecordBuilder.set("reason", …)` throws on a field its
schema lacks. `scripts/warmup.sh` needs no edit — it registers from the file.

## One field, not three — the state collapse (2026-08-22)

`awaitingSnapshot` and `snapshotRequested` were never independent. Every branch that set one set
the other; they diverged only on the `no_baseline` path, and that path is already discriminated one
line earlier by `lastSeq == null`. **Two flags for one condition is what made the deadlock
invisible** — it created states that should not exist, and the code reached one.

Both are now a single `ValueState<Long> resyncRequestedAt`, whose value is the processing time of
the last ask and whose nullness is the condition:

- `null` → the stream is trusted, nothing outstanding.
- non-null → the book cannot be trusted, we have asked for a replacement, and here is when.

The reject reason is derived, not stored: `lastSeq == null` → `no_baseline`, otherwise
`awaiting_snapshot`.

**The whole control plane therefore costs ZERO net state.** Field count is back to the four the
function had before the feature existed (`51be8dc`) — `awaitingSnapshot` (Boolean) simply became
`resyncRequestedAt` (Long) and absorbed the feature. 7 `ValueState` fields → 4, 25 methods → 19.

**Mutation-checked three ways, each killing a different set:**

- `resyncPending()` → `false` kills 6 tests, three of them pre-existing data-plane ones. That is
  the collapse proving itself: the field is load-bearing for both planes now, so ordinary gap/reset
  tests protect it too.
- ask once per episode and never re-ask → kills exactly the 4 retry tests, and none of the three
  negative controls.
- ask on every rejection with no interval → kills the flood guards:
  `rejectionsInsideTheIntervalDoNotReAsk`, `overlappingEpisodesDoNotMultiplyTheAskRate`, and the two
  pre-existing once-per-episode tests.

41 unit tests green. `overlappingEpisodesDoNotMultiplyTheAskRate` exists specifically to pin the
timer defect closed: two episodes inside one interval, then the ask rate asserted at one per
interval rather than one per episode ever opened.


## e2e for the DEADLOCK, not just the happy loop (44/45, added and verified live 2026-08-22)

42 and 43 both re-sync with a snapshot the ordering guards were always willing to accept — ex6's
is ahead of the pre-gap offset, ex1's is newer than the last accepted delta. **So the suite that
was written for this feature never once exercised a REJECTED resync, which is the entire bug
fixed on 2026-08-19.** Two scenarios close that:

- `ControlEx6StaleResyncAccepted` (numbered `46-…` as of 2026-08-23; was `44-`, then `45-`
  — the ex5 block has grown twice, so use the Go identifier, not the number) — the sequenced guard. Resync arrives at `u = 250`,
  below the pre-gap baseline of 301.
- `ControlEx1LaggingRestResync` (numbered `47-…` as of 2026-08-23) — the event-time guard. The REST resync is stamped
  08:00:00 while the last accepted delta was 08:00:02, so its snapshot's `event_time` goes
  BACKWARDS relative to the reset before it. That backwards step is declared in `WantSnapshots`
  and is the shape a real clock skew has.

Each asserts both halves together: the resync came out as a SNAPSHOT rather than a rejection, and
the episode actually closed — proven by a later gap opening a new one and asking again. Ending
each on a clean recovery keeps `WantAggregated` meaningful.

**Both verified non-vacuous by mutation against the LIVE stack**, which is the part worth
repeating: with `resyncOutstanding()` forced to `false`, 44 reads 4 snapshots instead of 7 and 45
reads 5 instead of 8 — the middle of the run vanishes into the dead-letter topic and the market
only recovers at the final ahead-of-baseline snapshot. Reading `control-plane` directly during
that mutant run showed **1 command instead of 2**: the second gap is rejected `awaiting_snapshot`
rather than `sequence_gap`, so it never reaches `requestSnapshotOnce`. That is the user-reported
symptom ("sometimes no command is sent at all") reproduced end to end.

Live results 2026-08-22, all PASS: 42, 43, 44, 45, and `30-ex6-snapshot-then-deltas` re-run as the
negative control. `Ex1StaleRestReplay` and `Ex8StaleDuplicate` remain the other half of the
control — the same two guards with NO request outstanding, where a stale snapshot must still be
rejected and no command may be sent.

**Re-asking still has no e2e coverage**, for the same reason as before the redesign:
`SNAPSHOT_RETRY_MS` is read from the JobManager's environment and is set NOWHERE in
`docker-compose.yml`, so every run uses the 5-minute default, and a scenario finishes in ~20s
against ~80s of read windows. That is also why the feature does not make the suite flaky. Covering
it needs the env var wired into the jobmanager service AND a per-scenario override — a short
global interval would make every scenario ending on an unresolved episode (02, 03, 10, 11, 33, 36,
38 …) emit extra commands mid-verification. The unit tests carry it instead, and can, because
`askForSnapshot` reads the clock through `timerService()` which the harness drives.

**Full-suite result after the redesign (2026-08-22): all 45 scenarios PASS**, run in batches. One
caveat worth knowing for the next session: a single 45-scenario pass in one process is NOT
reliable on this machine — `12-ex2-noise-frames` failed once with 9 dead-letters where it wants 0
(the shape of records leaking from the previous scenario on the same ex2/p1 topics), and from
scenario 21 the JobManager crashed and every later scenario failed on `connection reset`. Disk was
fine (48Gi). 12 then passed three times in a row, including immediately after 11. **Run the suite
in batches of ~10 and treat a lone failure mid-run as suspect until it reproduces in isolation.**

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

## 2026-08-23 — ex5's resync answer now has its own wire shape

The snapshot NiFi is being asked for is, on ex5/bitget, the **REST depth body** — a different
shape from the WS frames the same topic carries ([[project_pair_extractor]]). Nothing in job 2
changed: job 1 hands it up as an ordinary sequenced snapshot, so `resyncPending()` exempts it from
the `stale_or_duplicate` guard and it clears the episode exactly like any other.

What IS worth knowing here: because the user chose to sequence that REST body by its own
`data.ts` (jump 600 ± 10) rather than null-seq, **an ex5 resync only truly succeeds if the next WS
update lands 590–610 ms after the REST book's timestamp**. If it does not, the update gaps
immediately, the book is emptied again and a fresh command goes out — reset → request → snapshot →
gap, the loop flagged in todo.md. The deadlock fix means the market will keep *asking* rather than
going silently dark, so this degrades into a request loop rather than a black hole, but it is the
ex5-specific failure mode to look for on the live feed. `31-ex5-rest-snapshot-resync` is the e2e
scenario that covers the happy version of this path.

## Silence closes the last gap (2026-08-31) — staleness-triggered resync

The deliberate hole above — *"a market that goes SILENT after the gap gets no retries"* — is now
closed, on user request. Every key holds ONE processing-time deadline at `lastArrival +
staleness_threshold_seconds`; passing it emits the same `RESET` a sequence gap does (so the
exchange drops out of the aggregated view instead of serving a frozen book) and asks for a snapshot
with `reason: "stale"`, through the SAME `askForSnapshot` suppression window every other reason
uses.

**No new DB column.** `exchange_markets.staleness_threshold_seconds INT NOT NULL DEFAULT 60`
already existed and was already read by [[staleness-exporter]], which only ever *reported* it. Job
2 now acts on the same number. One knob drives both "warn a human" and "auto-resync"; that coupling
is the accepted cost of not adding a column and not hand-running another `ALTER TABLE` on the
provisioned server DB.

### ⚠ THE SCOPE WAS CUT BACK — read this before "restoring" anything

The first implementation (commit `d78d84d`) ALSO detected markets that had **never** sent
anything, as `reason: "no_data_received"`. The user asked for a second opinion before it ran, and
**it was removed.** Do not add it back without re-reading this.

The two conditions look like one feature and are not:

- **`stale`** is derived from job 2's OWN stream. `lastArrivalMs` is a fact it already holds, and
  the remedy — inject a `RESET` into the keyed stream — is something ONLY job 2 can do. It belongs
  here.
- **`no_data_received`** is not. Job 2 cannot know a market exists if it has never heard from it,
  so the condition can only be found by importing the subscription roster from Postgres. That is
  the question *"is the world the shape we expected?"*, which is monitoring, not validation.

Three arguments settled it:

1. **It was a duplicate.** `StalenessThresholdLoader` and `lpa-staleness-exporter` ran the *same
   query* (`exchange_markets WHERE status = 'subscribe'`) against the same column to answer the
   same question. [[staleness-exporter]] documents that this exact duplication already caused a
   bug once — its `NORMALIZER_STAGES` drifted from `warmup.sh` and every DB-derived series became a
   phantom pinned at `stale=1`. A never-fed market almost certainly already reads `stale=1` there.
   *(Believed, from that note — not re-verified against `exporter.py`. Confirm before relying on it.)*
2. **The remedy did not remedy.** A `snapshot_request` cannot start a subscription that was never
   wired up. The loader's own javadoc already used this reasoning to exclude the pending statuses.
   And the `simulation` flag was provably WRONG in this case (it sent 0/live because there was no
   event to copy it from) — the design saying out loud that it lacked the information it needed.
3. **It cost nearly all the complexity.** The `KeyedCoProcessFunction`, the second source, the
   `DataGeneratorSource` + `StalenessTickFanOut`, the `flink-connector-datagen` bundle, the
   **parallelism-1 ceiling on two operators**, a permanent N-ticks-per-poll keyed shuffle, and
   migrating all 53 rule tests to a two-input harness — every one of those existed ONLY for the
   never-received case. `stale` needs none of them, because the key already exists.

**The timer objection was real but narrow.** "A keyed function cannot register a timer for a key
that does not exist" is true, and it is an argument about the never-received case *only*. Once that
case is out of scope, a timer is the right mechanism.

### The timer, and how it avoids the defect that sank the first one

The 2026-08-22 redesign deleted an `onTimer` because timers were registered per episode and
cancelled nowhere, leaving several live chains each re-arming forever. This one is different by
construction:

- **`ValueState<Long> stalenessTimerAt` holds the single outstanding timer.** A timer is armed only
  when it is null, and it is cleared at the very top of `onTimer` before anything else. A key can
  never accumulate a second chain.
- **No per-event churn.** An arriving event does NOT cancel and re-register. If the deadline fires
  early, `onTimer` re-arms at `lastArrival + threshold` instead of judging — so detection still
  lands exactly on the deadline, at no timer cost per event.
- **The market's identity comes from its watch-list row, never from parsing the key string.**
  Parsing the key was one of the three specific defects of the old timer. That is why the lookup
  value is a `WatchedMarket` (exchange, pair, threshold) rather than a bare `Integer`.

**A market absent from the watch list is not watched at all** — no default threshold — so an
unsubscribe stops the asking on the next refresh with no resubmit. Guarded in BOTH `armSilenceTimer`
and `onTimer`; either guard alone absorbs the other's mutation, so only the combined mutation is
caught by a test.

**`lastArrivalMs` is stamped on EVERY arriving event whatever its verdict.** That is what "nothing
arrived" means: a key rejecting everything is alive and already re-asking on the rejection path, and
treating it as stale too would make one fault ask twice.

**The reset fires once per episode** (guarded on `resyncRequestedAt` having been null) while the ask
repeats.

**⚠ Parentless records.** A silence command has no trigger event, so `source_ids` is **empty**,
never `[""]`; `event_time` on the reset is processing time; and `simulation` comes from
`lastSimulation`. Because only markets that have spoken are watched, there is always a flag to copy
— the "never spoke sends 0" limitation of the first version is gone with the feature that caused it.

**⚠ Job 2 gained its first DB dependency**, the postgres driver shaded into the job jar (verified
present in the shaded jar, not assumed) with the same explicit `Class.forName` as
`ExchangeMarketsLoader`. `flink-connector-datagen` is NOT needed and was removed. The watch list
rides inside the operator via `RefreshingLookup` and is read by key — no second stream, no extra
shuffle, **no parallelism constraint**.

**No compose change.** `POSTGRES_URL`/`POSTGRES_USER`/`POSTGRES_PASSWORD` appear nowhere in
`docker-compose.yml`; jobs 3 and 4 run on the in-code defaults and job 2 now uses identical names
and defaults. One new knob, `REFRESH_INTERVAL_MS` (60 s). `STALENESS_POLL_MS` is gone — the poll
cadence WAS the tick stream; a timer needs no polling.

**`reason` needed no schema change** — a plain `string` with `default: ""`, so `stale` is free; only
the `.avsc` doc was updated, and re-registering is optional (docs only, not compatibility).
Contrast the `type` ENUM trap in [[type-validator]].

### Verification, including what it did NOT prove

Job 2 is **66 tests** (53 pre-existing + 13 staleness); the 53 pass unchanged, which is the evidence
that the rule set was not touched. Full normalizer build green, **282 tests across 7 modules**.
Mutations run against the new logic — killed: no one-timer guard, judging without the early re-arm,
reset on every deadline not once per episode, arrival never recorded, not clearing the spent timer,
and defaulting the threshold for an unwatched market.

**Two lessons about vacuous tests, both learned here the hard way:**

- The first "one timer chain, not one per event" test asserted on the OUTPUT and **passed under the
  mutation**. Duplicate chains are invisible downstream — the once-per-episode reset guard and the
  shared ask window absorb them completely. What they are not is free: 21 chains is unbounded timer
  *state* that grows with traffic. The test now asserts `harness.numProcessingTimeTimers() == 1`
  directly, which is the only place the invariant is observable.
- **One mutation still SURVIVES and is worth remembering: advancing `lastEventTime` inside the
  silence reset changes nothing observable**, because asking opens a resync episode and the ordering
  guards are suspended while one is outstanding, so the returning event is accepted and overwrites
  the poisoned value before any guard reads it. Not routing the reset through `emit()` is therefore
  defence in depth — **the exemption is the load-bearing part**, and it has been deleted by accident
  once already.

### VERIFIED LIVE 2026-09-01 (local docker stack)

Ran on the compose stack (kafka + postgres + schema-registry + jobmanager + taskmanager-1), jobs 1
and 2 submitted downstream-first. The seeded local DB has **only ex1/nobitex and all 50 rows
`unsubscribe`**, so the test subscribed ex1/BTCUSDT (`market_id 1`) with
`staleness_threshold_seconds = 20` before submitting — the row was restored to
`unsubscribe`/60 afterwards.

One nobitex REST snapshot (`action: "snapshot"`, `simulation: 1`) to `ex1-raw`, then silence:

- accepted through job 1 → job 2 at **10:12:35**;
- **10:12:56** (21 s) — `RESET` on `ex1-p1-type-validated-raw-flink`: `type: "reset"`, `asks`/`bids`
  **null**, `sequence_id` null, `event_time` = processing time, `pair_extract_*` null, and the two
  fields that matter — **`source_ids: []` (empty, NOT `[""]`)** and **`simulation: 1`** carried from
  `lastSimulation` rather than defaulted to 0;
- same instant — `control-plane`: `{"action":"snapshot_request","reason":"stale","exchange_id":1,`
  `"pair_id":1,"simulation":1,"source_ids":[]}`. The ids came from the **watch-list row**, which is
  the live proof that nothing parses the key string.

Then, over ~90 s of continued silence (4+ deadlines): **exactly 1 reset and 1 command**. Recovery
snapshot sent with an `event_time` **OLDER than the reset's** — **accepted, 0 rejects**, i.e. the
resync exemption holds when reached from the silence path (the 2026-08-19 deadlock, new direction).
21 s later a **NEW** episode opened (2nd reset + 2nd command) — and note the 2nd command appeared
despite `SNAPSHOT_RETRY_MS` = 300 s, because the resync had CLEARED `resyncRequestedAt`. Episode
boundaries work, not merely the suppression window.

**The unwatched-market rule was confirmed with a live key, not a missing one.** Pair 2 (BTCIRT,
`status = 'unsubscribe'`) was fed a snapshot so it held real keyed state in job 2, then left silent
for 2.5× the threshold: **nothing**. That is the mechanism that makes an unsubscribe take effect.

**The postgres driver loads on the cluster.** The job reaching RUNNING with both vertices up is
itself the proof — `RefreshingLookup.open()` propagates a failed initial load. Also confirmed from
the TaskManager classpath: **`flink-connector-datagen` is NOT on the cluster**, so the tick-stream
design really would have had to bundle it. One more line in the ledger against it.

**⚠ What this run did NOT prove.** (a) `RefreshingLookup` was only read at `open()` — the 60 s
refresh never fired, so "edit the threshold in Postgres and it lands without a resubmit" is still
unproven live. (b) Exactly ONE market was watched, so nothing exercised many markets going stale at
once. (c) Still no e2e coverage. (d) Only `stale` exists to test — the never-received case is out of
scope by design, and the assumption that [[staleness-exporter]] covers it is STILL unverified.

⚠ The counts recorded above this section (43 unit tests) were stale; it was 53 before this change.
