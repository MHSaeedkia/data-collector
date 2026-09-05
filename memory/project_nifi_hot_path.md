---
name: project-nifi-hot-path
description: Why the NiFi per-exchange ingest path stalled at ~100 msg/s and the 2026-08-31 redesign that removed the per-message Redis call and the Jolt recompile
metadata:
  type: project
---

**Symptom (2026-08-31, reported on the BYBIT group but true of all 9):** at ~5000 msg/s the websocket path collapsed to ~100 in/out and **stayed** collapsed. It only recovered by emptying the queue or stop/starting the downstream processor — i.e. a stuck state, not merely slowness. The queue built up behind `FetchDistributedMapCache`, which is why that processor looked guilty.

**Two real causes, both on the hot path, both since removed:**

1. **`JoltTransformJSON` with Expression Language in its spec.** The spec was `{"id":"${uuid}","simulation":0}` with **Transform Cache Size 1**. NiFi resolves the EL first and caches the *compiled* transform keyed by the resolved spec text — and `${uuid}` differs per FlowFile, so the cache key is unbounded and **every single message paid a full Jolt chain parse+compile**. This was the actual bottleneck; the Fetch queue was just back pressure from it. Non-obvious because the spec *looks* static.
2. **`FetchDistributedMapCache` doing a Redis round-trip per message for a constant** (`bybit` → `exchange_id`). One network hop × 5000/s × 9 groups.

**Decision — the replacement (applied to ALL 9 exchange groups 2026-08-31):**

- Redis lookup → **`SimpleDatabaseLookupService` (`nifi-lookup-services-nar`) + `LookupAttribute`**. Reads `exchanges` (key col `name`, value col `id`) through the existing `DBCPConnectionPool`, held in an in-memory cache with `Cache Expiration` as the refresh knob. **The user explicitly chose dynamic-from-DB over a static parameter**, and postgres is a better source of truth for `exchanges.id` than a Redis key some other flow has to remember to populate. Cost per message drops from a network hop to a hashmap get. **DistributedMapCache has no cached variant** — `FetchDistributedMapCache` and `DistributedMapCacheLookupService` are both one Redis call *per FlowFile*, which is the whole problem; a cache-backed lookup service is the only way to be dynamic AND free.
- Jolt → **`ReplaceText`** in Regex Replace / Entire text, search `^\s*\{`, replacement `{"id":"${uuid}","simulation":0,`. The `default` operation was only prepending two keys to a JSON object; no JSON parsing is needed for that.
- **Run Duration `25ms`** on the two new processors. It batches N `onTrigger` calls into ONE session commit (the commit — FlowFile repo + provenance disk writes — is the cost, not the CPU), so ~5000 commits/s becomes ~40. Only for `@SupportsBatching` processors; **never on `PutSQL`/`PutWebSocket`/`Put*`** — a failed batch rolls back and replays, which duplicates external writes. Telling detail: every other hot-path processor in the original export was already at `25ms`; the only two at `0` were the two that turned out to be the bottleneck.

**⚠ Two silent-failure traps this design introduces:**

- `LookupAttribute`'s **`unmatched` must NOT flow downstream**. The attribute feeds `kafak_topic_raw_data` = `ex${exchange_id}-raw` (its only consumer — see [[kafka-topic-strategy]]); missing ⇒ publishes to `ex-raw`, a topic no Flink job reads. Route `unmatched`+`failure` to the same LogAttribute the old `not-found` went to.
- **A `ReplaceText` regex that does not match is not an error** — content passes through `success` unchanged, with no `id`. Job 1 then drops it as `dropped-no-id`, i.e. **100% data loss on that feed with nothing anywhere in NiFi showing an error** (see [[record-lineage]]). `^\s*\{` rather than `^\{` for leading whitespace, and `dropped-no-id` is the only real proof the change worked.
- Minor: Jolt's `default` added the keys only if absent; a prepend always adds them. Safe today (no bybit payload has a root `id`/`simulation`), but a future exchange field would give a duplicate key and last-wins parsers would take theirs.

**Rejected: `ReplaceTextWithMapping`.** Its map comes from a **local file** on the node (`Mapping File` + `Mapping File Refresh Interval`), not from a DB or Redis; it rewrites **content**, not attributes; and it scans the whole body per message. Wrong tool on all three counts.

**Correction worth keeping:** "run a `GenerateFlowFile` every 60 s to fetch the value into an attribute" does **not** work — attributes belong to one FlowFile and cannot reach the message FlowFiles. Only a parameter or a caching lookup service avoids the per-message hop.

**Still open / unverified:**

- No before/after throughput number was recorded, so the fix is applied but not measured here.
- **Redis connection pool sizing.** All Redis processors in all 9 groups share ONE `RedisDistributedMapCacheClientService` → one connection pool whose NiFi default is **Max Total 8, Block When Exhausted true, Max Wait 10 s**. The hot-path readers are gone, but `Wait`, the websocket-session `Put/FetchDistributedMapCache` and the conn-state keys still use it. If stalls recur, check that first.
- **Queue swapping was a hypothesis, never confirmed.** `nifi.queue.swap.threshold` defaults to 20000 while back pressure was already 10000, so it should not have been reached — *unless* `ConnectWebSocket` creates FlowFiles from the Jetty callback thread and bypasses back pressure entirely (unverified). The observable test: if the stuck queue sits at ~10000 there is no swapping; if it climbs to 50k+ there is.
- **NiFi has no thread-dump button in the UI** (Summary → System Diagnostics gives counts only). Use `docker exec <c> /opt/nifi/nifi-current/bin/nifi.sh dump /tmp/d.txt` (no filename ⇒ goes to `logs/nifi-bootstrap.log`), take 3 dumps ~5 s apart, and read only the `Timer-Driven Process Thread` entries. See [[project-nifi-https]] for the container setup.
