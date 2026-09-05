# Simulation flag (`simulation`)

**Added 2026-08-03 (user request).** An integer that NiFi puts on every raw payload and that every
job carries, unchanged, to the aggregated topics.

- `0` = live data (**also the meaning of "absent"** — an unflagged payload is live)
- `1` = simulation data
- anything else = **not yet defined**, and deliberately carried verbatim. No job clamps, validates
  or branches on the value. Job 1 does not get to decide what a future value means.

## Where it rides on the wire — TWO carriers, not one

This is the part that is easy to get wrong.

- **ex1, ex2, ex4, ex5, ex6, ex8** — payload root is a JSON **object**, so NiFi injects
  `"simulation": N` as a **root field**, exactly the way it already injects `pair` on the ex1/ex2
  REST snapshots.
- **ex3/wallex** — payload root is a JSON **array**, so there is no root to inject into. NiFi
  appends it as a **trailing third element**:
  `["{market}@{side}", [levels…], {"simulation": 1}]`.
  `WallexParser` accepts 2 **and** 3 elements (2-element = pre-flag frame = 0) and drops 4+.

**The first design attempt assumed a root field everywhere and wrote ex3 off as "always 0".** That
was wrong — the user supplied the 3-element frame. If a future exchange has a non-object root, ask
where NiFi puts the flag rather than assuming it cannot carry one.

`Json.simulation(carrier)` in the pair-extractor takes *whichever node holds the field* and reads
`path("simulation").asInt(0)`, so a missing node is safe to pass in. Each parser passes its own
carrier: the root for six of them, `root.path(2)` for wallex.

## Why the aggregated flag is PER LEVEL

Job 6 unions across exchanges, so one aggregated record can mix a live exchange and a simulated
one. A record-level flag would be a lie. `AggregatedLevel` therefore carries
`exchange_id, simulation, price, quantity` — the flag is attached for the same reason `exchange_id`
is. (This was the user's explicit framing of the requirement.)

## Per-job behaviour

| Job | What it does |
| --- | --- |
| 1 pair-extract | Reads it off the payload (see carriers above) onto `RawOrderBookEvent` |
| 2 type-validate | Passes through; **the synthetic `reset` marker is built fresh, so it explicitly inherits the gap event's flag** — otherwise emptying a simulated exchange's book would emit a "live" record |
| 3 rebase / 4 precision | Nothing to do — both mutate the event in place and forward the same object |
| 5 book-build | Stamps the emitted book with the flag of the **event that produced it**. Deliberately NOT in `MapState`: it is a property of the feed, not of a price level, and a feed does not switch mid-stream. So it is last-event-wins, not sticky book state |
| 6 aggregate | `SnapshotSplitter` stamps every level with its snapshot's flag; the union and sort carry it along untouched |

## Operational gotcha — RE-REGISTER ALL FOUR SUBJECTS

All four `schemas/*.avsc` gained the field (`aggregated-order-book-event`,
`raw-order-book-event`, `order-book-snapshot`, `rejected-order-book-event`). The serializers fetch
the write schema from the registry, so **a stale registered subject makes the Avro sink throw on
the unknown field** — the same trap that bit `pipeline_timings` three separate times (see
[[pair-extractor]], [[type-validator]], [[book-builder]]). Re-run `scripts/warmup.sh` (or the e2e
harness's `RegisterDir`) and resubmit the jobs.

The field is `"default": 0` on every schema, so old records still read.

## Coverage

- `SimulationFlagTest` (job 1) is the single place the cross-parser rule is tested — it is not
  per-exchange wire format, so it is not repeated across the seven parser tests. 22 cases.
- Plus: reset-inherits (job 2), stamp/default/latest-event-wins/reset (job 5), per-level through
  the union (job 6), Avro round-trip incl. an undefined value (common).
- **Every e2e example sets `simulation: 1`** (user instruction) — 177 source payloads, 125 wanted
  snapshots, 215 wanted aggregated levels. So a parser that forgets the flag fails e2e immediately.
- `web/` carries it through `wireLevel` → `domain.RawLevel` → `domain.Level` → the WebSocket JSON.
  **The browser UI does not render it** — plumbed only, not displayed.

Related: [[avro-schema]], [[pair-extractor]], [[type-validator]], [[book-builder]], [[aggregator]],
[[e2e-harness]], [[orderbook-web]].
