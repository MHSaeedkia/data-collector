package io.tibobit.normalizer.parse.parser;

import com.fasterxml.jackson.databind.JsonNode;
import io.tibobit.normalizer.model.ParsedBookEvent;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import java.util.List;

/**
 * ex6 bybit — TWO raw streams share the {@code ex6-raw} topic (sample-raw-data.md § ex6), the
 * same REST+WS split ex1, ex2 and ex8 have (ex5 had it too until 2026-09-07):
 *
 * <ul>
 *   <li><b>WebSocket feed</b>: bybit's own {@code topic}/{@code ts}/{@code type}/{@code data}/
 *       {@code cts} envelope, a true delta feed — {@code type: "snapshot" | "delta"} ("delta" →
 *       our "update"). Market key = {@code data.s}; sides are {@code b}/{@code a} string pairs.
 *       Sequence id = {@code data.u} with jump 1; event time = {@code cts}.</li>
 *   <li><b>REST snapshot</b>: the {@code /v5/market/orderbook} response, which NiFi tags with
 *       {@code "action":"snapshot"} and stamps with the market as a top-level {@code "pair"}.
 *       The book lives under {@code result}, not {@code data}. <b>Null sequence id</b> — see
 *       below.</li>
 * </ul>
 *
 * <p><b>The discriminator is clean here</b>, unlike ex5 as it then was (where {@code action} read
 * "snapshot" on both streams and only the shape of {@code data} separated them): the REST body
 * carries the book
 * under {@code result} and the WS frame under {@code data}, and the WS frame has no {@code action}
 * field at all. An error body is discarded by the same shape whitelist — bybit returns
 * {@code "result": {}}, which has no {@code a}/{@code b} arrays, so {@code retCode}/{@code retMsg}
 * are not inspected.
 *
 * <p><b>The REST snapshot is null-seq, jump 0 — its {@code result.u} is NOT on the WS counter.</b>
 * Measured across the 2026-08-24 captures: the REST body is 24.3 hours LATER than the WS pair yet
 * its {@code u} is 171,928,550 LOWER (38992362 vs 210920912). A monotonic counter cannot go
 * backwards, so the two are separate counters. <b>The reason is documented</b>, not merely
 * inferred: bybit's own REST reference says {@code result.u} "corresponds to {@code u} in the
 * <b>1000-level</b> WebSocket orderbook stream" (for spot and for contract alike), while this
 * feed subscribes to {@code orderbook.50}. Each depth topic carries its OWN {@code u}, so the
 * REST body's counter belongs to a stream we do not consume. Sequencing the REST body by its own
 * {@code u} would make the next WS delta read as a ~172M jump: instant false {@code sequence_gap}
 * → book emptied → snapshot requested → repeat. That is the live resync loop ex5 was burned by
 * (see {@link BitgetParser}), so ex6 takes the {@code baselinePending} bootstrap instead — job 3
 * orders this body by EVENT TIME and lets the first WS delta after it adopt its own {@code u} as
 * the baseline, so the two counters are never compared.
 *
 * <p>{@code result.seq} is likewise unusable, for the reason it always was on this exchange: it
 * moves 10 per {@code u} and is bybit-internal cross-topic metadata. Never use it.
 *
 * <p>Event time is {@code result.cts} — the matching-engine time, the same field the WS branch
 * reads, so both ex6 streams are on one event-time clock. The sibling {@code result.ts} (gateway)
 * and the top-level {@code time} (API round trip) are metadata.
 *
 * <p><b>{@code data.u == 1} is a SERVICE RESTART, and it is null-seq too.</b> Bybit documents it
 * outright: "Occasionally, you'll receive {@code "u"=1}, which is a snapshot data due to the
 * restart of the service. So please overwrite your local orderbook." The counter restarted, so
 * the frame's {@code u} is not comparable to the running one — sequencing it would make job 3
 * order 1 against a {@code lastSeq} in the hundreds of millions and dead-letter it
 * {@code stale_or_duplicate}, taking every following delta with it (u=2, 3, … are all
 * {@code <= lastSeq}) until the no-progress timer notices. So it takes the SAME null-seq route
 * the REST body takes: job 3's {@code baselinePending} bootstrap lets the next delta adopt its
 * own {@code u} as the fresh baseline.
 *
 * <p><b>The levels are kept.</b> This frame is a full 50-level book and the exchange's
 * instruction is to OVERWRITE with it, so it stays {@code type: "snapshot"} — job 6 replaces both
 * sides wholesale. Emitting it as an empty {@code reset} instead would clear the book and leave
 * only subsequent deltas, which carry just the changed levels, to refill it: a partial book for
 * as long as the quiet levels take to churn. Null-seq buys the re-anchoring; it costs no data.
 *
 * <p><b>No control command is emitted on this path</b>, deliberately — the exchange has already
 * sent the snapshot a resync would have asked for. Job 3's null-seq branch calls no
 * {@code askForSnapshot}, and its {@code resyncTrusted()} clears an outstanding request, so a
 * restart lands silently on the control plane instead of adding to it.
 *
 * <p><b>Levels are string pairs on BOTH streams</b> — no JSON-number hazard anywhere on ex6,
 * unlike ex4, whose levels are numeric literals.
 *
 * <p><b>Never subscribe ex6 at depth 1.</b> Bybit's level-1 feed is snapshot-only and re-pushes
 * every 3 s with <i>the same</i> {@code u}, which job 3 orders as {@code stale_or_duplicate} on
 * every quiet repeat. The {@code orderbook.50} topic this parser is written against has no such
 * behaviour.
 */
public class BybitParser implements RawExchangeParser {

    @Override
    public List<ParsedBookEvent> parse(byte[] payload) throws Exception {
        JsonNode root = Json.MAPPER.readTree(payload);

        // REST body: the book is under `result`. The WS feed always puts it under `data`.
        if (root.path("result").isObject()) {
            return parseRestSnapshot(root);
        }
        return parseWsFrame(root);
    }

    /**
     * The REST depth response. Both sides are required: this is a full book, never a per-side
     * snapshot, so a body missing either side is dropped rather than half-applied. The market key
     * is {@code result.s} — bybit's own symbol, which keeps the key derivation identical to the WS
     * branch; NiFi's injected {@code pair} is redundant here (ex1/ex2/ex8 need theirs because
     * those REST bodies carry no symbol at all).
     */
    private List<ParsedBookEvent> parseRestSnapshot(JsonNode root) {
        JsonNode result = root.get("result");
        String market = result.path("s").asText(null);
        if (!"snapshot".equals(root.path("action").asText())
                || market == null
                || !result.path("a").isArray() || !result.path("b").isArray()
                || !result.path("cts").isIntegralNumber()) {
            return List.of();
        }
        RawOrderBookEvent event = new RawOrderBookEvent(0, 0, "snapshot",
                null, 0L,
                result.get("cts").asLong(),
                Levels.fromStringPairs(result.get("a")),
                Levels.fromStringPairs(result.get("b")));
        event.setExchangeEventTime(event.getEventTime());
        event.setSimulation(Json.simulation(root));
        event.setSourceIds(Json.sourceIds(root));
        return List.of(new ParsedBookEvent(market, event));
    }

    /**
     * A WS frame. A delta may carry one side only: a MISSING key is a null side ("no report",
     * which job 6 leaves untouched), while a present-but-empty array is a real report. On an
     * update an empty array merges nothing, so the live feed's {@code "b": []} on a one-sided
     * delta is a no-op; only on a SNAPSHOT does it clear the side.
     */
    private List<ParsedBookEvent> parseWsFrame(JsonNode root) {
        String type = switch (root.path("type").asText()) {
            case "snapshot" -> "snapshot";
            case "delta" -> "update";
            default -> null;
        };
        JsonNode data = root.path("data");
        String market = data.path("s").asText(null);
        if (type == null || market == null || !data.path("u").isIntegralNumber()
                || !root.path("cts").isIntegralNumber()
                || (!data.has("a") && !data.has("b"))) {
            return List.of();
        }
        // The service-restart snapshot (see the class javadoc): its counter restarted, so it is
        // stamped null-seq and re-anchors through job 3's baselinePending bootstrap. Guarded on
        // the type as well as the value because bybit documents u==1 as snapshot data only; a
        // delta claiming u==1 is undocumented and is left to the ordinary rules.
        boolean serviceRestart = "snapshot".equals(type) && data.get("u").asLong() == 1L;
        RawOrderBookEvent event = new RawOrderBookEvent(0, 0, type,
                serviceRestart ? null : data.get("u").asLong(),
                serviceRestart ? 0L : 1L,
                root.get("cts").asLong(),
                data.has("a") ? Levels.fromStringPairs(data.get("a")) : null,
                data.has("b") ? Levels.fromStringPairs(data.get("b")) : null);
        event.setExchangeEventTime(event.getEventTime());
        event.setSimulation(Json.simulation(root));
        event.setSourceIds(Json.sourceIds(root));
        return List.of(new ParsedBookEvent(market, event));
    }
}
