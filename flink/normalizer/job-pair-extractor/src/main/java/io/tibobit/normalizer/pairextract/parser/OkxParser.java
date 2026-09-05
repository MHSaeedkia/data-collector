package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.JsonNode;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * ex8 okx — TWO raw streams share the {@code ex8-raw} topic (sample-raw-data.md § ex8), exactly
 * as on ex5/bitget:
 *
 * <ul>
 *   <li><b>WebSocket {@code books-grouped} feed</b>: bitget-family {@code arg}/{@code action}/
 *       {@code data}-array envelope, true delta feed ({@code action: "snapshot" | "update"}).
 *       Market key = {@code arg.instId} (with a DASH: {@code BTC-USDT}); levels are [price, qty]
 *       string pairs, qty "0" = level delete (confirmed on wire). Sequence id = {@code data[i].ts}
 *       (STRING epoch millis — the only timestamp AND the ordering field) with jump 300;
 *       event time = the same ts.</li>
 *   <li><b>REST snapshot</b>: the depth endpoint's response, which NiFi tags with
 *       {@code "action":"snapshot"} and stamps with the market as a top-level {@code "pair"} field
 *       (the REST body carries no symbol of its own). Levels are FOUR-element string arrays
 *       ({@code [price, qty, "0", orderCount]}); {@link Levels#fromStringPairs} reads the first two
 *       and ignores the rest.</li>
 * </ul>
 *
 * <p><b>The discriminator is {@code arg}, NOT the shape of {@code data}.</b> ex5 can tell its two
 * streams apart because its REST {@code data} is an object while its WS {@code data} is an array;
 * here {@code data} is an ARRAY on both, and {@code action} reads {@code "snapshot"} on both. Only
 * the WS frame has an {@code arg}.
 *
 * <p><b>The REST snapshot is NULL-SEQ, and that is load-bearing.</b> The body carries BOTH a
 * {@code seqId} (okx's real book counter) and a {@code ts}, and neither can seed job 2's
 * {@code lastSeq} here:
 *
 * <ul>
 *   <li>{@code seqId} lives in a different number space from the {@code ts} the WS updates are
 *       sequenced by — order 1e9 against order 1e12 — so seeding it would make the very next
 *       update read as a ~1e12 forward jump: an instant {@code sequence_gap}. It becomes the right
 *       answer only once the WS side is sequenced by {@code seqId} too (todo.md).</li>
 *   <li>{@code ts} comes off the REST endpoint's clock, not the WS one. ex5 measured that mistake
 *       on a live feed: sequencing the REST body by its own {@code ts} left the next WS update
 *       inside the expected window only 9.9% of the time, so ~90% of resyncs re-gapped
 *       immediately.</li>
 * </ul>
 *
 * <p>Null hands job 2 the {@code baselinePending} bootstrap ex1/ex2/ex5 already take: order this
 * body by EVENT TIME, then let the first WS update after it adopt its own {@code ts} as the
 * baseline, so the two clocks are never compared. {@code data.ts} is still the event time — a real
 * timestamp, just not a comparable sequence.
 *
 * <p><b>Why the REST branch existing at all is the fix (found live 2026-09-05).</b> Without it
 * job 1 dropped the resync answer on the floor: no {@code arg} means {@code arg.instId} is null,
 * which discarded the whole frame. So a gap on ex8 was TERMINAL — job 2 emitted a reset, asked for
 * a snapshot, NiFi fetched one and published it, and job 2 never saw it. Every later update
 * dead-lettered as {@code awaiting_snapshot} and the market stayed dark until the job restarted.
 */
public class OkxParser implements RawExchangeParser {

    @Override
    public List<ParsedBookEvent> parse(byte[] payload) throws Exception {
        JsonNode root = Json.MAPPER.readTree(payload);

        // See the class javadoc: `arg` is the only field the two streams do not share.
        if (!root.has("arg")) {
            return parseRestSnapshot(root);
        }
        return parseWsFrames(root);
    }

    /**
     * The REST depth response. {@code code}/{@code msg} are not inspected: an error body carries no
     * {@code data} array of books, so the shape whitelist already discards it. A body NiFi never
     * stamped with {@code pair} is discarded too rather than emitted against an unknown market.
     */
    private List<ParsedBookEvent> parseRestSnapshot(JsonNode root) {
        if (!"snapshot".equals(root.path("action").asText())
                || !root.path("pair").isTextual()
                || !root.path("data").isArray()) {
            return List.of();
        }
        String market = root.get("pair").asText();
        List<ParsedBookEvent> events = new ArrayList<>();
        for (JsonNode book : root.get("data")) {
            if (!book.path("ts").isTextual() || (!book.has("asks") && !book.has("bids"))) {
                return List.of();
            }
            RawOrderBookEvent event = new RawOrderBookEvent(0, 0, "snapshot",
                    null, 0L, Long.parseLong(book.get("ts").asText()),
                    book.has("asks") ? Levels.fromStringPairs(book.get("asks")) : null,
                    book.has("bids") ? Levels.fromStringPairs(book.get("bids")) : null);
            event.setSimulation(Json.simulation(root));
            event.setSourceIds(Json.sourceIds(root));
            events.add(new ParsedBookEvent(market, event));
        }
        return events;
    }

    private List<ParsedBookEvent> parseWsFrames(JsonNode root) {
        String type = switch (root.path("action").asText()) {
            case "snapshot" -> "snapshot";
            case "update" -> "update";
            default -> null;
        };
        String market = root.path("arg").path("instId").asText(null);
        if (type == null || market == null || !root.path("data").isArray()) {
            return List.of();
        }
        List<ParsedBookEvent> events = new ArrayList<>();
        for (JsonNode book : root.get("data")) {
            if (!book.path("ts").isTextual() || (!book.has("asks") && !book.has("bids"))) {
                return List.of();
            }
            long ts = Long.parseLong(book.get("ts").asText());
            RawOrderBookEvent event = new RawOrderBookEvent(0, 0, type,
                    ts, 300L, ts,
                    book.has("asks") ? Levels.fromStringPairs(book.get("asks")) : null,
                    book.has("bids") ? Levels.fromStringPairs(book.get("bids")) : null);
            event.setSimulation(Json.simulation(root));
            event.setSourceIds(Json.sourceIds(root));
            events.add(new ParsedBookEvent(market, event));
        }
        return events;
    }
}
