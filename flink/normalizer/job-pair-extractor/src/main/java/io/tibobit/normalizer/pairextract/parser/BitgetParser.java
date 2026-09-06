package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.JsonNode;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * ex5 bitget — TWO raw streams share the {@code ex5-raw} topic (sample-raw-data.md § ex5):
 *
 * <ul>
 *   <li><b>WebSocket {@code depth} feed</b>: bitget's own {@code action}/{@code arg}/{@code data}
 *       envelope, market key = {@code arg.instId}, {@code data} an ARRAY of book objects whose
 *       {@code asks}/{@code bids} are [price, qty] STRING pairs.</li>
 *   <li><b>REST snapshot</b>: the depth endpoint's response, which NiFi tags with
 *       {@code "action":"snapshot"} and stamps with the market as a top-level {@code "pair"}
 *       field (the REST body carries no symbol of its own). {@code data} is a single OBJECT, the
 *       sides are spelled {@code a}/{@code b}, and their levels are JSON NUMBERS.</li>
 * </ul>
 *
 * <p><b>{@code action} cannot tell them apart</b> — it reads {@code "snapshot"} on both, the same
 * trap ex1/ex2 have. The discriminator is the shape of {@code data}: an object is the REST body,
 * an array is the WS frame.
 *
 * <p><b>REVISED 2026-08-22 — the snapshot-only assumption was WRONG and {@code seq} is gone.</b>
 * The WS feed moved from the {@code books50} channel to the price-GROUPED {@code depth} channel
 * ({@code arg.params.scale}), which is a true delta feed: {@code action: "snapshot" | "update"},
 * qty {@code "0"} = level delete. The old ordering field {@code data[i].seq} (and {@code pseq})
 * no longer exist on the wire; the replacement {@code checksum} is a CRC book-integrity value,
 * NOT monotonic and NOT usable as a sequence.
 *
 * <p>So the sequence id of a WS UPDATE is the inner {@code ts} — the STRING epoch millis that is
 * also the event time. bitget's is a wall clock on a variable cadence, so the event carries a wide
 * {@code sequenceJumpTolerance} and job 2 checks a window rather than an equality. See
 * {@link #JUMP_MS}. <b>ex5 is now the ONLY exchange sequenced by a timestamp</b> — ex8/okx used to
 * be the other one, until 2026-09-05 moved it to the {@code books} channel's real chained counter
 * (see {@link OkxParser}); a timestamp is a last resort, not a pattern to copy.
 *
 * <p><b>REVISED 2026-08-23 (2) — measured against the live dev feed, and both numbers were
 * wrong.</b> 4569 consecutive {@code ex5-raw} frames over 34 minutes, BTCUSDT only:
 *
 * <ul>
 *   <li><b>The WS feed sends NO snapshots at all</b> — 3538 updates, 0 {@code action:"snapshot"}
 *       frames. The REST endpoint is ex5's ONLY baseline source, which is what made the two
 *       mistakes below fatal rather than merely wasteful.</li>
 *   <li><b>The REST {@code data.ts} is not on the WS clock.</b> Against the update just before it
 *       it ranges −706..+662 ms and is BEHIND 57% of the time, and the update just after it landed
 *       inside the old 600 ± 10 window only <b>9.9%</b> of the time. Sequencing the REST body by
 *       its own {@code ts} therefore made ~90% of resyncs produce an instant false
 *       {@code sequence_gap}: snapshot accepted → gap → book emptied → snapshot requested →
 *       repeat, ~22 times a minute, with {@code control-plane} saturated by the loop.</li>
 *   <li><b>update → update is bimodal</b>: 93.2% inside 600 ± 10, plus a real cluster at
 *       725–775 ms. The old window dead-lettered that cluster too (~6.7 false gaps/minute).</li>
 * </ul>
 *
 * <p>Hence: the REST snapshot is now <b>null-seq</b>, taking the {@code baselinePending} bootstrap
 * ex1/ex2 give their REST bodies — job 2 orders it by event time and lets the first update after
 * it adopt its own {@code ts} as the baseline, so the two clocks are never compared. And the
 * update window widened to cover the measured distribution.
 */
public class BitgetParser implements RawExchangeParser {

    /**
     * Centre of the accepted update→update interval, in millis. NOT a cadence bitget publishes —
     * the measured distribution is bimodal (a 575–625 mass plus a 725–775 cluster), so this is the
     * midpoint of the band that covers it, {@code 650 ± 110} = [540, 760] = 99.83% of 3537 live
     * transitions. A genuinely missed tick is ~1200 ms and still falls outside, so gap detection
     * survives; 4 transitions in 34 minutes landed at 875–1149 ms and are indistinguishable from
     * one. The real divergence detector for this feed is the {@code checksum} CRC (todo.md).
     */
    private static final long JUMP_MS = 650L;

    /** Accepted drift either side of {@link #JUMP_MS} — a clock, not a counter. */
    private static final long JUMP_TOLERANCE_MS = 110L;

    @Override
    public List<ParsedBookEvent> parse(byte[] payload) throws Exception {
        JsonNode root = Json.MAPPER.readTree(payload);

        // REST body: `data` is the book itself. The WS feed always wraps its book(s) in an array,
        // so the shape of this one node is the only reliable discriminator.
        if (root.path("data").isObject()) {
            return parseRestSnapshot(root);
        }

        // Otherwise a WS depth frame — or noise, which parseWsFrames drops.
        return parseWsFrames(root);
    }

    /**
     * The REST depth response. {@code code}/{@code msg} are not inspected: an error body carries
     * no {@code data.a}/{@code data.b} arrays, so the shape whitelist already discards it.
     *
     * <p><b>Null sequence id, jump 0</b> — the ex1/ex2 REST treatment, and the fix for the resync
     * loop measured on 2026-08-23 (see the class javadoc). {@code data.ts} comes off the REST
     * endpoint's clock, not the WS one, so it can be neither ordered against a WS {@code ts} nor
     * used as the origin of the update window. Leaving it null hands job 2 the
     * {@code baselinePending} path instead: order this body by EVENT TIME, then let the first WS
     * update after it adopt its own {@code ts} as the baseline. {@code data.ts} is still the event
     * time — it is a real timestamp, just not a comparable sequence.
     */
    private List<ParsedBookEvent> parseRestSnapshot(JsonNode root) {
        JsonNode data = root.get("data");
        if (!"snapshot".equals(root.path("action").asText())
                || !root.path("pair").isTextual()
                || !data.path("a").isArray() || !data.path("b").isArray()
                || !data.path("ts").isTextual()) {
            return List.of();
        }
        long ts = Long.parseLong(data.get("ts").asText());
        RawOrderBookEvent event = new RawOrderBookEvent(0, 0, "snapshot",
                null, 0L, ts,
                Levels.fromNumericArrays(data.get("a")),
                Levels.fromNumericArrays(data.get("b")));
        event.setSimulation(Json.simulation(root));
        event.setSourceIds(Json.sourceIds(root));
        return List.of(new ParsedBookEvent(root.get("pair").asText(), event));
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
                    ts, JUMP_MS, ts,
                    book.has("asks") ? Levels.fromStringPairs(book.get("asks")) : null,
                    book.has("bids") ? Levels.fromStringPairs(book.get("bids")) : null);
            event.setSequenceJumpTolerance(JUMP_TOLERANCE_MS);
            event.setSimulation(Json.simulation(root));
            event.setSourceIds(Json.sourceIds(root));
            events.add(new ParsedBookEvent(market, event));
        }
        return events;
    }
}
