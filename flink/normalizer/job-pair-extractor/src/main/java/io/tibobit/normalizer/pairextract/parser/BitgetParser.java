package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.JsonNode;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * ex5 bitget — {@code action}/{@code arg}/{@code data}-array envelope, market key = arg.instId,
 * levels are [price, qty] string pairs.
 *
 * <p><b>REVISED 2026-08-22 — the snapshot-only assumption was WRONG and {@code seq} is gone.</b>
 * The feed moved from the {@code books50} channel to the price-GROUPED {@code depth} channel
 * ({@code arg.params.scale}), which is a true delta feed: {@code action: "snapshot" | "update"},
 * qty {@code "0"} = level delete. The old ordering field {@code data[i].seq} (and {@code pseq})
 * no longer exist on the wire; the replacement {@code checksum} is a CRC book-integrity value,
 * NOT monotonic and NOT usable as a sequence.
 *
 * <p>So the sequence id is {@code data[i].ts} — the STRING epoch millis that is also the event
 * time, as on ex8/okx. Unlike okx's exact 300 ms cadence bitget's is only NOMINALLY 600 ms, so
 * the event carries {@code sequenceJumpTolerance = 10}: job 2 accepts
 * {@code last + 590 <= ts <= last + 610}. See sample-raw-data.md § ex5.
 */
public class BitgetParser implements RawExchangeParser {

    /** Nominal publish cadence of the {@code depth} channel, in millis (user-confirmed). */
    private static final long JUMP_MS = 600L;

    /** Accepted drift either side of {@link #JUMP_MS} — a clock, not a counter. */
    private static final long JUMP_TOLERANCE_MS = 10L;

    @Override
    public List<ParsedBookEvent> parse(byte[] payload) throws Exception {
        JsonNode root = Json.MAPPER.readTree(payload);
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
