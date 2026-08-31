package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.JsonNode;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * ex8 okx — bitget-family {@code arg}/{@code action}/{@code data}-array envelope, true
 * delta feed: {@code action: "snapshot" | "update"}. Market key = arg.instId (with a DASH:
 * {@code BTC-USDT}); levels are [price, qty] string pairs, qty "0" = level delete (confirmed
 * on wire). Sequence id = data[i].ts (STRING epoch millis — the only timestamp AND the
 * ordering field) with jump 300 (user-confirmed 300 ms cadence); event time = the same ts.
 * See sample-raw-data.md § ex8.
 */
public class OkxParser implements RawExchangeParser {

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
                    ts, 300L, ts,
                    book.has("asks") ? Levels.fromStringPairs(book.get("asks")) : null,
                    book.has("bids") ? Levels.fromStringPairs(book.get("bids")) : null);
            events.add(new ParsedBookEvent(market, event));
        }
        return events;
    }
}
