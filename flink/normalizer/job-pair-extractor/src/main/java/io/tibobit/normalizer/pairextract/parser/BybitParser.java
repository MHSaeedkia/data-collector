package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.JsonNode;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import java.util.List;

/**
 * ex6 bybit — {@code topic}/{@code ts}/{@code type}/{@code data}/{@code cts} envelope,
 * true delta feed: {@code type: "snapshot" | "delta"} ("delta" → our "update"). Market key
 * = data.s; sides are b/a (→ bids/asks) string pairs — a delta may carry one side only
 * (missing key → null side, present-but-empty → empty list). Sequence id = data.u with
 * jump 1 (user-confirmed; data.seq is non-contiguous metadata — never use it); event time
 * = cts (matching-engine time — the book-change analog of okx's data ts).
 * See sample-raw-data.md § ex6.
 */
public class BybitParser implements RawExchangeParser {

    @Override
    public List<ParsedBookEvent> parse(byte[] payload) throws Exception {
        JsonNode root = Json.MAPPER.readTree(payload);
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
        RawOrderBookEvent event = new RawOrderBookEvent(0, 0, type,
                data.get("u").asLong(), 1L,
                root.get("cts").asLong(),
                data.has("a") ? Levels.fromStringPairs(data.get("a")) : null,
                data.has("b") ? Levels.fromStringPairs(data.get("b")) : null);
        return List.of(new ParsedBookEvent(market, event));
    }
}
