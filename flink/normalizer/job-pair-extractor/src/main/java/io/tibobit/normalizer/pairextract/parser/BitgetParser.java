package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.JsonNode;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * ex5 bitget — {@code action}/{@code arg}/{@code data}-array envelope with an explicit
 * {@code action: "snapshot"} discriminator (snapshot-only feed), market key = arg.instId,
 * levels are [price, qty] string pairs. Ordering field for job 2's out-of-order check =
 * data[i].seq (jump 0 = snapshot feed); event time = data[i].ts (STRING epoch millis).
 * See sample-raw-data.md § ex5.
 */
public class BitgetParser implements RawExchangeParser {

    @Override
    public List<ParsedBookEvent> parse(byte[] payload) throws Exception {
        JsonNode root = Json.MAPPER.readTree(payload);
        String market = root.path("arg").path("instId").asText(null);
        if (market == null || !"snapshot".equals(root.path("action").asText())
                || !root.path("data").isArray()) {
            return List.of();
        }
        List<ParsedBookEvent> events = new ArrayList<>();
        for (JsonNode book : root.get("data")) {
            if (!book.path("asks").isArray() || !book.path("bids").isArray()
                    || !book.path("seq").isIntegralNumber() || !book.path("ts").isTextual()) {
                return List.of();
            }
            RawOrderBookEvent event = new RawOrderBookEvent(0, 0, "snapshot",
                    book.get("seq").asLong(), 0L,
                    Long.parseLong(book.get("ts").asText()),
                    Levels.fromStringPairs(book.get("asks")),
                    Levels.fromStringPairs(book.get("bids")));
            events.add(new ParsedBookEvent(market, event));
        }
        return events;
    }
}
