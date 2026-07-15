package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.JsonNode;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import java.util.List;

/**
 * ex4 ramzinex — Centrifugo, channel {@code orderbook:{numeric market id}} (the id IS the
 * exchange_markets.market string), full snapshot on every message. Sides are buys/sells
 * (→ bids/asks); levels are 7-element JSON-NUMBER arrays — only [price, qty] matter, and
 * BOTH sides arrive price-descending (best ask LAST), passed through in wire order.
 * Ordering field = pub.offset (jump 0); no message-level timestamp on the wire → event
 * time is job-1 processing time. See sample-raw-data.md § ex4.
 */
public class RamzinexParser implements RawExchangeParser {

    private static final String CHANNEL_PREFIX = "orderbook:";

    @Override
    public List<ParsedBookEvent> parse(byte[] payload) throws Exception {
        JsonNode push = Centrifugo.push(Json.MAPPER.readTree(payload));
        if (push == null || !push.get("channel").asText().startsWith(CHANNEL_PREFIX)) {
            return List.of();
        }
        JsonNode data = push.get("pub").get("data");
        if (!data.path("buys").isArray() || !data.path("sells").isArray()) {
            return List.of();
        }
        String market = push.get("channel").asText().substring(CHANNEL_PREFIX.length());
        RawOrderBookEvent event = new RawOrderBookEvent(0, 0, "snapshot",
                push.get("pub").get("offset").asLong(), 0L,
                System.currentTimeMillis(),
                Levels.fromNumericArrays(data.get("sells")),
                Levels.fromNumericArrays(data.get("buys")));
        return List.of(new ParsedBookEvent(market, event));
    }
}
