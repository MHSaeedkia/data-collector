package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.JsonNode;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import java.time.Instant;
import java.util.List;

/**
 * ex2 bitpin — Centrifugo, channel {@code orderbook:{market}}, full snapshot on every
 * message, levels are [price, qty] string pairs. Ordering field for job 2's out-of-order
 * check = pub.offset (jump 0 = snapshot feed); event time = data.event_time (ISO-8601).
 * See sample-raw-data.md § ex2.
 */
public class BitpinParser implements RawExchangeParser {

    private static final String CHANNEL_PREFIX = "orderbook:";

    @Override
    public List<ParsedBookEvent> parse(byte[] payload) throws Exception {
        JsonNode push = Centrifugo.push(Json.MAPPER.readTree(payload));
        if (push == null || !push.get("channel").asText().startsWith(CHANNEL_PREFIX)) {
            return List.of();
        }
        JsonNode data = push.get("pub").get("data");
        if (!data.path("asks").isArray() || !data.path("bids").isArray()
                || !data.path("event_time").isTextual()) {
            return List.of();
        }
        String market = push.get("channel").asText().substring(CHANNEL_PREFIX.length());
        RawOrderBookEvent event = new RawOrderBookEvent(0, 0, "snapshot",
                push.get("pub").get("offset").asLong(), 0L,
                Instant.parse(data.get("event_time").asText()).toEpochMilli(),
                Levels.fromStringPairs(data.get("asks")),
                Levels.fromStringPairs(data.get("bids")));
        return List.of(new ParsedBookEvent(market, event));
    }
}
