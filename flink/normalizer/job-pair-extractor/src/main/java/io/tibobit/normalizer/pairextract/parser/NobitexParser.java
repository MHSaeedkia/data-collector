package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.JsonNode;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import java.util.List;

/**
 * ex1 nobitex — Centrifugo, channel {@code public:orderbook-{market}}, full snapshot on
 * every message, levels are [price, qty] string pairs. Ordering field for job 2's
 * out-of-order check = pub.offset (jump 0 = snapshot feed); event time = data.lastUpdate
 * (epoch millis). See sample-raw-data.md § ex1.
 */
public class NobitexParser implements RawExchangeParser {

    private static final String CHANNEL_PREFIX = "public:orderbook-";

    @Override
    public List<ParsedBookEvent> parse(byte[] payload) throws Exception {
        JsonNode push = Centrifugo.push(Json.MAPPER.readTree(payload));
        if (push == null || !push.get("channel").asText().startsWith(CHANNEL_PREFIX)) {
            return List.of();
        }
        JsonNode data = push.get("pub").get("data");
        if (!data.path("asks").isArray() || !data.path("bids").isArray()
                || !data.path("lastUpdate").isIntegralNumber()) {
            return List.of();
        }
        String market = push.get("channel").asText().substring(CHANNEL_PREFIX.length());
        RawOrderBookEvent event = new RawOrderBookEvent(0, 0, "snapshot",
                push.get("pub").get("offset").asLong(), 0L,
                data.get("lastUpdate").asLong(),
                Levels.fromStringPairs(data.get("asks")),
                Levels.fromStringPairs(data.get("bids")));
        return List.of(new ParsedBookEvent(market, event));
    }
}
