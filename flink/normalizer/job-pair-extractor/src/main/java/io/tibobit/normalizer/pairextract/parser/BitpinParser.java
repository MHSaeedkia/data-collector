package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.JsonNode;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import java.time.Instant;
import java.util.List;

/**
 * ex2 bitpin — TWO raw streams share the {@code ex2-raw} topic (2026-07-25, same shape as ex1;
 * see sample-raw-data.md § ex2):
 *
 * <ul>
 *   <li><b>REST snapshot</b>: NiFi tags the initial full book with {@code "action":"snapshot"}
 *       and injects the market as a top-level {@code "pair"} field. Levels are [price, qty]
 *       string pairs, no ordering field → {@code type="snapshot"}, {@code sequence_id=null};
 *       event time = {@code event_time} as <b>epoch millis</b> (a JSON number — NOT the
 *       ISO-8601 string the WS side uses under the same field name, user 2026-07-25).</li>
 *   <li><b>WebSocket snapshot</b>: the Centrifugo publication we already consumed — channel
 *       {@code orderbook:{market}}. <b>REVISED 2026-09-02</b>, same finding and same fix as ex1
 *       (see its javadoc): captures show every push resends the WHOLE book, so the 2026-07-25
 *       "these are deltas" call was wrong → {@code type="snapshot"},
 *       {@code sequence_id=pub.offset}, {@code sequence_jump=0} (unchecked on a snapshot — see
 *       job 2); event time = {@code data.event_time}.</li>
 * </ul>
 *
 * Anything else (connect acks, pings, malformed frames) is dropped by the whitelist rule.
 */
public class BitpinParser implements RawExchangeParser {

    private static final String CHANNEL_PREFIX = "orderbook:";

    @Override
    public List<ParsedBookEvent> parse(byte[] payload) throws Exception {
        JsonNode root = Json.MAPPER.readTree(payload);

        // REST snapshot: NiFi stamps action=snapshot and injects the market as `pair`.
        if ("snapshot".equals(root.path("action").asText())) {
            return parseRestSnapshot(root);
        }

        // Otherwise a WebSocket snapshot (Centrifugo push) — or noise, which parseWsSnapshot drops.
        return parseWsSnapshot(root);
    }

    private List<ParsedBookEvent> parseRestSnapshot(JsonNode root) {
        if (!root.path("pair").isTextual()
                || !root.path("asks").isArray() || !root.path("bids").isArray()
                || !root.path("event_time").isIntegralNumber()) {
            return List.of();
        }
        RawOrderBookEvent event = new RawOrderBookEvent(0, 0, "snapshot",
                null, 0L, root.get("event_time").asLong(),
                Levels.fromStringPairs(root.get("asks")),
                Levels.fromStringPairs(root.get("bids")));
        event.setSimulation(Json.simulation(root));
        event.setSourceIds(Json.sourceIds(root));
        return List.of(new ParsedBookEvent(root.get("pair").asText(), event));
    }

    private List<ParsedBookEvent> parseWsSnapshot(JsonNode root) {
        JsonNode push = Centrifugo.push(root);
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
        event.setSimulation(Json.simulation(root));
        event.setSourceIds(Json.sourceIds(root));
        return List.of(new ParsedBookEvent(market, event));
    }
}
