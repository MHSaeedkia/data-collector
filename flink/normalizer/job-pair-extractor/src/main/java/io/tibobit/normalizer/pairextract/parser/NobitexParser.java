package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.JsonNode;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import java.util.List;

/**
 * ex1 nobitex — TWO raw streams share the {@code ex1-raw} topic (NiFi fix 2026-07-21, see
 * sample-raw-data.md § ex1):
 *
 * <ul>
 *   <li><b>REST snapshot</b>: NiFi tags the initial full book with {@code "action":"snapshot"}
 *       and injects the market as a top-level {@code "pair"} field (the REST body carries no
 *       symbol of its own). Levels are [price, qty] string pairs, no ordering field →
 *       {@code type="snapshot"}, {@code sequence_id=null}; event time = {@code lastUpdate}. Job 2
 *       treats a null-seq snapshot as a resync signal (its offset is unknown), so the first WS
 *       update after it adopts the offset as the baseline.</li>
 *   <li><b>WebSocket update</b>: the Centrifugo publication we already consumed — channel
 *       {@code public:orderbook-{market}}, no {@code action} field. These are DELTAS, not
 *       snapshots (the old assumption was wrong) → {@code type="update"},
 *       {@code sequence_id=pub.offset}, {@code sequence_jump=1} (Centrifugo offsets increment by
 *       one per publication); event time = {@code data.lastUpdate}.</li>
 * </ul>
 *
 * Anything else (connect acks, pings, malformed frames) is dropped by the whitelist rule.
 */
public class NobitexParser implements RawExchangeParser {

    private static final String CHANNEL_PREFIX = "public:orderbook-";

    @Override
    public List<ParsedBookEvent> parse(byte[] payload) throws Exception {
        JsonNode root = Json.MAPPER.readTree(payload);

        // REST snapshot: NiFi stamps action=snapshot and injects the market as `pair`.
        if ("snapshot".equals(root.path("action").asText())) {
            return parseRestSnapshot(root);
        }

        // Otherwise a WebSocket delta (Centrifugo push) — or noise, which parseWsUpdate drops.
        return parseWsUpdate(root);
    }

    private List<ParsedBookEvent> parseRestSnapshot(JsonNode root) {
        if (!root.path("pair").isTextual()
                || !root.path("asks").isArray() || !root.path("bids").isArray()
                || !root.path("lastUpdate").isIntegralNumber()) {
            return List.of();
        }
        RawOrderBookEvent event = new RawOrderBookEvent(0, 0, "snapshot",
                null, 0L, root.get("lastUpdate").asLong(),
                Levels.fromStringPairs(root.get("asks")),
                Levels.fromStringPairs(root.get("bids")));
        return List.of(new ParsedBookEvent(root.get("pair").asText(), event));
    }

    private List<ParsedBookEvent> parseWsUpdate(JsonNode root) {
        JsonNode push = Centrifugo.push(root);
        if (push == null || !push.get("channel").asText().startsWith(CHANNEL_PREFIX)) {
            return List.of();
        }
        JsonNode data = push.get("pub").get("data");
        if (!data.path("asks").isArray() || !data.path("bids").isArray()
                || !data.path("lastUpdate").isIntegralNumber()) {
            return List.of();
        }
        String market = push.get("channel").asText().substring(CHANNEL_PREFIX.length());
        RawOrderBookEvent event = new RawOrderBookEvent(0, 0, "update",
                push.get("pub").get("offset").asLong(), 1L,
                data.get("lastUpdate").asLong(),
                Levels.fromStringPairs(data.get("asks")),
                Levels.fromStringPairs(data.get("bids")));
        return List.of(new ParsedBookEvent(market, event));
    }
}
