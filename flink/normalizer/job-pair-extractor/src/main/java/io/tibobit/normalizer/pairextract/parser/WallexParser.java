package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.JsonNode;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import java.util.List;

/**
 * ex3 wallex — 2-element array envelope {@code ["{market}@{side}", [levels...]]}, full
 * snapshot per SIDE (buyDepth = bids, sellDepth = asks; the other side stays null = "not
 * part of this event"). Levels are objects with JSON-NUMBER price/quantity. The ONLY
 * exchange with no ordering field: sequence_id stays null (job 2 passes ex3 through
 * unchecked) and event time is job-1 processing time — nothing on the wire to use.
 * See sample-raw-data.md § ex3.
 */
public class WallexParser implements RawExchangeParser {

    @Override
    public List<ParsedBookEvent> parse(byte[] payload) throws Exception {
        JsonNode root = Json.MAPPER.readTree(payload);
        if (!root.isArray() || root.size() != 2
                || !root.get(0).isTextual() || !root.get(1).isArray()) {
            return List.of();
        }
        String key = root.get(0).asText();
        int at = key.indexOf('@');
        if (at < 1) {
            return List.of();
        }
        String market = key.substring(0, at);
        String side = key.substring(at + 1);
        RawOrderBookEvent event = new RawOrderBookEvent(0, 0, "snapshot",
                null, 0L, System.currentTimeMillis(), null, null);
        switch (side) {
            case "sellDepth" -> event.setAsks(Levels.fromPriceQuantityObjects(root.get(1)));
            case "buyDepth" -> event.setBids(Levels.fromPriceQuantityObjects(root.get(1)));
            default -> {
                return List.of();
            }
        }
        return List.of(new ParsedBookEvent(market, event));
    }
}
