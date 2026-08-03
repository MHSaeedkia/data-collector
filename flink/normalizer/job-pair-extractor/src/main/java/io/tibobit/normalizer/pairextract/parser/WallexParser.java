package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.JsonNode;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import java.util.List;

/**
 * ex3 wallex — array envelope {@code ["{market}@{side}", [levels...]]}, full snapshot per SIDE
 * (buyDepth = bids, sellDepth = asks; the other side stays null = "not part of this event").
 * Levels are objects with JSON-NUMBER price/quantity. The ONLY exchange with no ordering field:
 * sequence_id stays null (job 2 passes ex3 through unchecked) and event time is job-1 processing
 * time — nothing on the wire to use. See sample-raw-data.md § ex3.
 *
 * <p><b>NiFi's metadata rides in a THIRD element</b> (user 2026-08-03):
 * {@code ["{market}@{side}", [levels...], {"simulation": 1, "sink_id": "<uuid>"}]}. Every other
 * exchange has an object payload root, so NiFi injects these as root FIELDS there; ex3's root is an
 * array, so they are appended as a trailing object instead — {@code sink_id} goes in the SAME object
 * as {@code simulation}, not as a fourth element. Both the 2- and 3-element forms are still accepted
 * here, but they no longer behave the same: a 2-element frame has no flag (reads as 0, live) AND no
 * sink_id, and {@code PairExtractFunction} drops anything with no sink_id. So in practice ex3 now
 * requires the 3-element form.
 */
public class WallexParser implements RawExchangeParser {

    @Override
    public List<ParsedBookEvent> parse(byte[] payload) throws Exception {
        JsonNode root = Json.MAPPER.readTree(payload);
        if (!root.isArray() || root.size() < 2 || root.size() > 3
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
        // Trailing metadata object; absent on the 2-element form, which then reads as 0.
        event.setSimulation(Json.simulation(root.path(2)));
        event.setSourceIds(Json.sourceIds(root.path(2)));
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
