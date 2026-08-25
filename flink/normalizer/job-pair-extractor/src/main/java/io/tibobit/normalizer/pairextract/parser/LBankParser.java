package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.JsonNode;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

public class LBankParser implements RawExchangeParser {

    @Override
    public List<ParsedBookEvent> parse(byte[] payload) throws Exception {
        JsonNode root = Json.MAPPER.readTree(payload);

        String market = root.path("pair").asText(null);
        JsonNode depth = root.path("depth");

        if (market == null
                || !depth.path("asks").isArray()
                || !depth.path("bids").isArray()
                || !root.path("TS").isTextual()) {
            return List.of();
        }

        long eventTimeMs = LocalDateTime
                .parse(root.get("TS").asText())
                .toInstant(ZoneOffset.UTC)
                .toEpochMilli();

        RawOrderBookEvent event = new RawOrderBookEvent(
                0,
                0,
                "snapshot",
                null,
                0L,
                eventTimeMs,
                Levels.fromStringPairs(depth.get("asks")),
                Levels.fromStringPairs(depth.get("bids"))
        );

        event.setSimulation(Json.simulation(root));
        event.setSourceIds(Json.sourceIds(root));

        return List.of(new ParsedBookEvent(market, event));
    }
}