package io.tibobit.normalizer.pairextract.parser;

import io.tibobit.normalizer.model.RawOrderBookEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link WallexParser} (ex3) against the captured wire samples (sample-raw-data.md § ex3):
 * per-SIDE snapshots in a ["{market}@{side}", [levels]] array envelope with JSON-NUMBER
 * price/quantity — the BigDecimal-from-literal exchange, and the only one with no ordering
 * field (sequence_id stays null, event time is processing time).
 */
class WallexParserTest {

    private final WallexParser parser = new WallexParser();

    /**
     * Given the captured buyDepth message, When parsed, Then bids are populated and asks stay
     * NULL (side not part of this event — never an empty list), sequence_id is null, and
     * JSON-number values keep their exact decimal literal ("62200", "0.02624").
     */
    @Test
    @DisplayName("parses buyDepth into a bids-only snapshot")
    void parsesBuyDepth() throws Exception {
        long before = System.currentTimeMillis();

        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex3-buy-depth.json"));

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).getMarket()).isEqualTo("BTCUSDT");
        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("snapshot");
        assertThat(event.getSequenceId()).isNull(); // no ordering field on ex3's wire
        assertThat(event.getSequenceJump()).isZero();
        assertThat(event.getAsks()).isNull(); // absent side, NOT empty
        assertThat(event.getBids()).hasSize(3);
        assertThat(event.getBids().get(0).getPrice()).isEqualTo("62525.04");
        assertThat(event.getBids().get(0).getQuantity()).isEqualTo("0.000451");
        assertThat(event.getBids().get(1).getQuantity()).isEqualTo("0.02624");
        assertThat(event.getBids().get(2).getPrice()).isEqualTo("62200"); // integer literal kept
        assertThat(event.getEventTime()).isGreaterThanOrEqualTo(before); // processing time
    }

    /**
     * Given the captured sellDepth message, When parsed, Then asks are populated and bids stay
     * null.
     */
    @Test
    @DisplayName("parses sellDepth into an asks-only snapshot")
    void parsesSellDepth() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex3-sell-depth.json"));

        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getBids()).isNull();
        assertThat(event.getAsks()).hasSize(3);
        assertThat(event.getAsks().get(0).getPrice()).isEqualTo("62579.56");
        assertThat(event.getAsks().get(1).getQuantity()).isEqualTo("0.002");
    }

    /**
     * Given frames that aren't buyDepth/sellDepth messages (unknown side key, plain object),
     * When parsed, Then they are silently discarded.
     */
    @Test
    @DisplayName("discards non-book frames")
    void discardsNonBookFrames() throws Exception {
        byte[] unknownSide = "[\"BTCUSDT@trades\", [{\"price\": 1, \"quantity\": 2}]]"
                .getBytes(StandardCharsets.UTF_8);

        assertThat(parser.parse(unknownSide)).isEmpty();
        assertThat(parser.parse("{\"ping\":1}".getBytes(StandardCharsets.UTF_8))).isEmpty();
    }
}
