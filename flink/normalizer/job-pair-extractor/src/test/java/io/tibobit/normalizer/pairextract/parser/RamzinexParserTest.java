package io.tibobit.normalizer.pairextract.parser;

import io.tibobit.normalizer.model.RawOrderBookEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link RamzinexParser} (ex4) against the captured wire sample (sample-raw-data.md § ex4):
 * Centrifugo envelope with a NUMERIC channel market id, buys/sells sides as 7-element
 * JSON-NUMBER arrays (elements 2+ are metadata), and BOTH sides price-descending on the wire.
 */
class RamzinexParserTest {

    private final RamzinexParser parser = new RamzinexParser();

    /**
     * Given the captured ex4 snapshot, When parsed, Then the market is the channel's numeric id
     * as a string, buys→bids / sells→asks with only [price, qty] kept (exact decimal literals,
     * including the tiny "0.00005"), the sells' descending wire order is preserved as-is, and
     * seq = pub.offset with jump 0.
     */
    @Test
    @DisplayName("parses the captured snapshot")
    void parsesSnapshot() throws Exception {
        long before = System.currentTimeMillis();

        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex4-snapshot.json"));

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).getMarket()).isEqualTo("12");
        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("snapshot");
        assertThat(event.getSequenceId()).isEqualTo(5412464L);
        assertThat(event.getSequenceJump()).isZero();
        assertThat(event.getBids()).hasSize(3);
        assertThat(event.getBids().get(0).getPrice()).isEqualTo("62423.72");
        assertThat(event.getBids().get(0).getQuantity()).isEqualTo("0.011617");
        assertThat(event.getBids().get(1).getQuantity()).isEqualTo("0.00005"); // exact literal
        assertThat(event.getBids().get(2).getPrice()).isEqualTo("62400"); // integer literal kept
        // sells arrive price-DESCENDING (best ask last) — wire order passed through untouched
        assertThat(event.getAsks()).extracting(l -> l.getPrice())
                .containsExactly("64490", "64467.99", "62616.58");
        assertThat(event.getEventTime()).isGreaterThanOrEqualTo(before); // no wire timestamp
    }

    /**
     * Given a Centrifugo book frame with another exchange's data shape (bitpin's asks/bids
     * instead of buys/sells), When parsed, Then it is discarded.
     */
    @Test
    @DisplayName("discards non-book frames")
    void discardsNonBookFrames() throws Exception {
        assertThat(parser.parse(Fixtures.bytes("ex2-snapshot.json"))).isEmpty();
        assertThat(parser.parse("{}".getBytes())).isEmpty();
    }
}
