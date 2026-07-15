package io.tibobit.normalizer.pairextract.parser;

import io.tibobit.normalizer.model.RawOrderBookEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link NobitexParser} (ex1) against the captured wire sample (sample-raw-data.md § ex1):
 * Centrifugo envelope, channel {@code public:orderbook-{market}}, snapshot-only feed with
 * pub.offset as the ordering field.
 */
class NobitexParserTest {

    private final NobitexParser parser = new NobitexParser();

    /**
     * Given the captured ex1 snapshot, When parsed, Then the market comes from the channel
     * suffix, the event is a jump-0 snapshot ordered by pub.offset, event time = lastUpdate,
     * and level strings survive verbatim (including the no-decimals price "62678").
     */
    @Test
    @DisplayName("parses the captured snapshot")
    void parsesSnapshot() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex1-snapshot.json"));

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).getMarket()).isEqualTo("BTCUSDT");
        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("snapshot");
        assertThat(event.getSequenceId()).isEqualTo(33259L);
        assertThat(event.getSequenceJump()).isZero();
        assertThat(event.getEventTime()).isEqualTo(1784021328931L);
        assertThat(event.getAsks()).hasSize(3);
        assertThat(event.getAsks().get(0).getPrice()).isEqualTo("62678");
        assertThat(event.getAsks().get(0).getQuantity()).isEqualTo("0.000963");
        assertThat(event.getBids().get(0).getPrice()).isEqualTo("62669");
        assertThat(event.getExchangeId()).isZero(); // ids are PairExtractFunction's job
        assertThat(event.getPairId()).isZero();
    }

    /**
     * Given non-book frames (a Centrifugo connect ack and an empty object), When parsed, Then
     * both are silently discarded — the whitelist rule.
     */
    @Test
    @DisplayName("discards non-book frames")
    void discardsNonBookFrames() throws Exception {
        byte[] connectAck = "{\"connect\":{\"client\":\"abc\",\"version\":\"5.0\"}}"
                .getBytes(StandardCharsets.UTF_8);

        assertThat(parser.parse(connectAck)).isEmpty();
        assertThat(parser.parse("{}".getBytes(StandardCharsets.UTF_8))).isEmpty();
    }

    /**
     * Given a book frame from another exchange's channel format (bitpin's orderbook:BTC_USDT),
     * When parsed, Then it is discarded — the channel prefix is the recognition key.
     */
    @Test
    @DisplayName("discards frames from a foreign channel format")
    void discardsForeignChannel() throws Exception {
        assertThat(parser.parse(Fixtures.bytes("ex2-snapshot.json"))).isEmpty();
    }
}
