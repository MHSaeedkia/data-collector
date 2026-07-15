package io.tibobit.normalizer.pairextract.parser;

import io.tibobit.normalizer.model.RawOrderBookEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link BitpinParser} (ex2) against the captured wire sample (sample-raw-data.md § ex2):
 * Centrifugo envelope, channel {@code orderbook:{market}}, snapshot-only feed with pub.offset
 * as the ordering field and an ISO-8601 event_time.
 */
class BitpinParserTest {

    private final BitpinParser parser = new BitpinParser();

    /**
     * Given the captured ex2 snapshot, When parsed, Then the market comes from the channel
     * suffix, seq = pub.offset with jump 0, event time is the parsed ISO event_time, and
     * trailing-zero level strings survive verbatim ("62672.30").
     */
    @Test
    @DisplayName("parses the captured snapshot")
    void parsesSnapshot() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex2-snapshot.json"));

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).getMarket()).isEqualTo("BTC_USDT");
        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("snapshot");
        assertThat(event.getSequenceId()).isEqualTo(11286199L);
        assertThat(event.getSequenceJump()).isZero();
        assertThat(event.getEventTime())
                .isEqualTo(Instant.parse("2026-07-14T05:56:09.833955Z").toEpochMilli());
        assertThat(event.getBids().get(0).getPrice()).isEqualTo("62672.30");
        assertThat(event.getBids().get(0).getQuantity()).isEqualTo("0.01003106");
        assertThat(event.getAsks().get(0).getPrice()).isEqualTo("62714.50");
    }

    /**
     * Given non-book frames (connect ack, ex1's foreign channel format), When parsed, Then
     * both are silently discarded.
     */
    @Test
    @DisplayName("discards non-book frames")
    void discardsNonBookFrames() throws Exception {
        byte[] connectAck = "{\"connect\":{\"client\":\"abc\"}}".getBytes(StandardCharsets.UTF_8);

        assertThat(parser.parse(connectAck)).isEmpty();
        assertThat(parser.parse(Fixtures.bytes("ex1-snapshot.json"))).isEmpty();
    }
}
