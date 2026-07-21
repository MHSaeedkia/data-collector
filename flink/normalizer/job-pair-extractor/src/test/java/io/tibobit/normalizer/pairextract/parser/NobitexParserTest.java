package io.tibobit.normalizer.pairextract.parser;

import io.tibobit.normalizer.model.RawOrderBookEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link NobitexParser} (ex1) against both captured wire samples (sample-raw-data.md § ex1):
 * the REST snapshot (NiFi stamps {@code action=snapshot} + injects {@code pair}) and the WebSocket
 * delta (Centrifugo envelope, no action, pub.offset as the ordering field).
 */
class NobitexParserTest {

    private final NobitexParser parser = new NobitexParser();

    /**
     * Given the REST snapshot (action=snapshot, market in the injected `pair` field), When parsed,
     * Then it is a null-seq snapshot (no offset on the wire), market from `pair`, event time =
     * lastUpdate, and level strings survive verbatim (including the no-decimals price "65660").
     */
    @Test
    @DisplayName("parses the REST snapshot, market from the injected pair field")
    void parsesRestSnapshot() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex1-snapshot.json"));

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).getMarket()).isEqualTo("BTCUSDT");
        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("snapshot");
        assertThat(event.getSequenceId()).isNull();
        assertThat(event.getSequenceJump()).isZero();
        assertThat(event.getEventTime()).isEqualTo(1784614865284L);
        assertThat(event.getBids().get(0).getPrice()).isEqualTo("65660");
        assertThat(event.getBids().get(0).getQuantity()).isEqualTo("0.000615");
        assertThat(event.getAsks().get(0).getPrice()).isEqualTo("65708.76");
        assertThat(event.getExchangeId()).isZero(); // ids are PairExtractFunction's job
        assertThat(event.getPairId()).isZero();
    }

    /**
     * Given the WebSocket Centrifugo delta, When parsed, Then the market comes from the channel
     * suffix, the event is an UPDATE ordered by pub.offset with jump 1 (Centrifugo increments by
     * one), event time = lastUpdate, and level strings survive verbatim.
     */
    @Test
    @DisplayName("parses the WebSocket message as an update ordered by pub.offset, jump 1")
    void parsesWebSocketUpdate() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex1-update.json"));

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).getMarket()).isEqualTo("BTCUSDT");
        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("update");
        assertThat(event.getSequenceId()).isEqualTo(33259L);
        assertThat(event.getSequenceJump()).isEqualTo(1L);
        assertThat(event.getEventTime()).isEqualTo(1784021328931L);
        assertThat(event.getAsks()).hasSize(3);
        assertThat(event.getAsks().get(0).getPrice()).isEqualTo("62678");
        assertThat(event.getBids().get(0).getPrice()).isEqualTo("62669");
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
