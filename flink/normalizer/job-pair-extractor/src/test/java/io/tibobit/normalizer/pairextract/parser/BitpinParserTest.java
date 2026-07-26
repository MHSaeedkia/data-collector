package io.tibobit.normalizer.pairextract.parser;

import io.tibobit.normalizer.model.RawOrderBookEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link BitpinParser} (ex2) against the captured wire samples (sample-raw-data.md § ex2):
 * the REST snapshot (action=snapshot, injected {@code pair}, no offset) and the Centrifugo WS
 * delta (channel {@code orderbook:{market}}, pub.offset with jump 1), both with an ISO-8601
 * event_time.
 */
class BitpinParserTest {

    private final BitpinParser parser = new BitpinParser();

    /**
     * Given the REST snapshot, When parsed, Then the market comes from the injected `pair`
     * field, it is a snapshot with NO sequence id (job 2 resyncs off it), event time is the
     * epoch-millis event_time taken verbatim (the REST side sends a NUMBER, unlike the WS
     * side's ISO string), and trailing-zero level strings survive verbatim ("62672.30").
     */
    @Test
    @DisplayName("parses the REST snapshot")
    void parsesRestSnapshot() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex2-snapshot.json"));

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).getMarket()).isEqualTo("BTC_USDT");
        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("snapshot");
        assertThat(event.getSequenceId()).isNull();
        assertThat(event.getSequenceJump()).isZero();
        assertThat(event.getEventTime()).isEqualTo(1784008564112L);
        assertThat(event.getBids().get(0).getPrice()).isEqualTo("62672.30");
        assertThat(event.getBids().get(0).getQuantity()).isEqualTo("0.01003106");
        assertThat(event.getAsks().get(0).getPrice()).isEqualTo("62714.50");
    }

    /**
     * Given the captured WS publication, When parsed, Then the market comes from the channel
     * suffix and it is an UPDATE (a delta, not a snapshot) keyed by pub.offset with jump 1 —
     * Centrifugo offsets increment by exactly one.
     */
    @Test
    @DisplayName("parses the WebSocket delta as an update")
    void parsesWsUpdate() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex2-update.json"));

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).getMarket()).isEqualTo("BTC_USDT");
        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("update");
        assertThat(event.getSequenceId()).isEqualTo(11286199L);
        assertThat(event.getSequenceJump()).isOne();
        assertThat(event.getEventTime())
                .isEqualTo(Instant.parse("2026-07-14T05:56:09.833955Z").toEpochMilli());
        assertThat(event.getBids().get(0).getPrice()).isEqualTo("62672.30");
        assertThat(event.getAsks().get(0).getPrice()).isEqualTo("62714.50");
    }

    /**
     * Given non-book frames (a Centrifugo connect ack, and a well-formed publication on a
     * channel that isn't bitpin's {@code orderbook:} prefix), When parsed, Then both are
     * silently discarded — the channel prefix is the WS recognition key.
     */
    @Test
    @DisplayName("discards non-book frames")
    void discardsNonBookFrames() throws Exception {
        byte[] connectAck = "{\"connect\":{\"client\":\"abc\"}}".getBytes(StandardCharsets.UTF_8);
        byte[] foreignChannel = ("{\"push\":{\"channel\":\"depth:BTC_USDT\",\"pub\":"
                + "{\"data\":{\"asks\":[],\"bids\":[],\"event_time\":\"2026-07-14T05:56:09.833955Z\"},"
                + "\"offset\":1}}}").getBytes(StandardCharsets.UTF_8);

        assertThat(parser.parse(connectAck)).isEmpty();
        assertThat(parser.parse(foreignChannel)).isEmpty();
    }

    /**
     * Given ex1's REST snapshot (same action/pair/epoch-millis shape, but nobitex names the
     * timestamp `lastUpdate` where bitpin names it `event_time`), When parsed, Then it is
     * discarded — the snapshot branch keys off the timestamp FIELD NAME, not just `action`.
     * That name is now the only thing telling the two REST payloads apart.
     */
    @Test
    @DisplayName("discards a foreign REST snapshot shape")
    void discardsForeignRestSnapshot() throws Exception {
        assertThat(parser.parse(Fixtures.bytes("ex1-snapshot.json"))).isEmpty();
    }
}
