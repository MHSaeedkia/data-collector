package io.tibobit.normalizer.pairextract.parser;

import io.tibobit.normalizer.model.RawOrderBookEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link BybitParser} (ex6) against the captured wire samples (sample-raw-data.md § ex6):
 * the first true delta feed — {@code type: "snapshot" | "delta"}, sides b/a, sequence id = u
 * with jump 1 (data.seq is non-contiguous metadata and must NOT be used).
 */
class BybitParserTest {

    private final BybitParser parser = new BybitParser();

    /**
     * Given the captured ex6 snapshot, When parsed, Then market = data.s, b/a map to
     * bids/asks, seq = u (NOT data.seq) with jump 1, and event time = cts.
     */
    @Test
    @DisplayName("parses the captured snapshot")
    void parsesSnapshot() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex6-snapshot.json"));

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).getMarket()).isEqualTo("BTCUSDT");
        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("snapshot");
        assertThat(event.getSequenceId()).isEqualTo(126776811L); // u, not seq 111416318484
        assertThat(event.getSequenceJump()).isEqualTo(1L);
        assertThat(event.getEventTime()).isEqualTo(1784027470170L); // cts
        assertThat(event.getBids()).hasSize(3);
        assertThat(event.getBids().get(0).getPrice()).isEqualTo("62724.1");
        assertThat(event.getAsks().get(0).getPrice()).isEqualTo("62724.2");
    }

    /**
     * Given the captured ex6 delta (only changed levels), When parsed, Then the type maps to
     * our "update" and the single changed level per side comes through.
     */
    @Test
    @DisplayName("parses the captured delta as an update")
    void parsesDelta() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex6-delta.json"));

        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("update");
        assertThat(event.getSequenceId()).isEqualTo(126776812L);
        assertThat(event.getBids()).hasSize(1);
        assertThat(event.getBids().get(0).getPrice()).isEqualTo("62709.4");
        assertThat(event.getAsks()).hasSize(1);
        assertThat(event.getAsks().get(0).getQuantity()).isEqualTo("0.529037");
    }

    /**
     * Given non-book frames (a subscribe ack, an empty object), When parsed, Then they are
     * silently discarded.
     */
    @Test
    @DisplayName("discards non-book frames")
    void discardsNonBookFrames() throws Exception {
        byte[] subscribeAck = "{\"op\":\"subscribe\",\"success\":true,\"conn_id\":\"x\"}"
                .getBytes(StandardCharsets.UTF_8);

        assertThat(parser.parse(subscribeAck)).isEmpty();
        assertThat(parser.parse("{}".getBytes(StandardCharsets.UTF_8))).isEmpty();
    }
}
