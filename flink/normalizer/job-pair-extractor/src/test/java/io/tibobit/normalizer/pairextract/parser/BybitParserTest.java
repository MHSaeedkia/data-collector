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
 * with jump 1 (data.seq is non-contiguous metadata and must NOT be used) — plus the SECOND
 * stream on {@code ex6-raw}, the REST depth snapshot, whose book sits under {@code result} and
 * which is deliberately NULL-SEQ because its {@code u} is not on the WS counter.
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
     * Given a delta whose unchanged side is a present-but-EMPTY array (the shape the live feed
     * actually sends, e.g. {@code "b": []}), When parsed, Then that side is an empty list rather
     * than null — a real report of "nothing changed here". It is job 5 that makes this harmless:
     * it clears a side only on a snapshot, so an empty array on an UPDATE merges nothing.
     */
    @Test
    @DisplayName("a delta's present-but-empty side is empty, not null")
    void emptySideOnDeltaIsEmptyNotNull() throws Exception {
        byte[] payload = ("{\"topic\":\"orderbook.50.BTCUSDT\",\"ts\":1787404531956,"
                + "\"type\":\"delta\",\"data\":{\"s\":\"BTCUSDT\",\"b\":[],"
                + "\"a\":[[\"77254.7\",\"0.187961\"]],\"u\":210920913,"
                + "\"seq\":112975848022},\"cts\":1787404531953}")
                .getBytes(StandardCharsets.UTF_8);

        RawOrderBookEvent event = parser.parse(payload).get(0).getEvent();
        assertThat(event.getType()).isEqualTo("update");
        assertThat(event.getBids()).isNotNull().isEmpty();
        assertThat(event.getAsks()).hasSize(1);
    }

    /**
     * Given the captured REST depth snapshot, When parsed, Then the book comes off {@code result},
     * the market off {@code result.s}, the event time off {@code result.cts} — and the sequence id
     * is NULL with jump 0, so job 2 takes the baselinePending path.
     */
    @Test
    @DisplayName("parses the captured REST snapshot as null-seq")
    void parsesRestSnapshot() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex6-rest-snapshot.json"));

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).getMarket()).isEqualTo("BTCUSDT");
        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("snapshot");
        assertThat(event.getEventTime()).isEqualTo(1787491955741L); // result.cts, not ts/time
        assertThat(event.getBids()).hasSize(3);
        assertThat(event.getBids().get(0).getPrice()).isEqualTo("77443.4");
        assertThat(event.getAsks().get(0).getPrice()).isEqualTo("77443.5");
        assertThat(event.getAsks().get(0).getQuantity()).isEqualTo("0.185647");
    }

    /**
     * The whole point of the REST branch. {@code result.u} (38992362) is NOT on the WS counter
     * (210920912 twenty-four hours EARLIER), so adopting it would gap the next delta by ~172M and
     * restart the ex5 resync loop. It must be dropped on the floor.
     */
    @Test
    @DisplayName("the REST snapshot's u and seq are never used as a sequence id")
    void restSnapshotIgnoresItsOwnCounters() throws Exception {
        RawOrderBookEvent event =
                parser.parse(Fixtures.bytes("ex6-rest-snapshot.json")).get(0).getEvent();

        assertThat(event.getSequenceId()).isNull(); // NOT 38992362, and NOT seq 113017010359
        assertThat(event.getSequenceJump()).isZero();
    }

    /**
     * Given a REST error body, When parsed, Then it is discarded by the shape whitelist —
     * bybit answers {@code "result": {}}, which has no a/b arrays, so retCode is never inspected.
     */
    @Test
    @DisplayName("discards a REST error body")
    void discardsRestErrorBody() throws Exception {
        byte[] error = ("{\"retCode\":10001,\"retMsg\":\"params error\",\"result\":{},"
                + "\"retExtInfo\":{},\"time\":1787491955827,\"action\":\"snapshot\","
                + "\"pair\":\"BTCUSDT\"}").getBytes(StandardCharsets.UTF_8);

        assertThat(parser.parse(error)).isEmpty();
    }

    /**
     * Given a REST body missing one side, When parsed, Then the whole frame is dropped: a depth
     * response is always a full book, so a half-book would silently wipe the other side.
     */
    @Test
    @DisplayName("discards a REST snapshot missing a side")
    void discardsOneSidedRestSnapshot() throws Exception {
        byte[] payload = ("{\"retCode\":0,\"retMsg\":\"OK\",\"result\":{\"s\":\"BTCUSDT\","
                + "\"a\":[[\"77443.5\",\"0.185647\"]],\"ts\":1787491955753,"
                + "\"u\":38992362,\"cts\":1787491955741},\"action\":\"snapshot\","
                + "\"pair\":\"BTCUSDT\"}").getBytes(StandardCharsets.UTF_8);

        assertThat(parser.parse(payload)).isEmpty();
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
