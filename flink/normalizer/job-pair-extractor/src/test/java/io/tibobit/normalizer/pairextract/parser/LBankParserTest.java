package io.tibobit.normalizer.pairextract.parser;

import io.tibobit.normalizer.model.RawOrderBookEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link LBankParser} (ex9) against the captured wire sample (sample-raw-data.md § ex9):
 * a snapshot-only feed, so every frame is a whole book with a NULL sequence id, and the only
 * ordering field is the zone-less ISO-8601 {@code TS} read as UTC.
 */
class LBankParserTest {

    private final LBankParser parser = new LBankParser();

    private static byte[] json(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Given the captured ex9 snapshot, When parsed, Then the underscored market key survives
     * verbatim, the event is a snapshot with no sequence, and the string levels are untouched.
     */
    @Test
    @DisplayName("parses the captured snapshot")
    void parsesSnapshot() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex9-snapshot.json"));

        assertThat(parsed).hasSize(1);
        // Lowercase with an underscore, exactly as exchange_markets.market spells it — nothing
        // upper-cases or strips it, so a case change on either side would drop every ex9 message.
        assertThat(parsed.get(0).getMarket()).isEqualTo("btc_usdt");
        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("snapshot");
        assertThat(event.getAsks()).hasSize(3);
        assertThat(event.getAsks().get(0).getPrice()).isEqualTo("79654.45");
        assertThat(event.getAsks().get(0).getQuantity()).isEqualTo("1.04718");
        assertThat(event.getBids()).hasSize(3);
        assertThat(event.getBids().get(0).getPrice()).isEqualTo("79654.44");
        assertThat(event.getBids().get(2).getQuantity()).isEqualTo("0.00016");
    }

    /**
     * Given any ex9 frame, When parsed, Then it carries NO sequence id and jump 0 — the wire has
     * no counter, so job 2 must fall through to its event-time branch. This is the whole reason
     * ex9 behaves like ex3 downstream; if it ever gains a sequence, everything below changes.
     */
    @Test
    @DisplayName("carries no sequence id, so job 2 orders it by event time")
    void hasNoSequence() throws Exception {
        RawOrderBookEvent event = parser.parse(Fixtures.bytes("ex9-snapshot.json")).get(0).getEvent();

        assertThat(event.getSequenceId()).isNull();
        assertThat(event.getSequenceJump()).isZero();
        assertThat(event.getSequenceJumpTolerance()).isZero();
    }

    /**
     * Given {@code TS} with no zone marker, When parsed, Then it is read as UTC. Pinned as its own
     * test because it is the parser's one unverifiable-from-the-payload assumption: an 8-hour
     * error here is invisible in the levels and would only ever show up as a staleness alarm.
     */
    @Test
    @DisplayName("reads the zone-less TS as UTC")
    void readsTsAsUtc() throws Exception {
        RawOrderBookEvent event = parser.parse(Fixtures.bytes("ex9-snapshot.json")).get(0).getEvent();

        assertThat(event.getEventTime()).isEqualTo(1787680011723L); // 2026-08-25T17:46:51.723Z
    }

    /**
     * Given lbank frames that are not a depth book — a ping, a subscribe ack, the incremental
     * depth channel (whose levels hang off {@code incrDepth}, not {@code depth}) and a
     * half-populated book — When parsed, Then they are silently discarded. The parser selects on
     * SHAPE, so a frame missing either side is dropped whole rather than emitted half-empty.
     */
    @Test
    @DisplayName("discards non-book frames")
    void discardsNonBookFrames() throws Exception {
        assertThat(parser.parse(json("{\"action\":\"ping\",\"ping\":\"0c1c1a4b\"}"))).isEmpty();
        assertThat(parser.parse(json("{\"status\":\"success\",\"pair\":\"btc_usdt\"}"))).isEmpty();
        assertThat(parser.parse(json(
                "{\"incrDepth\":{\"asks\":[],\"bids\":[]},\"type\":\"incrDepth\","
                        + "\"pair\":\"btc_usdt\",\"TS\":\"2026-08-25T17:46:51.723\"}"))).isEmpty();
        assertThat(parser.parse(json(
                "{\"depth\":{\"asks\":[[\"1\",\"2\"]]},\"type\":\"fdepth\","
                        + "\"pair\":\"btc_usdt\",\"TS\":\"2026-08-25T17:46:51.723\"}"))).isEmpty();
        assertThat(parser.parse(json("{}"))).isEmpty();
    }

    /**
     * Given a book frame with no {@code TS}, When parsed, Then it is discarded rather than stamped
     * with a substitute clock. ex9's event time IS its ordering field — a frame without one cannot
     * be placed relative to the book job 2 already holds, so guessing would defeat the guard.
     */
    @Test
    @DisplayName("discards a book frame with no TS rather than inventing one")
    void discardsFrameWithoutTs() throws Exception {
        assertThat(parser.parse(json(
                "{\"depth\":{\"asks\":[[\"1\",\"2\"]],\"bids\":[[\"1\",\"2\"]]},"
                        + "\"type\":\"fdepth\",\"pair\":\"btc_usdt\"}"))).isEmpty();
    }

    /**
     * Given numeric levels where ex9's wire sends strings, When parsed, Then the parser throws so
     * job 1 drops the whole frame — never a partial book (the shared {@link Levels} rule).
     */
    @Test
    @DisplayName("throws on numeric levels so the frame is dropped whole")
    void throwsOnNumericLevels() {
        byte[] payload = json("{\"depth\":{\"asks\":[[79654.45,1.04718]],\"bids\":[]},"
                + "\"type\":\"fdepth\",\"pair\":\"btc_usdt\",\"TS\":\"2026-08-25T17:46:51.723\"}");

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> parser.parse(payload))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
