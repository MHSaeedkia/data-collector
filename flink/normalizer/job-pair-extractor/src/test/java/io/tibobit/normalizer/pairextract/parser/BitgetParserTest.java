package io.tibobit.normalizer.pairextract.parser;

import io.tibobit.normalizer.model.PriceLevel;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link BitgetParser} (ex5) against the captured wire sample (sample-raw-data.md § ex5),
 * REVISED 2026-08-22 for the {@code depth}/{@code scale} channel: an {@code action:
 * "snapshot" | "update"} discriminator on a true delta feed, no {@code seq} on the wire at all,
 * and the STRING epoch-millis inner ts doing double duty as sequence id (jump 600 ± 10) and
 * event time. Extended 2026-08-23 with the REST depth response, the second stream on
 * {@code ex5-raw}: same exchange, a different shape on every axis that matters.
 */
class BitgetParserTest {

    private final BitgetParser parser = new BitgetParser();

    /**
     * Given the captured ex5 snapshot, When parsed, Then the market comes from arg.instId, the
     * data array is unwrapped, and the string ts becomes both sequence id (jump 600, tolerance
     * 10) and event time — the outer numeric ts is ignored.
     */
    @Test
    @DisplayName("parses the captured snapshot")
    void parsesSnapshot() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex5-snapshot.json"));

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).getMarket()).isEqualTo("BTCUSDT");
        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("snapshot");
        assertThat(event.getSequenceId()).isEqualTo(1787404282388L);
        assertThat(event.getSequenceJump()).isEqualTo(600L);
        assertThat(event.getSequenceJumpTolerance()).isEqualTo(10L);
        assertThat(event.getEventTime()).isEqualTo(1787404282388L); // inner STRING ts
        assertThat(event.getAsks().get(0).getPrice()).isEqualTo("77208.71");
        assertThat(event.getAsks().get(0).getQuantity()).isEqualTo("0.755945");
        assertThat(event.getBids().get(0).getPrice()).isEqualTo("77208.70");
    }

    /**
     * Given the captured ex5 update, When parsed, Then the type maps to "update" and the
     * qty-"0" delete levels survive verbatim (job 5 needs them to remove the level) — the
     * capability the old snapshot-only parser rejected outright.
     */
    @Test
    @DisplayName("parses the captured update including the qty-0 deletes")
    void parsesUpdate() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex5-update.json"));

        assertThat(parsed).hasSize(1);
        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("update");
        assertThat(event.getSequenceId()).isEqualTo(1787404282410L);
        assertThat(event.getSequenceJump()).isEqualTo(600L);
        assertThat(event.getSequenceJumpTolerance()).isEqualTo(10L);
        assertThat(event.getAsks().get(0).getPrice()).isEqualTo("77208.71");
        assertThat(event.getAsks().get(0).getQuantity()).isEqualTo("0"); // delete signal
        assertThat(event.getBids().get(2).getQuantity()).isEqualTo("0");
    }

    /**
     * Given an update that carries only one side, When parsed, Then the missing side is null
     * rather than empty — job 5 reads null as "leave this side alone", empty as "the exchange
     * reported the side empty", and conflating them would wipe a live side.
     */
    @Test
    @DisplayName("a one-sided update nulls the absent side")
    void oneSidedUpdate() throws Exception {
        byte[] asksOnly = ("{\"action\":\"update\","
                + "\"arg\":{\"instType\":\"sp\",\"channel\":\"depth\",\"instId\":\"BTCUSDT\"},"
                + "\"data\":[{\"asks\":[[\"77210.00\",\"0.5\"]],\"checksum\":1,\"ts\":\"1787404283010\"}]}")
                .getBytes(StandardCharsets.UTF_8);

        RawOrderBookEvent event = parser.parse(asksOnly).get(0).getEvent();
        assertThat(event.getAsks()).hasSize(1);
        assertThat(event.getBids()).isNull();
    }

    /**
     * Given frames with no usable action (a subscribe ack, an empty object) or no inner ts,
     * When parsed, Then they are silently discarded.
     */
    @Test
    @DisplayName("discards non-book frames")
    void discardsNonBookFrames() throws Exception {
        byte[] subscribeAck = "{\"event\":\"subscribe\",\"arg\":{\"instId\":\"BTCUSDT\"}}"
                .getBytes(StandardCharsets.UTF_8);
        byte[] numericTs = ("{\"action\":\"snapshot\","
                + "\"arg\":{\"instId\":\"BTCUSDT\"},"
                + "\"data\":[{\"asks\":[],\"bids\":[],\"ts\":1787404282388}]}")
                .getBytes(StandardCharsets.UTF_8);

        assertThat(parser.parse(subscribeAck)).isEmpty();
        assertThat(parser.parse("{}".getBytes(StandardCharsets.UTF_8))).isEmpty();
        assertThat(parser.parse(numericTs)).isEmpty();
    }

    /**
     * Given the captured REST depth response, When parsed, Then every way it differs from a WS
     * frame is handled: the market comes from NiFi's injected {@code pair} (not {@code
     * arg.instId}), the single {@code data} OBJECT is read directly (not unwrapped from an
     * array), the sides are {@code a}/{@code b}, and the JSON-NUMBER levels become plain
     * decimal strings via BigDecimal — never via double.
     */
    @Test
    @DisplayName("parses the captured REST snapshot")
    void parsesRestSnapshot() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex5-rest-snapshot.json"));

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).getMarket()).isEqualTo("XRPUSDT"); // injected `pair`
        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("snapshot");
        // Sequenced exactly like a WS snapshot (user decision 2026-08-23), NOT null-seq.
        assertThat(event.getSequenceId()).isEqualTo(1787465707152L);
        assertThat(event.getSequenceJump()).isEqualTo(600L);
        assertThat(event.getSequenceJumpTolerance()).isEqualTo(10L);
        assertThat(event.getEventTime()).isEqualTo(1787465707152L); // data.ts, not requestTime
        assertThat(event.getAsks()).extracting(PriceLevel::getPrice)
                .containsExactly("1.4482", "1.4483", "1.4484");
        assertThat(event.getAsks().get(0).getQuantity()).isEqualTo("433.2497");
        // 1.448 keeps the literal's own scale — no trailing zero invented to match its neighbours.
        assertThat(event.getBids()).extracting(PriceLevel::getPrice)
                .containsExactly("1.4481", "1.448", "1.4479");
    }

    /**
     * Given REST-shaped bodies that are not a usable book — an error response (whose {@code data}
     * is null), and a success body NiFi never stamped with {@code pair} — When parsed, Then they
     * are silently discarded rather than emitted against an unknown market.
     */
    @Test
    @DisplayName("discards a REST error body and an unstamped one")
    void discardsUnusableRestBodies() throws Exception {
        byte[] errorBody = ("{\"code\":\"40034\",\"msg\":\"param error\","
                + "\"requestTime\":1787465707150,\"data\":null,"
                + "\"pair\":\"XRPUSDT\",\"action\":\"snapshot\"}")
                .getBytes(StandardCharsets.UTF_8);
        byte[] noPair = ("{\"code\":\"00000\",\"msg\":\"success\","
                + "\"requestTime\":1787465707150,"
                + "\"data\":{\"a\":[[1.4482,433.2497]],\"b\":[[1.4481,83.5936]],"
                + "\"ts\":\"1787465707152\"},\"action\":\"snapshot\"}")
                .getBytes(StandardCharsets.UTF_8);

        assertThat(parser.parse(errorBody)).isEmpty();
        assertThat(parser.parse(noPair)).isEmpty();
    }
}
