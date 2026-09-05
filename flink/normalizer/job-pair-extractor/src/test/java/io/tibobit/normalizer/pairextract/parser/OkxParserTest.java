package io.tibobit.normalizer.pairextract.parser;

import io.tibobit.normalizer.model.RawOrderBookEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link OkxParser} (ex8) against the captured wire samples (sample-raw-data.md § ex8):
 * bitget-family envelope, {@code action: "snapshot" | "update"}, dashed market key
 * (BTC-USDT), and the STRING epoch-millis ts doubling as sequence id (jump 300) and event
 * time. The update sample carries the set's first confirmed qty-"0" level delete.
 */
class OkxParserTest {

    private final OkxParser parser = new OkxParser();

    /**
     * Given the captured ex8 snapshot, When parsed, Then the market keeps its dash, and the
     * string ts becomes both sequence id (jump 300) and event time.
     */
    @Test
    @DisplayName("parses the captured snapshot")
    void parsesSnapshot() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex8-snapshot.json"));

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).getMarket()).isEqualTo("BTC-USDT");
        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("snapshot");
        assertThat(event.getSequenceId()).isEqualTo(1784028204900L);
        assertThat(event.getSequenceJump()).isEqualTo(300L);
        assertThat(event.getEventTime()).isEqualTo(1784028204900L); // same ts field
        assertThat(event.getAsks()).hasSize(3);
        assertThat(event.getAsks().get(0).getPrice()).isEqualTo("62770");
        assertThat(event.getBids().get(0).getQuantity()).isEqualTo("0.50795335");
    }

    /**
     * Given the captured ex8 update, When parsed, Then the type maps to "update" and the
     * qty-"0" delete level at ask 62773 survives verbatim (job 5 needs it to remove the level).
     */
    @Test
    @DisplayName("parses the captured update including the qty-0 delete")
    void parsesUpdate() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex8-update.json"));

        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("update");
        assertThat(event.getSequenceId()).isEqualTo(1784028205200L);
        assertThat(event.getAsks()).hasSize(15);
        assertThat(event.getAsks().get(2).getPrice()).isEqualTo("62773");
        assertThat(event.getAsks().get(2).getQuantity()).isEqualTo("0"); // delete signal
        assertThat(event.getBids()).hasSize(8);
    }

    /**
     * Given non-book frames (okx subscribe event, empty object), When parsed, Then they are
     * silently discarded.
     */
    @Test
    @DisplayName("discards non-book frames")
    void discardsNonBookFrames() throws Exception {
        byte[] subscribeEvent =
                "{\"event\":\"subscribe\",\"arg\":{\"channel\":\"books-grouped\",\"instId\":\"BTC-USDT\"}}"
                        .getBytes(StandardCharsets.UTF_8);

        assertThat(parser.parse(subscribeEvent)).isEmpty();
        assertThat(parser.parse("{}".getBytes(StandardCharsets.UTF_8))).isEmpty();
    }

    /**
     * Given the captured REST depth response, When parsed, Then every way it differs from a WS
     * frame is handled: the market comes from NiFi's injected {@code pair} (there is no
     * {@code arg} to read {@code instId} from), and the FOUR-element levels
     * {@code [price, qty, "0", orderCount]} keep only their first two entries.
     *
     * <p>And it is NULL-SEQ. The body carries both a {@code seqId} and a {@code ts} and neither may
     * seed the update window: {@code seqId} is a different number space from the {@code ts} the WS
     * updates are sequenced by, and the REST {@code ts} is a different clock (ex5 measured that
     * one — the next update landed inside the expected window only 9.9% of the time). Null hands
     * job 2 the {@code baselinePending} bootstrap instead.
     */
    @Test
    @DisplayName("the REST snapshot is null-seq so it never seeds the update window")
    void parsesRestSnapshot() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex8-rest-snapshot.json"));

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).getMarket()).isEqualTo("ZEC-USDT"); // injected `pair`
        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("snapshot");
        assertThat(event.getSequenceId()).isNull();
        assertThat(event.getSequenceJump()).isZero();
        assertThat(event.getSequenceJumpTolerance()).isZero();
        assertThat(event.getEventTime()).isEqualTo(1788605352151L); // data.ts
        assertThat(event.getAsks()).hasSize(3);
        assertThat(event.getAsks().get(0).getPrice()).isEqualTo("1011.99");
        assertThat(event.getAsks().get(0).getQuantity()).isEqualTo("0.2362");
        assertThat(event.getBids().get(2).getPrice()).isEqualTo("1011.65");
    }

    /**
     * Given REST-shaped bodies that are not a usable book — an error response (whose {@code data}
     * is empty), and a success body NiFi never stamped with {@code pair} — When parsed, Then they
     * are silently discarded rather than emitted against an unknown market.
     */
    @Test
    @DisplayName("discards a REST error body and an unstamped one")
    void discardsUnusableRestBodies() throws Exception {
        byte[] errorBody = ("{\"code\":\"51001\",\"msg\":\"Instrument ID does not exist\","
                + "\"data\":[],\"pair\":\"ZEC-USDT\",\"action\":\"snapshot\"}")
                .getBytes(StandardCharsets.UTF_8);
        byte[] noPair = ("{\"code\":\"0\",\"msg\":\"\",\"data\":[{"
                + "\"asks\":[[\"1011.99\",\"0.2362\",\"0\",\"1\"]],"
                + "\"bids\":[[\"1011.74\",\"0.26887\",\"0\",\"2\"]],"
                + "\"ts\":\"1788605352151\",\"seqId\":4428333610}],\"action\":\"snapshot\"}")
                .getBytes(StandardCharsets.UTF_8);

        assertThat(parser.parse(errorBody)).isEmpty();
        assertThat(parser.parse(noPair)).isEmpty();
    }
}
