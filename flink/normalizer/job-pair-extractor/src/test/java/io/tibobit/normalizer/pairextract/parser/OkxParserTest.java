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
}
