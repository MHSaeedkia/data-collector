package io.tibobit.normalizer.pairextract.parser;

import io.tibobit.normalizer.model.RawOrderBookEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link BitgetParser} (ex5) against the captured wire sample (sample-raw-data.md § ex5):
 * action/arg/data-array envelope with an explicit {@code action: "snapshot"} discriminator,
 * seq as the ordering field and a STRING epoch-millis inner ts as event time.
 */
class BitgetParserTest {

    private final BitgetParser parser = new BitgetParser();

    /**
     * Given the captured ex5 snapshot, When parsed, Then the market comes from arg.instId, the
     * data array is unwrapped, seq = data[0].seq with jump 0, and event time is the inner
     * string ts parsed to millis.
     */
    @Test
    @DisplayName("parses the captured snapshot")
    void parsesSnapshot() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex5-snapshot.json"));

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).getMarket()).isEqualTo("BTCUSDT");
        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("snapshot");
        assertThat(event.getSequenceId()).isEqualTo(655666926391L);
        assertThat(event.getSequenceJump()).isZero();
        assertThat(event.getEventTime()).isEqualTo(1784026071995L); // inner STRING ts, not outer
        assertThat(event.getAsks().get(0).getPrice()).isEqualTo("62815");
        assertThat(event.getAsks().get(0).getQuantity()).isEqualTo("0.021591");
        assertThat(event.getBids().get(0).getPrice()).isEqualTo("62814.99");
    }

    /**
     * Given frames without the snapshot action (a subscribe ack, an empty object), When
     * parsed, Then they are silently discarded.
     */
    @Test
    @DisplayName("discards non-snapshot frames")
    void discardsNonSnapshotFrames() throws Exception {
        byte[] subscribeAck = "{\"event\":\"subscribe\",\"arg\":{\"instId\":\"BTCUSDT\"}}"
                .getBytes(StandardCharsets.UTF_8);

        assertThat(parser.parse(subscribeAck)).isEmpty();
        assertThat(parser.parse("{}".getBytes(StandardCharsets.UTF_8))).isEmpty();
    }
}
