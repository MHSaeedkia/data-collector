package io.tibobit.consolidator.deserializer;

import io.tibobit.consolidator.model.PriceLevelEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link PriceLevelEventDeserializer} against the wire contract: the JSON value bytes of an
 * input Kafka record (see schemas/price_level_event.avsc and the {@code @JsonProperty} mappings on
 * {@link PriceLevelEvent}) must decode into a fully populated event. This pins the snake_case →
 * camelCase mapping, the exact decimal strings for price/quantity, and the "ignore unknown wire
 * fields" rule — any drift here silently breaks every downstream stage, so it is asserted directly
 * on the bytes the source receives.
 */
class PriceLevelEventDeserializerTest {

    private final PriceLevelEventDeserializer deserializer = new PriceLevelEventDeserializer();

    private PriceLevelEvent deserialize(String json) throws IOException {
        return deserializer.deserialize(json.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Given a full price-level JSON record as produced upstream, When deserialized, Then every
     * snake_case wire field maps onto its camelCase POJO field and price/quantity keep their exact
     * decimal strings. The happy-path contract the whole job depends on.
     */
    @Test
    @DisplayName("maps every snake_case wire field and preserves exact decimal strings")
    void deserializesFullEvent() throws IOException {
        String json = """
                {
                  "exchange_id": 3,
                  "pair_id": 7,
                  "side": "asks",
                  "event_time": 1717171717000,
                  "price": "97240.50",
                  "quantity": "1.5"
                }
                """;

        PriceLevelEvent event = deserialize(json);

        assertThat(event.getExchangeId()).isEqualTo(3);
        assertThat(event.getPairId()).isEqualTo(7);
        assertThat(event.getSide()).isEqualTo("asks");
        assertThat(event.getEventTime()).isEqualTo(1717171717000L);
        assertThat(event.getPrice()).isEqualTo("97240.50");   // scale preserved, not normalized
        assertThat(event.getQuantity()).isEqualTo("1.5");
    }

    /**
     * Given a quantity 0 record (the R2 "remove this level" signal), When deserialized, Then the
     * "0" is preserved as its exact string for the operator to test via BigDecimal.signum. Pins
     * that the delete signal survives the wire decode intact.
     */
    @Test
    @DisplayName("preserves a quantity 0 (the R2 remove signal)")
    void deserializesZeroQuantity() throws IOException {
        String json = """
                {"exchange_id": 5, "pair_id": 7, "side": "bids",
                 "event_time": 1717171718000, "price": "97000", "quantity": "0"}
                """;

        PriceLevelEvent event = deserialize(json);

        assertThat(event.getSide()).isEqualTo("bids");
        assertThat(event.getPrice()).isEqualTo("97000");
        assertThat(event.getQuantity()).isEqualTo("0");
    }

    /**
     * Given a record that also carries the wire-only display fields the job ignores
     * ({@code exchange_name}, {@code base}, {@code quote}), When deserialized, Then they are
     * silently dropped and the known fields still bind. Protects the
     * {@code @JsonIgnoreProperties(ignoreUnknown = true)} contract: the producer may add fields
     * without breaking the Flink job.
     */
    @Test
    @DisplayName("ignores unknown wire fields (exchange_name/base/quote)")
    void ignoresUnknownFields() throws IOException {
        String json = """
                {
                  "exchange_id": 1,
                  "exchange_name": "binance",
                  "base": "BTC",
                  "quote": "USDT",
                  "pair_id": 7,
                  "side": "asks",
                  "event_time": 10,
                  "price": "100",
                  "quantity": "2"
                }
                """;

        PriceLevelEvent event = deserialize(json);

        assertThat(event.getExchangeId()).isEqualTo(1);
        assertThat(event.getSide()).isEqualTo("asks");
        assertThat(event.getPrice()).isEqualTo("100");
    }

    /**
     * Given the same deserializer instance used for two records, When both are decoded, Then both
     * succeed — the {@link com.fasterxml.jackson.databind.ObjectMapper} is created lazily on the
     * first call (it is {@code transient}, rebuilt after Flink ships the operator) and reused on
     * the second. Pins that the lazy-init guard does not rebuild or fail on reuse.
     */
    @Test
    @DisplayName("reuses the lazily-created mapper across calls")
    void reusesMapperAcrossCalls() throws IOException {
        String json = """
                {"exchange_id": 1, "pair_id": 7, "side": "asks",
                 "event_time": 10, "price": "100", "quantity": "2"}
                """;

        assertThat(deserialize(json).getExchangeId()).isEqualTo(1);
        assertThat(deserialize(json).getExchangeId()).isEqualTo(1);
    }

    /**
     * Given any element, When {@code isEndOfStream} is queried, Then it is always false — this is
     * an unbounded live stream that never signals completion. Pinned so the contract can't be
     * flipped accidentally (a true here would tear down the source mid-stream).
     */
    @Test
    @DisplayName("never reports end of stream")
    void isNeverEndOfStream() {
        assertThat(deserializer.isEndOfStream(new PriceLevelEvent())).isFalse();
    }

    /**
     * Given the deserializer, When its produced type is queried, Then it advertises
     * {@link PriceLevelEvent} so Flink builds the correct serializer for the source's output type.
     */
    @Test
    @DisplayName("advertises PriceLevelEvent as its produced type")
    void producesPriceLevelEventType() {
        assertThat(deserializer.getProducedType().getTypeClass()).isEqualTo(PriceLevelEvent.class);
    }
}
