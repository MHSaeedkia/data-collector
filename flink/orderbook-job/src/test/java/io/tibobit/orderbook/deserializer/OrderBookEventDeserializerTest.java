package io.tibobit.orderbook.deserializer;

import io.tibobit.orderbook.model.OrderBookEvent;
import io.tibobit.orderbook.model.OrderBookEventType;
import io.tibobit.orderbook.model.PriceLevel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link OrderBookEventDeserializer} against the wire contract: the JSON value bytes of
 * an input Kafka record (see schemas/orderbook_event.avsc and the {@code @JsonProperty}
 * mappings on {@link OrderBookEvent}) must decode into a fully populated event. This pins the
 * snake_case → camelCase field mapping, the {@code type} enum binding, the nested
 * {@code levels}, and the "ignore unknown wire fields" rule — any drift here silently breaks
 * every downstream stage, so it is asserted directly on the bytes the source actually receives.
 */
class OrderBookEventDeserializerTest {

    private final OrderBookEventDeserializer deserializer = new OrderBookEventDeserializer();

    private OrderBookEvent deserialize(String json) throws IOException {
        return deserializer.deserialize(json.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Given a full snapshot JSON record as produced upstream, When deserialized, Then every
     * snake_case wire field maps onto its camelCase POJO field, the {@code type} string binds
     * to the enum, and the nested {@code levels} array becomes {@link PriceLevel}s with their
     * exact decimal strings preserved. This is the happy-path contract the whole job depends on.
     */
    @Test
    @DisplayName("maps every snake_case wire field, the type enum, and nested levels")
    void deserializesFullSnapshot() throws IOException {
        String json = """
                {
                  "exchange_id": 3,
                  "pair_id": 7,
                  "side": "asks",
                  "type": "snapshot",
                  "event_time": 1717171717000,
                  "sequence_id": 1000,
                  "sequence_jump": 0,
                  "levels": [
                    {"price": "97240.50", "quantity": "1.5"},
                    {"price": "97241.00", "quantity": "0.25"}
                  ]
                }
                """;

        OrderBookEvent event = deserialize(json);

        assertThat(event.getExchangeId()).isEqualTo(3);
        assertThat(event.getPairId()).isEqualTo(7);
        assertThat(event.getSide()).isEqualTo("asks");
        assertThat(event.getType()).isEqualTo(OrderBookEventType.SNAPSHOT);
        assertThat(event.getEventTime()).isEqualTo(1717171717000L);
        assertThat(event.getSequenceId()).isEqualTo(1000L);
        assertThat(event.getSequenceJump()).isEqualTo(0L);
        assertThat(event.getLevels())
                .extracting(PriceLevel::getPrice, PriceLevel::getQuantity)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("97240.50", "1.5"),
                        org.assertj.core.groups.Tuple.tuple("97241.00", "0.25"));
    }

    /**
     * Given an update record carrying a non-zero {@code sequence_jump}, When deserialized, Then
     * the {@code type} binds to {@code UPDATE} and the jump is preserved. Updates are the only
     * events that carry a meaningful jump (snapshots are always 0), and the merger's sequence
     * validation is meaningless if either field is mis-mapped.
     */
    @Test
    @DisplayName("binds the update type and preserves a non-zero sequence_jump")
    void deserializesUpdateWithJump() throws IOException {
        String json = """
                {
                  "exchange_id": 5,
                  "pair_id": 7,
                  "side": "bids",
                  "type": "update",
                  "event_time": 1717171718000,
                  "sequence_id": 1300,
                  "sequence_jump": 300,
                  "levels": [{"price": "97000", "quantity": "0"}]
                }
                """;

        OrderBookEvent event = deserialize(json);

        assertThat(event.getType()).isEqualTo(OrderBookEventType.UPDATE);
        assertThat(event.getSequenceId()).isEqualTo(1300L);
        assertThat(event.getSequenceJump()).isEqualTo(300L);
        assertThat(event.getLevels()).singleElement()
                .extracting(PriceLevel::getPrice, PriceLevel::getQuantity)
                .containsExactly("97000", "0");
    }

    /**
     * Given a record that also carries the wire-only fields the job ignores ({@code
     * exchange_name}, {@code base}, {@code quote} — see the {@link OrderBookEvent} Javadoc),
     * When deserialized, Then they are silently dropped and the known fields still bind. This
     * protects the {@code @JsonIgnoreProperties(ignoreUnknown = true)} contract: the producer
     * may add fields without breaking the Flink job.
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
                  "type": "snapshot",
                  "event_time": 10,
                  "sequence_id": 1,
                  "sequence_jump": 0,
                  "levels": []
                }
                """;

        OrderBookEvent event = deserialize(json);

        assertThat(event.getExchangeId()).isEqualTo(1);
        assertThat(event.getSide()).isEqualTo("asks");
        assertThat(event.getLevels()).isEmpty();
    }

    /**
     * Given a record whose {@code type} is not one of the known wire values, When deserialized,
     * Then the enum's {@code @JsonCreator} rejects it with {@link IllegalArgumentException}
     * (wrapped by Jackson). A malformed type must fail loudly rather than decode to a null type
     * the merger would then mis-branch on.
     */
    @Test
    @DisplayName("rejects an unknown type value")
    void rejectsUnknownType() {
        String json = """
                {"exchange_id": 1, "pair_id": 7, "side": "asks", "type": "delta",
                 "event_time": 10, "sequence_id": 1, "sequence_jump": 0, "levels": []}
                """;

        assertThatThrownBy(() -> deserialize(json))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .rootCause()
                .hasMessageContaining("Unknown type: delta");
    }

    /**
     * Given a record with no {@code levels} key at all, When deserialized, Then {@code levels}
     * is null (not an empty list). The merger explicitly guards {@code getLevels() != null}, so
     * this documents the shape that guard exists for.
     */
    @Test
    @DisplayName("leaves levels null when the field is absent")
    void absentLevelsStaysNull() throws IOException {
        String json = """
                {"exchange_id": 1, "pair_id": 7, "side": "asks", "type": "snapshot",
                 "event_time": 10, "sequence_id": 1, "sequence_jump": 0}
                """;

        OrderBookEvent event = deserialize(json);

        assertThat(event.getLevels()).isNull();
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
                {"exchange_id": 1, "pair_id": 7, "side": "asks", "type": "snapshot",
                 "event_time": 10, "sequence_id": 1, "sequence_jump": 0, "levels": []}
                """;

        assertThat(deserialize(json).getExchangeId()).isEqualTo(1);
        assertThat(deserialize(json).getExchangeId()).isEqualTo(1);
    }

    /**
     * Given any element, When {@code isEndOfStream} is queried, Then it is always false — this
     * is an unbounded live stream that never signals completion. Pinned so the contract can't be
     * flipped accidentally (a true here would tear down the source mid-stream).
     */
    @Test
    @DisplayName("never reports end of stream")
    void isNeverEndOfStream() {
        assertThat(deserializer.isEndOfStream(new OrderBookEvent())).isFalse();
    }

    /**
     * Given the deserializer, When its produced type is queried, Then it advertises
     * {@link OrderBookEvent} so Flink builds the correct serializer for the source's output type.
     */
    @Test
    @DisplayName("advertises OrderBookEvent as its produced type")
    void producesOrderBookEventType() {
        assertThat(deserializer.getProducedType().getTypeClass()).isEqualTo(OrderBookEvent.class);
    }
}
