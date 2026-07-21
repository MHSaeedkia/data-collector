package io.tibobit.orderbook.serializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tibobit.orderbook.model.ConsolidatedLevel;
import io.tibobit.orderbook.model.ConsolidatedOrderBook;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link ConsolidatedOrderBookSerializer} against the output wire contract: the bytes it
 * produces are exactly what the {side}-p{pair_id} topic carries and what the web UI parses.
 * This pins the snake_case key names ({@code pair_id}, {@code event_time}, {@code exchange_id})
 * and the level shape, decoupled from the Java field names — a renamed JSON key would break
 * every consumer silently, so it is asserted on the emitted JSON tree rather than via round-trip.
 */
class ConsolidatedOrderBookSerializerTest {

    private final ConsolidatedOrderBookSerializer serializer = new ConsolidatedOrderBookSerializer();
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Given a consolidated book with one level, When serialized, Then the JSON uses the
     * snake_case keys consumers expect ({@code pair_id}, {@code event_time}, {@code exchange_id})
     * and carries the side, the decimal price/quantity strings verbatim, and the event time.
     * This is the exact shape the output topic publishes.
     */
    @Test
    @DisplayName("emits snake_case keys and the exact level values")
    void serializesToWireShape() throws IOException {
        ConsolidatedOrderBook book = new ConsolidatedOrderBook(
                7, "asks",
                List.of(new ConsolidatedLevel(3, "97240.50", "1.5")),
                1717171717000L);

        JsonNode json = mapper.readTree(serializer.serialize(book));

        assertThat(json.get("pair_id").asInt()).isEqualTo(7);
        assertThat(json.get("side").asText()).isEqualTo("asks");
        assertThat(json.get("event_time").asLong()).isEqualTo(1717171717000L);
        JsonNode level = json.get("levels").get(0);
        assertThat(level.get("exchange_id").asInt()).isEqualTo(3);
        assertThat(level.get("price").asText()).isEqualTo("97240.50");
        assertThat(level.get("quantity").asText()).isEqualTo("1.5");
    }

    /**
     * Given a book with several levels, When serialized, Then the {@code levels} array preserves
     * their order. The merger emits levels already price-sorted, so the serializer must not
     * reorder them — the book the UI renders is exactly the order on the wire.
     */
    @Test
    @DisplayName("preserves level order")
    void preservesLevelOrder() throws IOException {
        ConsolidatedOrderBook book = new ConsolidatedOrderBook(
                7, "bids",
                List.of(
                        new ConsolidatedLevel(1, "97240", "1"),
                        new ConsolidatedLevel(2, "97239", "2"),
                        new ConsolidatedLevel(1, "97238", "3")),
                10L);

        JsonNode levels = mapper.readTree(serializer.serialize(book)).get("levels");

        assertThat(levels).hasSize(3);
        assertThat(levels.get(0).get("price").asText()).isEqualTo("97240");
        assertThat(levels.get(1).get("price").asText()).isEqualTo("97239");
        assertThat(levels.get(2).get("price").asText()).isEqualTo("97238");
    }

    /**
     * Given the same serializer instance used for two books, When both are serialized, Then both
     * succeed — the {@link ObjectMapper} is created lazily on the first call (it is {@code
     * transient}, rebuilt after Flink ships the operator) and reused on the second. Pins that the
     * lazy-init guard does not rebuild or fail on reuse.
     */
    @Test
    @DisplayName("reuses the lazily-created mapper across calls")
    void reusesMapperAcrossCalls() {
        ConsolidatedOrderBook book = new ConsolidatedOrderBook(7, "asks", List.of(), 0L);

        assertThat(serializer.serialize(book)).isNotEmpty();
        assertThat(serializer.serialize(book)).isNotEmpty();
    }

    /**
     * Given an empty book (no levels), When serialized, Then it emits an empty {@code levels}
     * array rather than failing or omitting the key. A pair+side with no current depth is a valid
     * state the UI must still receive.
     */
    @Test
    @DisplayName("serializes an empty book to an empty levels array")
    void serializesEmptyBook() throws IOException {
        ConsolidatedOrderBook book = new ConsolidatedOrderBook(7, "asks", List.of(), 0L);

        JsonNode json = mapper.readTree(serializer.serialize(book));

        assertThat(json.get("levels")).isEmpty();
    }
}
