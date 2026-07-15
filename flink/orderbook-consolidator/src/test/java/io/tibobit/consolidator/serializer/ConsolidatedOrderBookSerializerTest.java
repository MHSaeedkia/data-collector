package io.tibobit.consolidator.serializer;

import io.tibobit.consolidator.avro.AvroSchemaLoader;
import io.tibobit.consolidator.model.ConsolidatedLevel;
import io.tibobit.consolidator.model.ConsolidatedOrderBook;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link ConsolidatedOrderBookSerializer#toGenericRecord(ConsolidatedOrderBook, Schema)}
 * against the output wire contract: the Avro record it builds is exactly what
 * schemas/consolidated_order_book_event.avsc describes and what the p{pair_id}-{side} topic
 * carries. The Confluent registry encode itself (GenericRecord -> bytes) is Flink/Confluent
 * library code, not asserted here — only our mapping onto the GenericRecord.
 */
class ConsolidatedOrderBookSerializerTest {

    private static final Schema SCHEMA = AvroSchemaLoader.load("/avro/consolidated_order_book_event.avsc");

    /**
     * Given a consolidated book with one level, When mapped, Then the record carries the
     * pair_id, side, event_time, and the level's exchange_id/price/quantity verbatim. This is
     * the exact shape the output topic publishes.
     */
    @Test
    @DisplayName("maps pair_id, side, event_time and the exact level values")
    void mapsToWireShape() {
        ConsolidatedOrderBook book = new ConsolidatedOrderBook(
                7, "asks",
                List.of(new ConsolidatedLevel(3, "97240.50", "1.5")),
                1717171717000L);

        GenericRecord record = ConsolidatedOrderBookSerializer.toGenericRecord(book, SCHEMA);

        assertThat(record.get("pair_id")).isEqualTo(7);
        assertThat(record.get("side").toString()).isEqualTo("asks");
        assertThat(record.get("event_time")).isEqualTo(1717171717000L);
        @SuppressWarnings("unchecked")
        List<GenericRecord> levels = (List<GenericRecord>) record.get("levels");
        GenericRecord level = levels.get(0);
        assertThat(level.get("exchange_id")).isEqualTo(3);
        assertThat(level.get("price").toString()).isEqualTo("97240.50");
        assertThat(level.get("quantity").toString()).isEqualTo("1.5");
    }

    /**
     * Given a book with several levels, When mapped, Then the {@code levels} array preserves
     * their order. The merger emits levels already price-sorted, so the mapping must not
     * reorder them — the book the UI renders is exactly the order on the wire.
     */
    @Test
    @DisplayName("preserves level order")
    void preservesLevelOrder() {
        ConsolidatedOrderBook book = new ConsolidatedOrderBook(
                7, "bids",
                List.of(
                        new ConsolidatedLevel(1, "97240", "1"),
                        new ConsolidatedLevel(2, "97239", "2"),
                        new ConsolidatedLevel(1, "97238", "3")),
                10L);

        GenericRecord record = ConsolidatedOrderBookSerializer.toGenericRecord(book, SCHEMA);

        @SuppressWarnings("unchecked")
        List<GenericRecord> levels = (List<GenericRecord>) record.get("levels");
        assertThat(levels).hasSize(3);
        assertThat(levels.get(0).get("price").toString()).isEqualTo("97240");
        assertThat(levels.get(1).get("price").toString()).isEqualTo("97239");
        assertThat(levels.get(2).get("price").toString()).isEqualTo("97238");
    }

    /**
     * Given an empty book (no levels), When mapped, Then it produces an empty {@code levels}
     * array rather than failing or leaving the field null. A pair+side with no current depth is
     * a valid state the UI must still receive.
     */
    @Test
    @DisplayName("maps an empty book to an empty levels array")
    void mapsEmptyBook() {
        ConsolidatedOrderBook book = new ConsolidatedOrderBook(7, "asks", List.of(), 0L);

        GenericRecord record = ConsolidatedOrderBookSerializer.toGenericRecord(book, SCHEMA);

        assertThat((List<?>) record.get("levels")).isEmpty();
    }
}
