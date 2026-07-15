package io.tibobit.normalizer.serde;

import io.tibobit.normalizer.avro.AvroSchemaLoader;
import io.tibobit.normalizer.model.PriceLevel;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link RawOrderBookEventSerializer#toGenericRecord} against the wire contract
 * (schemas/raw_order_book_event.avsc). Pins the null-vs-empty side semantics and the nullable
 * sequence_id — the schema's two deliberate design decisions (memory/project_avro_schema.md).
 * The Confluent registry round-trip itself (GenericRecord -> bytes) is library code, not
 * asserted here — only our mapping onto the GenericRecord.
 */
class RawOrderBookEventSerializerTest {

    private static final Schema SCHEMA = AvroSchemaLoader.load("/avro/raw_order_book_event.avsc");

    /**
     * Given a delta event with both sides and a sequence, When mapped, Then every field lands on
     * its wire name, the type enum is a real Avro EnumSymbol, and level decimal strings are
     * preserved exactly.
     */
    @Test
    @DisplayName("maps a full delta event onto the wire record")
    void mapsFullEvent() {
        RawOrderBookEvent event = new RawOrderBookEvent(6, 1, "update", 126776812L, 1L,
                1752473005123L,
                List.of(new PriceLevel("62775.5", "0.031418")),
                List.of(new PriceLevel("62774.9", "0"), new PriceLevel("62770.10", "1.5")));

        GenericRecord record = RawOrderBookEventSerializer.toGenericRecord(event, SCHEMA);

        assertThat(record.get("exchange_id")).isEqualTo(6);
        assertThat(record.get("pair_id")).isEqualTo(1);
        assertThat(record.get("type")).isInstanceOf(GenericData.EnumSymbol.class)
                .hasToString("update");
        assertThat(record.get("sequence_id")).isEqualTo(126776812L);
        assertThat(record.get("sequence_jump")).isEqualTo(1L);
        assertThat(record.get("event_time")).isEqualTo(1752473005123L);
        List<?> asks = (List<?>) record.get("asks");
        assertThat(((GenericRecord) asks.get(0)).get("price")).isEqualTo("62775.5");
        assertThat(((GenericRecord) asks.get(0)).get("quantity")).isEqualTo("0.031418");
        List<?> bids = (List<?>) record.get("bids");
        assertThat(((GenericRecord) bids.get(0)).get("quantity")).isEqualTo("0"); // delete signal survives
        assertThat(((GenericRecord) bids.get(1)).get("price")).isEqualTo("62770.10"); // scale preserved
    }

    /**
     * Given an ex3-style per-side snapshot (bids absent, no ordering field), When mapped, Then
     * the absent side and sequence_id stay null on the wire — null means "not part of this
     * event", which downstream must distinguish from an empty side.
     */
    @Test
    @DisplayName("keeps an absent side and absent sequence_id as wire nulls")
    void keepsAbsentSideNull() {
        RawOrderBookEvent event = new RawOrderBookEvent(3, 1, "snapshot", null, 0L,
                1752473005123L, List.of(new PriceLevel("62775.5", "1")), null);

        GenericRecord record = RawOrderBookEventSerializer.toGenericRecord(event, SCHEMA);

        assertThat(record.get("sequence_id")).isNull();
        assertThat(record.get("asks")).isNotNull();
        assertThat(record.get("bids")).isNull();
    }

    /**
     * Given a snapshot whose exchange reported one side empty, When mapped, Then that side is an
     * EMPTY array — not null. The other half of the null-vs-empty contract.
     */
    @Test
    @DisplayName("keeps a reported-empty side as an empty array, not null")
    void keepsEmptySideEmpty() {
        RawOrderBookEvent event = new RawOrderBookEvent(1, 1, "snapshot", 42L, 0L,
                1752473005123L, List.of(), List.of(new PriceLevel("62770", "1")));

        GenericRecord record = RawOrderBookEventSerializer.toGenericRecord(event, SCHEMA);

        assertThat(record.get("asks")).isNotNull();
        assertThat((List<?>) record.get("asks")).isEmpty();
    }
}
