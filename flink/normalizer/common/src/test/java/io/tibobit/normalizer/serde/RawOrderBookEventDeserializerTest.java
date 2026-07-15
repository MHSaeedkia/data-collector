package io.tibobit.normalizer.serde;

import io.tibobit.normalizer.avro.AvroSchemaLoader;
import io.tibobit.normalizer.model.PriceLevel;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link RawOrderBookEventDeserializer#fromGenericRecord(GenericRecord)}: the wire record
 * (schemas/raw_order_book_event.avsc) must map back onto the POJO with the null-vs-empty side
 * semantics and nullable sequence_id intact. Round-trips through
 * {@link RawOrderBookEventSerializer#toGenericRecord} so serializer and deserializer can never
 * drift apart silently.
 */
class RawOrderBookEventDeserializerTest {

    private static final Schema SCHEMA = AvroSchemaLoader.load("/avro/raw_order_book_event.avsc");

    private final RawOrderBookEventDeserializer deserializer =
            new RawOrderBookEventDeserializer("http://unused:8082");

    private static RawOrderBookEvent roundTrip(RawOrderBookEvent event) {
        return RawOrderBookEventDeserializer.fromGenericRecord(
                RawOrderBookEventSerializer.toGenericRecord(event, SCHEMA));
    }

    /**
     * Given a full delta event, When round-tripped through the wire record, Then every field —
     * including exact level decimal strings — survives unchanged.
     */
    @Test
    @DisplayName("round-trips a full delta event unchanged")
    void roundTripsFullEvent() {
        RawOrderBookEvent out = roundTrip(new RawOrderBookEvent(8, 2, "update", 1752473005000L,
                300L, 1752473005123L,
                List.of(new PriceLevel("62775.5", "0.031418")),
                List.of(new PriceLevel("62774.90", "0"))));

        assertThat(out.getExchangeId()).isEqualTo(8);
        assertThat(out.getPairId()).isEqualTo(2);
        assertThat(out.getType()).isEqualTo("update");
        assertThat(out.getSequenceId()).isEqualTo(1752473005000L);
        assertThat(out.getSequenceJump()).isEqualTo(300L);
        assertThat(out.getEventTime()).isEqualTo(1752473005123L);
        assertThat(out.getAsks()).hasSize(1);
        assertThat(out.getAsks().get(0).getPrice()).isEqualTo("62775.5");
        assertThat(out.getAsks().get(0).getQuantity()).isEqualTo("0.031418");
        assertThat(out.getBids().get(0).getPrice()).isEqualTo("62774.90"); // scale preserved
        assertThat(out.getBids().get(0).getQuantity()).isEqualTo("0");     // delete signal preserved
    }

    /**
     * Given an ex3-style per-side snapshot (bids null, sequence_id null) whose asks are EMPTY,
     * When round-tripped, Then null stays null and empty stays empty — conflating them would
     * make the book builder clear a side it was never told about.
     */
    @Test
    @DisplayName("keeps null side distinct from empty side through the round-trip")
    void keepsNullVsEmptyDistinct() {
        RawOrderBookEvent out = roundTrip(new RawOrderBookEvent(3, 1, "snapshot", null, 0L,
                1752473005123L, List.of(), null));

        assertThat(out.getSequenceId()).isNull();
        assertThat(out.getAsks()).isNotNull().isEmpty();
        assertThat(out.getBids()).isNull();
    }

    /**
     * Given any element, When {@code isEndOfStream} is queried, Then it is always false — an
     * unbounded live stream must never signal completion.
     */
    @Test
    @DisplayName("never reports end of stream")
    void isNeverEndOfStream() {
        assertThat(deserializer.isEndOfStream(new RawOrderBookEvent())).isFalse();
    }

    /**
     * Given the deserializer, When its produced type is queried, Then it advertises
     * {@link RawOrderBookEvent} so Flink builds the correct serializer for the source output.
     */
    @Test
    @DisplayName("advertises RawOrderBookEvent as its produced type")
    void producesRawOrderBookEventType() {
        assertThat(deserializer.getProducedType().getTypeClass()).isEqualTo(RawOrderBookEvent.class);
    }
}
