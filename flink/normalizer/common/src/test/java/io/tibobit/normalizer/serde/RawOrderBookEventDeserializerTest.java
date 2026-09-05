package io.tibobit.normalizer.serde;

import io.tibobit.normalizer.avro.AvroSchemaLoader;
import io.tibobit.normalizer.model.PriceLevel;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
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
     * Given an event stamped by job 1 only, When round-tripped, Then the pair-extract timings
     * survive and every unreached stage stays null — jobs 2–5 read these to measure latency.
     */
    @Test
    @DisplayName("round-trips pipeline_timings, leaving unreached stages null")
    void roundTripsPipelineTimings() {
        RawOrderBookEvent in = new RawOrderBookEvent(6, 1, "update", 1L, 1L, 123L,
                List.of(), List.of());
        in.getPipelineTimings().setPairExtractIn(140L);
        in.getPipelineTimings().setPairExtractOut(142L);

        RawOrderBookEvent out = roundTrip(in);

        assertThat(out.getPipelineTimings().getPairExtractIn()).isEqualTo(140L);
        assertThat(out.getPipelineTimings().getPairExtractOut()).isEqualTo(142L);
        assertThat(out.getPipelineTimings().getTypeValidateIn()).isNull();
    }

    /**
     * Given events flagged live, simulated and with an as-yet-undefined value, When round-tripped,
     * Then the flag survives verbatim — the wire must not reinterpret or clamp it.
     */
    @Test
    @DisplayName("round-trips the simulation flag verbatim, including undefined values")
    void roundTripsSimulationFlag() {
        RawOrderBookEvent live = new RawOrderBookEvent(6, 1, "update", 1L, 1L, 123L,
                List.of(), List.of());
        assertThat(roundTrip(live).getSimulation()).isZero();

        RawOrderBookEvent simulated = new RawOrderBookEvent(6, 1, "update", 1L, 1L, 123L,
                List.of(), List.of());
        simulated.setSimulation(1);
        assertThat(roundTrip(simulated).getSimulation()).isEqualTo(1);

        RawOrderBookEvent undefined = new RawOrderBookEvent(6, 1, "update", 1L, 1L, 123L,
                List.of(), List.of());
        undefined.setSimulation(7);
        assertThat(roundTrip(undefined).getSimulation()).isEqualTo(7);
    }

    /**
     * Given an event carrying lineage, When round-tripped, Then its id and its single source
     * survive on their wire names.
     */
    @Test
    @DisplayName("round-trips id and source_ids")
    void roundTripsLineage() {
        RawOrderBookEvent event = new RawOrderBookEvent(6, 1, "update", 1L, 1L, 123L,
                List.of(), List.of());
        event.setId("22222222-2222-4222-8222-222222222222");
        event.setSourceIds(List.of("11111111-1111-4111-8111-111111111111"));

        RawOrderBookEvent out = roundTrip(event);

        assertThat(out.getId()).isEqualTo("22222222-2222-4222-8222-222222222222");
        assertThat(out.getSourceIds()).containsExactly("11111111-1111-4111-8111-111111111111");
    }

    /**
     * A record written before these fields existed reads as the schema defaults rather than null —
     * an empty id and no sources, which downstream can recognise as "no lineage" instead of
     * NPE-ing on it.
     */
    @Test
    @DisplayName("a record with no lineage reads as the empty defaults, not null")
    void missingLineageReadsAsDefaults() {
        RawOrderBookEvent out = roundTrip(new RawOrderBookEvent(6, 1, "update", 1L, 1L, 123L,
                List.of(), List.of()));

        assertThat(out.getId()).isEmpty();
        assertThat(out.getSourceIds()).isEmpty();
    }

    /**
     * A REAL Avro decode hands back {@link Utf8}, not String. This is the one case the in-memory
     * round trip above cannot reach — a GenericRecordBuilder stores whatever object it was given, so
     * the String goes in and comes back out as a String and the bug hides. A Utf8 left unconverted
     * would compare unequal to every String it should match while still printing identically in a
     * log, so it is asserted explicitly here.
     */
    @Test
    @DisplayName("converts Utf8 lineage values to String, as a real decode produces")
    void convertsUtf8LineageToString() {
        GenericRecord record = RawOrderBookEventSerializer.toGenericRecord(
                new RawOrderBookEvent(6, 1, "update", 1L, 1L, 123L, List.of(), List.of()), SCHEMA);
        record.put("id", new Utf8("22222222-2222-4222-8222-222222222222"));
        record.put("source_ids", List.of(new Utf8("11111111-1111-4111-8111-111111111111")));

        RawOrderBookEvent out = RawOrderBookEventDeserializer.fromGenericRecord(record);

        assertThat(out.getId())
                .isInstanceOf(String.class)
                .isEqualTo("22222222-2222-4222-8222-222222222222");
        assertThat(out.getSourceIds())
                .containsExactly("11111111-1111-4111-8111-111111111111")
                .allSatisfy(id -> assertThat(id).isInstanceOf(String.class));
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
