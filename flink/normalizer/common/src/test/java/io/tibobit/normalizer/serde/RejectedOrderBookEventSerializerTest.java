package io.tibobit.normalizer.serde;

import io.tibobit.normalizer.avro.AvroSchemaLoader;
import io.tibobit.normalizer.model.PriceLevel;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import io.tibobit.normalizer.model.RejectedOrderBookEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link RejectedOrderBookEventSerializer#toGenericRecord} against
 * schemas/rejected_order_book_event.avsc (job-2 dead-letter). The nested event record is built
 * by {@link RawOrderBookEventSerializer#toGenericRecord} against the INLINE RawOrderBookEvent
 * definition inside this schema — this test is what breaks first if the two copies of that
 * definition ever drift apart (they must stay field-for-field identical).
 */
class RejectedOrderBookEventSerializerTest {

    private static final Schema SCHEMA = AvroSchemaLoader.load("/avro/rejected_order_book_event.avsc");

    /**
     * Given a rejected delta event with a reason, When mapped, Then the envelope fields land on
     * their wire names and the nested event record carries the rejected event verbatim —
     * including its null bids side (dead-letter must preserve the event exactly for audit).
     */
    @Test
    @DisplayName("maps the envelope and nests the rejected event verbatim")
    void mapsEnvelopeAndNestedEvent() {
        RawOrderBookEvent event = new RawOrderBookEvent(6, 1, "update", 126776820L, 1L,
                1752473006456L, List.of(new PriceLevel("62775.5", "0.031418")), null);
        RejectedOrderBookEvent rejection = new RejectedOrderBookEvent(event,
                "sequence gap: expected 126776813, got 126776820", 1752473006460L);

        GenericRecord record = RejectedOrderBookEventSerializer.toGenericRecord(rejection, SCHEMA);

        assertThat(record.get("reject_reason"))
                .isEqualTo("sequence gap: expected 126776813, got 126776820");
        assertThat(record.get("rejected_at")).isEqualTo(1752473006460L);
        GenericRecord nested = (GenericRecord) record.get("event");
        assertThat(nested.get("exchange_id")).isEqualTo(6);
        assertThat(nested.get("sequence_id")).isEqualTo(126776820L);
        assertThat(nested.get("type")).hasToString("update");
        List<?> asks = (List<?>) nested.get("asks");
        assertThat(((GenericRecord) asks.get(0)).get("price")).isEqualTo("62775.5");
        assertThat(nested.get("bids")).isNull();
    }

    /**
     * Given an event rejected mid-validation, When mapped, Then the nested event's
     * pipeline_timings survives — pair-extract set, type-validate ingest set, but no
     * type-validate emit (it was rejected before emitting). Dead-letter must keep the timings
     * for latency audit exactly as they stood at rejection.
     */
    @Test
    @DisplayName("carries the rejected event's pipeline_timings in the nested record")
    void nestsPipelineTimings() {
        RawOrderBookEvent event = new RawOrderBookEvent(6, 1, "update", 1L, 1L, 123L,
                List.of(new PriceLevel("62775.5", "0.031418")), null);
        event.getPipelineTimings().setPairExtractIn(140L);
        event.getPipelineTimings().setTypeValidateIn(150L);
        RejectedOrderBookEvent rejection = new RejectedOrderBookEvent(event, "sequence gap", 160L);

        GenericRecord record = RejectedOrderBookEventSerializer.toGenericRecord(rejection, SCHEMA);

        GenericRecord nestedTimings =
                (GenericRecord) ((GenericRecord) record.get("event")).get("pipeline_timings");
        assertThat(nestedTimings.get("pair_extract_in")).isEqualTo(140L);
        assertThat(nestedTimings.get("type_validate_in")).isEqualTo(150L);
        assertThat(nestedTimings.get("type_validate_out")).isNull();
    }

    /**
     * Given a dead-letter record, When mapped, Then the ENVELOPE's lineage and the NESTED event's
     * lineage land on their own fields and stay distinct. They are two different records: the
     * envelope is the thing written to the dead-letter topic, the nested event is the thing being
     * recorded, and the envelope names it as its source. Collapsing the two would make the
     * dead-letter record claim to be the event it is reporting on.
     */
    @Test
    @DisplayName("keeps the envelope's lineage distinct from the nested event's")
    void mapsEnvelopeAndNestedLineageSeparately() {
        RawOrderBookEvent event = new RawOrderBookEvent(6, 1, "update", 1L, 1L, 123L,
                List.of(), null);
        event.setId("22222222-2222-4222-8222-222222222222");
        event.setSourceIds(List.of("11111111-1111-4111-8111-111111111111"));
        RejectedOrderBookEvent rejection = new RejectedOrderBookEvent(event, "sequence gap", 160L);
        rejection.setId("33333333-3333-4333-8333-333333333333");
        rejection.setSourceIds(List.of(event.getId()));

        GenericRecord record = RejectedOrderBookEventSerializer.toGenericRecord(rejection, SCHEMA);

        assertThat(record.get("id")).isEqualTo("33333333-3333-4333-8333-333333333333");
        assertThat(record.get("source_ids"))
                .isEqualTo(List.of("22222222-2222-4222-8222-222222222222"));
        GenericRecord nested = (GenericRecord) record.get("event");
        assertThat(nested.get("id")).isEqualTo("22222222-2222-4222-8222-222222222222");
        assertThat(nested.get("source_ids"))
                .isEqualTo(List.of("11111111-1111-4111-8111-111111111111"));
    }
}
