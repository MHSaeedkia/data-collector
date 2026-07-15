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
}
