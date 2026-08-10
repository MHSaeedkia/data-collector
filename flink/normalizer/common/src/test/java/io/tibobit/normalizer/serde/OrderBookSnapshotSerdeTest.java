package io.tibobit.normalizer.serde;

import io.tibobit.normalizer.avro.AvroSchemaLoader;
import io.tibobit.normalizer.model.OrderBookSnapshot;
import io.tibobit.normalizer.model.PriceLevel;
import org.apache.avro.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trip tests for {@link OrderBookSnapshotSerializer#toGenericRecord} +
 * {@link OrderBookSnapshotDeserializer#fromGenericRecord} against
 * schemas/order_book_snapshot.avsc (job-5 output / job-6 input). Unlike RawOrderBookEvent both
 * sides are REQUIRED here — a built book always has both, possibly empty.
 */
class OrderBookSnapshotSerdeTest {

    private static final Schema SCHEMA = AvroSchemaLoader.load("/avro/order_book_snapshot.avsc");

    private final OrderBookSnapshotDeserializer deserializer =
            new OrderBookSnapshotDeserializer("http://unused:8082");

    private static OrderBookSnapshot roundTrip(OrderBookSnapshot snapshot) {
        return OrderBookSnapshotDeserializer.fromGenericRecord(
                OrderBookSnapshotSerializer.toGenericRecord(snapshot, SCHEMA));
    }

    /**
     * Given a full book with a sequence position, When round-tripped through the wire record,
     * Then every field and every exact level decimal string survives unchanged.
     */
    @Test
    @DisplayName("round-trips a full book unchanged")
    void roundTripsFullBook() {
        OrderBookSnapshot out = roundTrip(new OrderBookSnapshot(6, 1, 1752473005123L, 126776812L,
                List.of(new PriceLevel("62775.5", "0.031418"), new PriceLevel("62776.00", "2")),
                List.of(new PriceLevel("62774.9", "1.5"))));

        assertThat(out.getExchangeId()).isEqualTo(6);
        assertThat(out.getPairId()).isEqualTo(1);
        assertThat(out.getEventTime()).isEqualTo(1752473005123L);
        assertThat(out.getLastSequenceId()).isEqualTo(126776812L);
        assertThat(out.getAsks()).hasSize(2);
        assertThat(out.getAsks().get(1).getPrice()).isEqualTo("62776.00"); // scale preserved
        assertThat(out.getBids()).hasSize(1);
        assertThat(out.getBids().get(0).getQuantity()).isEqualTo("1.5");
    }

    /**
     * Given an ex3-style book (no ordering field) with an empty bids side, When round-tripped,
     * Then last_sequence_id stays null and the empty side stays an empty list — required sides
     * are always present, even when empty.
     */
    @Test
    @DisplayName("keeps a null last_sequence_id and an empty required side")
    void keepsNullSequenceAndEmptySide() {
        OrderBookSnapshot out = roundTrip(new OrderBookSnapshot(3, 1, 1752473005123L, null,
                List.of(new PriceLevel("62775.5", "1")), List.of()));

        assertThat(out.getLastSequenceId()).isNull();
        assertThat(out.getAsks()).hasSize(1);
        assertThat(out.getBids()).isNotNull().isEmpty();
    }

    /**
     * Given a book carrying timings accumulated through jobs 1–5, When round-tripped, Then the
     * set stages survive and the not-yet-run stages stay null.
     */
    @Test
    @DisplayName("round-trips pipeline_timings through the book builder's output")
    void roundTripsPipelineTimings() {
        OrderBookSnapshot in = new OrderBookSnapshot(6, 1, 123L, 1L, List.of(), List.of());
        in.getPipelineTimings().setPairExtractIn(140L);
        in.getPipelineTimings().setBookBuildOut(182L);

        OrderBookSnapshot out = roundTrip(in);

        assertThat(out.getPipelineTimings().getPairExtractIn()).isEqualTo(140L);
        assertThat(out.getPipelineTimings().getBookBuildOut()).isEqualTo(182L);
    }

    /**
     * The record's own lineage: its id, and the single event that triggered the emit. Everything
     * else about where this book came from lives on the levels.
     */
    @Test
    @DisplayName("round-trips the record id and trigger_id")
    void roundTripsRecordLineage() {
        OrderBookSnapshot in = new OrderBookSnapshot(6, 1, 123L, 1L, List.of(), List.of());
        in.setId("66666666-6666-4666-8666-666666666666");
        in.setTriggerId("55555555-5555-4555-8555-555555555555");

        OrderBookSnapshot out = roundTrip(in);

        assertThat(out.getId()).isEqualTo("66666666-6666-4666-8666-666666666666");
        assertThat(out.getTriggerId()).isEqualTo("55555555-5555-4555-8555-555555555555");
    }

    /**
     * The per-level lineage: levels of one book routinely come from DIFFERENT events, so each
     * source_id has to survive attached to its own level rather than as a set on the record.
     */
    @Test
    @DisplayName("round-trips a per-level source_id on both sides")
    void roundTripsPerLevelSources() {
        OrderBookSnapshot out = roundTrip(new OrderBookSnapshot(6, 1, 123L, 1L,
                List.of(new PriceLevel("62775.5", "1", "ev-a"),
                        new PriceLevel("62776.0", "2", "ev-b")),
                List.of(new PriceLevel("62774.9", "3", "ev-a"))));

        assertThat(out.getAsks()).extracting(PriceLevel::getSourceId)
                .containsExactly("ev-a", "ev-b");
        assertThat(out.getBids()).extracting(PriceLevel::getSourceId).containsExactly("ev-a");
    }

    /**
     * source_id is non-null in the schema, so an unstamped level has to come back as "" rather
     * than blowing up the sink — a missing stamp is a bug, but one worth seeing on the topic.
     */
    @Test
    @DisplayName("an unstamped level serializes as a blank source_id")
    void unstampedLevelBecomesBlank() {
        OrderBookSnapshot out = roundTrip(new OrderBookSnapshot(6, 1, 123L, 1L,
                List.of(new PriceLevel("62775.5", "1")), List.of()));

        assertThat(out.getAsks().get(0).getSourceId()).isEmpty();
    }

    /**
     * Given the deserializer, When queried, Then it never ends the stream and advertises
     * {@link OrderBookSnapshot} as its produced type.
     */
    @Test
    @DisplayName("never ends the stream and advertises OrderBookSnapshot")
    void streamContract() {
        assertThat(deserializer.isEndOfStream(new OrderBookSnapshot())).isFalse();
        assertThat(deserializer.getProducedType().getTypeClass()).isEqualTo(OrderBookSnapshot.class);
    }
}
