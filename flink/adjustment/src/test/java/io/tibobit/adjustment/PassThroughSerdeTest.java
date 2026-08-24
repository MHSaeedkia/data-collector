package io.tibobit.adjustment;

import java.io.ByteArrayOutputStream;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

/**
 * Step 1 writes the record back out unchanged, so the ONLY thing that can break
 * it is the decode → re-encode round trip quietly losing a field. These tests
 * exist for that and little else.
 *
 * <p>
 * Every fixture value is deliberately NON-default ({@code simulation} nonzero,
 * ids non-empty), because a field this job's model forgot would come back as
 * the schema's default and a fixture built from defaults would agree with it.
 *
 * <p>
 * Run against the CANONICAL schema in schemas/ (copied onto the test classpath
 * by the pom), not a checked-in copy, so a schema edit that this job does not
 * carry fails the build.
 */
class PassThroughSerdeTest {

    private static final Schema AGGREGATED = AvroSchemaLoader.load("/avro/aggregated_order_book_event.avsc");

    /**
     * Strings are deliberately Utf8, exactly as Avro hands them back off the
     * wire.
     */
    private static GenericRecord aggregatedRecord(String side) {
        Schema levelSchema = AGGREGATED.getField("levels").schema().getElementType();
        GenericRecord first = new GenericRecordBuilder(levelSchema)
                .set("exchange_id", 6)
                .set("simulation", 1)
                .set("source_id", new Utf8("snapshot-A"))
                .set("price", new Utf8("62650.00"))
                .set("quantity", new Utf8("0.50000000"))
                .build();
        GenericRecord second = new GenericRecordBuilder(levelSchema)
                .set("exchange_id", 8)
                .set("simulation", 1)
                .set("source_id", new Utf8("snapshot-B"))
                .set("price", new Utf8("62650.00"))
                .set("quantity", new Utf8("1.25"))
                .build();

        return new GenericRecordBuilder(AGGREGATED)
                .set("pair_id", 3)
                .set("side", new GenericData.EnumSymbol(AGGREGATED.getField("side").schema(), side))
                .set("id", new Utf8("agg-id-1"))
                .set("event_time", 1750680000000L)
                .set("levels", List.of(first, second))
                .build();
    }

    private static GenericRecord roundTrip(GenericRecord in) {
        return AggregatedOrderBookSerializer.toGenericRecord(
                AggregatedOrderBookDeserializer.fromGenericRecord(in), AGGREGATED);
    }

    @Test
    void losslessRoundTrip() {
        GenericRecord out = roundTrip(aggregatedRecord("asks"));

        assertThat(out.get("pair_id")).isEqualTo(3);
        assertThat(out.get("side")).hasToString("asks");
        assertThat(out.get("id")).hasToString("agg-id-1");
        assertThat(out.get("event_time")).isEqualTo(1750680000000L);

        @SuppressWarnings("unchecked")
        List<GenericRecord> levels = (List<GenericRecord>) out.get("levels");
        assertThat(levels).hasSize(2);

        // Per-level fields are where a forgotten field hides: simulation would come back 0 and
        // source_id "" — both valid-looking values that a live record could legitimately hold.
        assertThat(levels.get(0).get("exchange_id")).isEqualTo(6);
        assertThat(levels.get(0).get("simulation")).isEqualTo(1);
        assertThat(levels.get(0).get("source_id")).hasToString("snapshot-A");
        assertThat(levels.get(0).get("price")).hasToString("62650.00");
        assertThat(levels.get(0).get("quantity")).hasToString("0.50000000");

        assertThat(levels.get(1).get("exchange_id")).isEqualTo(8);
        assertThat(levels.get(1).get("source_id")).hasToString("snapshot-B");
        assertThat(levels.get(1).get("quantity")).hasToString("1.25");
    }

    /**
     * The price/quantity STRINGS must survive character for character. Job 6
     * emits whatever the upstream chain produced, trailing zeros and all, and
     * this job is not allowed to canonicalize them — the merger does, and that
     * is a documented difference between the two topics.
     */
    @Test
    void priceStringsAreNotCanonicalised() {
        GenericRecord out = roundTrip(aggregatedRecord("bids"));

        @SuppressWarnings("unchecked")
        List<GenericRecord> levels = (List<GenericRecord>) out.get("levels");
        assertThat(levels.get(0).get("price")).hasToString("62650.00");
        assertThat(levels.get(0).get("quantity")).hasToString("0.50000000");
    }

    @Test
    void bothSideSymbolsSurvive() {
        assertThat(roundTrip(aggregatedRecord("asks")).get("side")).hasToString("asks");
        assertThat(roundTrip(aggregatedRecord("bids")).get("side")).hasToString("bids");
    }

    /**
     * {@code side} is an Avro ENUM. Writing a plain String there passes a
     * casual reading and then NPEs inside the serializer at the first emit —
     * the exact shape of the live bug that killed job 2 when the {@code reset}
     * symbol was added (memory/project_type_validator.md).
     */
    @Test
    void sideIsWrittenAsAnEnumSymbolNotAString() {
        Object side = roundTrip(aggregatedRecord("asks")).get("side");

        assertThat(side).isInstanceOf(GenericData.EnumSymbol.class);
        assertThat(GenericData.get().validate(AGGREGATED, roundTrip(aggregatedRecord("asks")))).isTrue();
    }

    /**
     * A real Avro binary round-trip. {@code GenericData.validate} is a shape
     * check and would still pass for a record the encoder rejects, so the bytes
     * are actually written and read back.
     */
    @Test
    void survivesABinaryRoundTrip() throws Exception {
        GenericRecord written = roundTrip(aggregatedRecord("asks"));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(AGGREGATED).write(written, encoder);
        encoder.flush();

        GenericRecord read = new GenericDatumReader<GenericRecord>(AGGREGATED)
                .read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));

        assertThat(read.get("pair_id")).isEqualTo(3);
        assertThat(read.get("id")).hasToString("agg-id-1");
        assertThat(read.get("event_time")).isEqualTo(1750680000000L);
        @SuppressWarnings("unchecked")
        List<GenericRecord> levels = (List<GenericRecord>) read.get("levels");
        assertThat(levels).hasSize(2);
        assertThat(levels.get(0).get("simulation")).isEqualTo(1);
        assertThat(levels.get(0).get("source_id")).hasToString("snapshot-A");
    }

    /**
     * An emptied book is a real record, not an edge case: a gap makes job 2
     * emit a reset, job 5 empties the book and job 6 publishes an aggregated
     * record with no levels (memory/project_type_validator.md). It has to pass
     * through like any other.
     */
    @Test
    void anEmptyBookPassesThrough() {
        GenericRecord empty = new GenericRecordBuilder(AGGREGATED)
                .set("pair_id", 1)
                .set("side", new GenericData.EnumSymbol(AGGREGATED.getField("side").schema(), "asks"))
                .set("id", new Utf8("agg-id-empty"))
                .set("event_time", 1750680000000L)
                .set("levels", List.of())
                .build();

        GenericRecord out = roundTrip(empty);

        assertThat((List<?>) out.get("levels")).isEmpty();
        assertThat(out.get("id")).hasToString("agg-id-empty");
        assertThat(GenericData.get().validate(AGGREGATED, out)).isTrue();
    }
}
