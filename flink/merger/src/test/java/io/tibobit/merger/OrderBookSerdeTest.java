package io.tibobit.merger;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the two wire boundaries against the CANONICAL schemas in schemas/ (copied onto the test
 * classpath by the pom), without a live Schema Registry.
 */
class OrderBookSerdeTest {

    private static final Schema AGGREGATED = AvroSchemaLoader.load("/avro/aggregated_order_book_event.avsc");
    private static final Schema MERGED = AvroSchemaLoader.load("/avro/merged_order_book_event.avsc");

    /** Strings are deliberately Utf8, exactly as Avro hands them back off the wire. */
    private static GenericRecord aggregatedRecord() {
        Schema levelSchema = AGGREGATED.getField("levels").schema().getElementType();
        GenericRecord first = new GenericRecordBuilder(levelSchema)
                .set("exchange_id", 1)
                .set("simulation", 0)
                .set("source_id", new Utf8("A"))
                .set("price", new Utf8("10"))
                .set("quantity", new Utf8("3"))
                .build();
        GenericRecord second = new GenericRecordBuilder(levelSchema)
                .set("exchange_id", 2)
                .set("simulation", 0)
                .set("source_id", new Utf8("B"))
                .set("price", new Utf8("10"))
                .set("quantity", new Utf8("4"))
                .build();

        return new GenericRecordBuilder(AGGREGATED)
                .set("pair_id", 1)
                .set("side", new GenericData.EnumSymbol(AGGREGATED.getField("side").schema(), "asks"))
                .set("id", new Utf8("agg-id"))
                .set("event_time", 1750680000000L)
                .set("levels", List.of(first, second))
                .build();
    }

    @Test
    void decodesAnAggregatedRecordConvertingUtf8ToString() {
        AggregatedOrderBook book = AggregatedOrderBookDeserializer.fromGenericRecord(aggregatedRecord());

        assertThat(book.getPairId()).isEqualTo(1);
        assertThat(book.getSide()).isEqualTo("asks");
        assertThat(book.getId()).isEqualTo("agg-id");
        assertThat(book.getEventTime()).isEqualTo(1750680000000L);
        assertThat(book.getLevels()).hasSize(2);

        AggregatedLevel level = book.getLevels().get(0);
        assertThat(level.getExchangeId()).isEqualTo(1);
        assertThat(level.getSimulation()).isZero();
        // Utf8 would print identically but compare unequal — assert the real type, not just the text.
        assertThat(level.getSourceId()).isInstanceOf(String.class).isEqualTo("A");
        assertThat(level.getPrice()).isInstanceOf(String.class).isEqualTo("10");
        assertThat(level.getQuantity()).isEqualTo("3");
    }

    @Test
    void encodesAMergedRecordThatValidatesAgainstTheSchema() {
        MergedOrderBook book = new PriceMergeFunction()
                .map(AggregatedOrderBookDeserializer.fromGenericRecord(aggregatedRecord()));

        GenericRecord record = MergedOrderBookSerializer.toGenericRecord(book, MERGED);

        assertThat(GenericData.get().validate(MERGED, record)).isTrue();
        assertThat(record.get("pair_id")).isEqualTo(1);
        assertThat(record.get("side")).hasToString("asks");
        assertThat(record.get("source_id")).isEqualTo("agg-id");
        assertThat(record.get("event_time")).isEqualTo(1750680000000L);

        @SuppressWarnings("unchecked")
        List<GenericRecord> levels = (List<GenericRecord>) record.get("levels");
        assertThat(levels).hasSize(1);
        assertThat(levels.get(0).get("price")).isEqualTo("10");
        assertThat(levels.get(0).get("quantity")).isEqualTo("7");
        assertThat(levels.get(0).get("simulation")).isEqualTo(0);
        @SuppressWarnings("unchecked")
        List<Object> exchangeIds = (List<Object>) levels.get(0).get("exchange_ids");
        @SuppressWarnings("unchecked")
        List<Object> sourceIds = (List<Object>) levels.get(0).get("source_ids");
        assertThat(exchangeIds).containsExactly(1, 2);
        assertThat(sourceIds).containsExactly("A", "B");
    }

    /**
     * A real Avro binary round-trip. {@code GenericData.validate} above is a shape check and would
     * still pass for a record that blows up in the encoder — the two array fields are exactly where
     * that could happen, since they are the only thing in this schema the pipeline has not written
     * before.
     */
    @Test
    void mergedRecordSurvivesABinaryRoundTrip() throws Exception {
        MergedOrderBook book = new PriceMergeFunction()
                .map(AggregatedOrderBookDeserializer.fromGenericRecord(aggregatedRecord()));
        GenericRecord written = MergedOrderBookSerializer.toGenericRecord(book, MERGED);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(MERGED).write(written, encoder);
        encoder.flush();

        GenericRecord read = new GenericDatumReader<GenericRecord>(MERGED)
                .read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));

        assertThat(read.get("pair_id")).isEqualTo(1);
        assertThat(read.get("source_id")).hasToString("agg-id");
        @SuppressWarnings("unchecked")
        List<GenericRecord> levels = (List<GenericRecord>) read.get("levels");
        assertThat(levels).hasSize(1);
        assertThat(levels.get(0).get("price")).hasToString("10");
        assertThat(levels.get(0).get("quantity")).hasToString("7");
        assertThat((List<?>) levels.get(0).get("exchange_ids")).hasSize(2);
        assertThat(levels.get(0).get("source_ids").toString()).isEqualTo("[A, B]");
    }

    @Test
    void theEndToEndShapeIsOneSummedLevelPerPrice() {
        AggregatedOrderBook decoded = AggregatedOrderBookDeserializer.fromGenericRecord(aggregatedRecord());
        MergedOrderBook merged = new PriceMergeFunction().map(decoded);

        assertThat(decoded.getLevels()).hasSize(2);
        assertThat(merged.getLevels()).hasSize(1);
        assertThat(merged.getLevels().get(0).getQuantity()).isEqualTo("7");
    }
}
