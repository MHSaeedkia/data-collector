package io.tibobit.normalizer.aggregate;

import io.tibobit.normalizer.avro.AvroSchemaLoader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link AggregatedOrderBookSerializer#toGenericRecord} against the canonical
 * schemas/aggregated_order_book_event.avsc — the frozen web contract. Every record is pushed through
 * a real Avro binary encode/decode, because {@code GenericData.validate} alone would not catch a
 * field landing in the wrong position.
 */
class AggregatedOrderBookSerializerTest {

    private static final Schema SCHEMA = AvroSchemaLoader.load("/avro/aggregated_order_book_event.avsc");

    private static GenericRecord roundTrip(GenericRecord record) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bytes, null);
        new GenericDatumWriter<GenericRecord>(SCHEMA).write(record, encoder);
        encoder.flush();
        return new GenericDatumReader<GenericRecord>(SCHEMA)
                .read(null, DecoderFactory.get().binaryDecoder(bytes.toByteArray(), null));
    }

    /**
     * Given a book mixing two exchanges and both simulation flags, When mapped and round-tripped,
     * Then every record and level field lands on its wire name with its value, and level order is
     * kept exactly as the aggregator sorted it.
     */
    @Test
    @DisplayName("maps every field and keeps level order through a binary round trip")
    void mapsEveryFieldInOrder() throws IOException {
        AggregatedOrderBook book = new AggregatedOrderBook(7, "bids", List.of(
                new AggregatedLevel(5, 0, "snap-5", "77322.6", "0.702754"),
                new AggregatedLevel(8, 1, "snap-8", "77322.6", "0.1"),
                new AggregatedLevel(6, 0, "snap-6", "77319.4", "3")), 1789310527051L, 1789310520000L);
        book.setId("agg-1");

        GenericRecord record = AggregatedOrderBookSerializer.toGenericRecord(book, SCHEMA);
        assertThat(GenericData.get().validate(SCHEMA, record)).isTrue();
        GenericRecord decoded = roundTrip(record);

        assertThat(decoded.get("pair_id")).isEqualTo(7);
        assertThat(decoded.get("side")).hasToString("bids");
        assertThat(decoded.get("id")).hasToString("agg-1");
        assertThat(decoded.get("max_event_time")).isEqualTo(1789310527051L);
        assertThat(decoded.get("min_event_time")).isEqualTo(1789310520000L);
        List<?> levels = (List<?>) decoded.get("levels");
        assertThat(levels).hasSize(3);
        assertThat(levels).extracting(l -> ((GenericRecord) l).get("exchange_id")).containsExactly(5, 8, 6);
        assertThat(levels).extracting(l -> ((GenericRecord) l).get("simulation")).containsExactly(0, 1, 0);
        assertThat(levels).extracting(l -> ((GenericRecord) l).get("source_id").toString())
                .containsExactly("snap-5", "snap-8", "snap-6");
        assertThat(levels).extracting(l -> ((GenericRecord) l).get("price").toString())
                .containsExactly("77322.6", "77322.6", "77319.4");
        assertThat(levels).extracting(l -> ((GenericRecord) l).get("quantity").toString())
                .containsExactly("0.702754", "0.1", "3");
    }

    /**
     * Given every exchange reset, When the empty book is mapped, Then it encodes as an empty level
     * array — the record the web reads as "nothing to show", not a failure.
     */
    @Test
    @DisplayName("an empty book encodes as an empty level array")
    void emptyBook() throws IOException {
        AggregatedOrderBook book = new AggregatedOrderBook(7, "asks", List.of(), 1L, null);
        book.setId("agg-2");

        GenericRecord decoded = roundTrip(AggregatedOrderBookSerializer.toGenericRecord(book, SCHEMA));

        assertThat((List<?>) decoded.get("levels")).isEmpty();
        assertThat(decoded.get("side")).hasToString("asks");
        // Nothing contributed a level, so there is no oldest contributing time to name.
        assertThat(decoded.get("min_event_time")).isNull();
    }

    /**
     * Given a level with no price, When encoded, Then it fails loudly rather than writing a record
     * the web cannot decode. price is a required string on the frozen contract.
     */
    @Test
    @DisplayName("a missing required level field fails instead of encoding")
    void missingRequiredFieldFails() {
        AggregatedOrderBook book = new AggregatedOrderBook(7, "asks",
                List.of(new AggregatedLevel(5, 0, "snap-5", null, "1")), 1L, 1L);
        book.setId("agg-3");

        assertThatThrownBy(() -> roundTrip(AggregatedOrderBookSerializer.toGenericRecord(book, SCHEMA)))
                .isInstanceOf(RuntimeException.class);
    }
}
