package io.tibobit.adjustment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;

/**
 * Decodes Confluent-wire-format Avro bytes (schema
 * schemas/aggregated_order_book_event.avsc, subject
 * {@code aggregated-order-book-event}) into an {@link AggregatedOrderBook} —
 * job 6's output, this job's input. The reader schema is fetched from the
 * Schema Registry at first use — never from a local/bundled copy.
 */
public class AggregatedOrderBookDeserializer implements DeserializationSchema<AggregatedOrderBook> {

    static final String SUBJECT = "aggregated-order-book-event";

    private final String schemaRegistryUrl;

    // Not Serializable — initialize lazily after Flink ships this instance to the task.
    private transient DeserializationSchema<GenericRecord> avroDeserializer;

    public AggregatedOrderBookDeserializer(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public AggregatedOrderBook deserialize(byte[] message) throws IOException {
        if (avroDeserializer == null) {
            Schema schema = AvroSchemaLoader.loadLatest(schemaRegistryUrl, SUBJECT);
            avroDeserializer = ConfluentRegistryAvroDeserializationSchema.forGeneric(schema, schemaRegistryUrl);
        }
        return fromGenericRecord(avroDeserializer.deserialize(message));
    }

    static AggregatedOrderBook fromGenericRecord(GenericRecord record) {
        return new AggregatedOrderBook(
                (int) record.get("pair_id"),
                text(record.get("side")),
                text(record.get("id")),
                (long) record.get("event_time"),
                levels(record.get("levels")));
    }

    private static List<AggregatedLevel> levels(Object value) {
        if (!(value instanceof Collection<?> raw)) {
            return List.of();
        }
        List<AggregatedLevel> levels = new ArrayList<>(raw.size());
        for (Object element : raw) {
            GenericRecord level = (GenericRecord) element;
            levels.add(
                    new AggregatedLevel(
                            (int) level.get("exchange_id"),
                            (int) level.get("simulation"),
                            text(level.get("source_id")),
                            text(level.get("price")),
                            text(level.get("quantity"))));
        }
        return levels;
    }

    /**
     * Avro hands back {@link org.apache.avro.util.Utf8} for strings (and an
     * EnumSymbol for {@code side}), not {@link String}. A Utf8 kept as-is
     * compares unequal to every String while still printing correctly in a log
     * — so a price would never match its own map key and a lineage id would
     * fail every assertion, with nothing visibly wrong. Convert at the
     * boundary.
     */
    private static String text(Object value) {
        return value == null ? "" : value.toString();
    }

    @Override
    public boolean isEndOfStream(AggregatedOrderBook nextElement) {
        return false;
    }

    @Override
    public TypeInformation<AggregatedOrderBook> getProducedType() {
        return TypeInformation.of(AggregatedOrderBook.class);
    }
}
