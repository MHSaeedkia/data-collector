package io.tibobit.merger;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * Encodes a {@link MergedOrderBook} to Confluent-wire-format Avro bytes (schema
 * schemas/merged_order_book_event.avsc, subject {@code merged-order-book-event}). The write schema
 * is fetched from the Schema Registry at first use — never from a local/bundled copy.
 */
public class MergedOrderBookSerializer implements SerializationSchema<MergedOrderBook> {

    static final String SUBJECT = "merged-order-book-event";

    private final String schemaRegistryUrl;

    // Not Serializable — initialize lazily after Flink ships this instance to the task.
    private transient SerializationSchema<GenericRecord> avroSerializer;
    private transient Schema schema;

    public MergedOrderBookSerializer(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public byte[] serialize(MergedOrderBook element) {
        if (avroSerializer == null) {
            schema = AvroSchemaLoader.loadLatest(schemaRegistryUrl, SUBJECT);
            avroSerializer = ConfluentRegistryAvroSerializationSchema.forGeneric(SUBJECT, schema, schemaRegistryUrl);
        }
        return avroSerializer.serialize(toGenericRecord(element, schema));
    }

    static GenericRecord toGenericRecord(MergedOrderBook book, Schema schema) {
        Schema sideSchema = schema.getField("side").schema();
        Schema levelSchema = schema.getField("levels").schema().getElementType();

        List<GenericRecord> levels = new ArrayList<>();
        for (MergedLevel level : book.getLevels()) {
            levels.add(new GenericRecordBuilder(levelSchema)
                    .set("simulation", level.getSimulation())
                    .set("exchange_ids", level.getExchangeIds())
                    .set("source_ids", level.getSourceIds())
                    .set("price", level.getPrice())
                    .set("quantity", level.getQuantity())
                    .build());
        }

        return new GenericRecordBuilder(schema)
                .set("pair_id", book.getPairId())
                .set("side", new GenericData.EnumSymbol(sideSchema, book.getSide()))
                .set("id", book.getId())
                .set("source_id", book.getSourceId())
                .set("event_time", book.getEventTime())
                .set("levels", levels)
                .build();
    }
}
