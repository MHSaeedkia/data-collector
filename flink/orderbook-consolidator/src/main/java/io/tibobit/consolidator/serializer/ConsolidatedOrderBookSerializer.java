package io.tibobit.consolidator.serializer;

import io.tibobit.consolidator.avro.AvroSchemaLoader;
import io.tibobit.consolidator.model.ConsolidatedLevel;
import io.tibobit.consolidator.model.ConsolidatedOrderBook;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * Encodes a {@link ConsolidatedOrderBook} to Confluent-wire-format Avro bytes (schema
 * schemas/consolidated_order_book_event.avsc, subject {@code consolidated-order-book-event})
 * for the output Kafka record. Used by the sink (see ConsolidatedOrderBookSinkFactory).
 * The write schema is fetched from the Schema Registry at first use — never from a
 * local/bundled copy — so every produced record is built against whatever is registered there.
 */
public class ConsolidatedOrderBookSerializer implements SerializationSchema<ConsolidatedOrderBook> {

    private static final String SUBJECT = "consolidated-order-book-event";

    private final String schemaRegistryUrl;

    // Not Serializable — initialize lazily after Flink ships this instance to the task.
    private transient SerializationSchema<GenericRecord> avroSerializer;
    private transient Schema schema;

    public ConsolidatedOrderBookSerializer(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public byte[] serialize(ConsolidatedOrderBook element) {
        if (avroSerializer == null) {
            schema = AvroSchemaLoader.loadLatest(schemaRegistryUrl, SUBJECT);
            avroSerializer = ConfluentRegistryAvroSerializationSchema.forGeneric(SUBJECT, schema, schemaRegistryUrl);
        }
        return avroSerializer.serialize(toGenericRecord(element, schema));
    }

    static GenericRecord toGenericRecord(ConsolidatedOrderBook book, Schema schema) {
        Schema sideSchema = schema.getField("side").schema();
        Schema levelSchema = schema.getField("levels").schema().getElementType();

        List<GenericRecord> levels = new ArrayList<>();
        for (ConsolidatedLevel level : book.getLevels()) {
            levels.add(new GenericRecordBuilder(levelSchema)
                    .set("exchange_id", level.getExchangeId())
                    .set("price", level.getPrice())
                    .set("quantity", level.getQuantity())
                    .build());
        }

        return new GenericRecordBuilder(schema)
                .set("pair_id", book.getPairId())
                .set("side", new GenericData.EnumSymbol(sideSchema, book.getSide()))
                .set("event_time", book.getEventTime())
                .set("levels", levels)
                .build();
    }
}
