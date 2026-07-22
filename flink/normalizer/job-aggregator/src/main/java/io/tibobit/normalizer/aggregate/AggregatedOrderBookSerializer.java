package io.tibobit.normalizer.aggregate;

import io.tibobit.normalizer.avro.AvroSchemaLoader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * Encodes a {@link AggregatedOrderBook} to Confluent-wire-format Avro bytes (schema
 * schemas/aggregated_order_book_event.avsc, subject {@code aggregated-order-book-event}) — the
 * frozen web contract. The write schema is fetched from the Schema Registry at first use — never
 * from a local/bundled copy. Ported from the deprecated orderbook-consolidator; wire shape
 * unchanged, only the schema loader is the normalizer-common one.
 */
public class AggregatedOrderBookSerializer implements SerializationSchema<AggregatedOrderBook> {

    static final String SUBJECT = "aggregated-order-book-event";

    private final String schemaRegistryUrl;

    // Not Serializable — initialize lazily after Flink ships this instance to the task.
    private transient SerializationSchema<GenericRecord> avroSerializer;
    private transient Schema schema;

    public AggregatedOrderBookSerializer(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public byte[] serialize(AggregatedOrderBook element) {
        if (avroSerializer == null) {
            schema = AvroSchemaLoader.loadLatest(schemaRegistryUrl, SUBJECT);
            avroSerializer = ConfluentRegistryAvroSerializationSchema.forGeneric(SUBJECT, schema, schemaRegistryUrl);
        }
        return avroSerializer.serialize(toGenericRecord(element, schema));
    }

    static GenericRecord toGenericRecord(AggregatedOrderBook book, Schema schema) {
        Schema sideSchema = schema.getField("side").schema();
        Schema levelSchema = schema.getField("levels").schema().getElementType();

        List<GenericRecord> levels = new ArrayList<>();
        for (AggregatedLevel level : book.getLevels()) {
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
