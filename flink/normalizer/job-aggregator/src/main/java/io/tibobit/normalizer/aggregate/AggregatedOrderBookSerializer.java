package io.tibobit.normalizer.aggregate;

import io.tibobit.normalizer.avro.AvroSchemaLoader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * Encodes a {@link AggregatedOrderBook} to Confluent-wire-format Avro bytes (schema
 * schemas/aggregated_order_book_event.avsc, subject {@code aggregated-order-book-event}) — the
 * frozen web contract. The write schema is fetched from the Schema Registry at first use — never
 * from a local/bundled copy. Uses the normalizer-common schema loader.
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

    /**
     * Plain {@link GenericData.Record}s with positional puts, not {@code GenericRecordBuilder}: the
     * builder validates each field and builds a second record for every level, which was about half
     * of this serializer's time on a ~750-level book. Every field is set explicitly, so the builder's
     * defaults were never used. A null in a required field still fails, at encode time instead.
     */
    static GenericRecord toGenericRecord(AggregatedOrderBook book, Schema schema) {
        Schema sideSchema = schema.getField("side").schema();
        Schema levelSchema = schema.getField("levels").schema().getElementType();
        int exchangeIdPos = levelSchema.getField("exchange_id").pos();
        int simulationPos = levelSchema.getField("simulation").pos();
        int sourceIdPos = levelSchema.getField("source_id").pos();
        int pricePos = levelSchema.getField("price").pos();
        int quantityPos = levelSchema.getField("quantity").pos();

        List<GenericRecord> levels = new ArrayList<>(book.getLevels().size());
        for (AggregatedLevel level : book.getLevels()) {
            GenericData.Record record = new GenericData.Record(levelSchema);
            record.put(exchangeIdPos, level.getExchangeId());
            record.put(simulationPos, level.getSimulation());
            record.put(sourceIdPos, level.getSourceId());
            record.put(pricePos, level.getPrice());
            record.put(quantityPos, level.getQuantity());
            levels.add(record);
        }

        GenericData.Record record = new GenericData.Record(schema);
        record.put("pair_id", book.getPairId());
        record.put("side", new GenericData.EnumSymbol(sideSchema, book.getSide()));
        record.put("id", book.getId());
        record.put("event_time", book.getEventTime());
        record.put("levels", levels);
        return record;
    }
}
