package io.tibobit.normalizer.serde;

import io.tibobit.normalizer.avro.AvroSchemaLoader;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;

/**
 * Encodes a {@link RawOrderBookEvent} to Confluent-wire-format Avro bytes (schema
 * schemas/raw_order_book_event.avsc, subject {@code raw-order-book-event}) — the value shape of
 * every job 1–4 output topic. The write schema is fetched from the Schema Registry at first
 * use — never from a local/bundled copy.
 */
public class RawOrderBookEventSerializer implements SerializationSchema<RawOrderBookEvent> {

    static final String SUBJECT = "raw-order-book-event";

    private final String schemaRegistryUrl;

    // Not Serializable — initialize lazily after Flink ships this instance to the task.
    private transient SerializationSchema<GenericRecord> avroSerializer;
    private transient Schema schema;

    public RawOrderBookEventSerializer(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public byte[] serialize(RawOrderBookEvent element) {
        if (avroSerializer == null) {
            schema = AvroSchemaLoader.loadLatest(schemaRegistryUrl, SUBJECT);
            avroSerializer = ConfluentRegistryAvroSerializationSchema.forGeneric(SUBJECT, schema, schemaRegistryUrl);
        }
        return avroSerializer.serialize(toGenericRecord(element, schema));
    }

    static GenericRecord toGenericRecord(RawOrderBookEvent event, Schema schema) {
        Schema typeSchema = schema.getField("type").schema();
        Schema levelSchema = PriceLevels.elementType(schema.getField("asks").schema());

        return new GenericRecordBuilder(schema)
                .set("exchange_id", event.getExchangeId())
                .set("pair_id", event.getPairId())
                .set("type", new GenericData.EnumSymbol(typeSchema, event.getType()))
                .set("sequence_id", event.getSequenceId())
                .set("sequence_jump", event.getSequenceJump())
                .set("event_time", event.getEventTime())
                .set("asks", PriceLevels.toRecords(event.getAsks(), levelSchema))
                .set("bids", PriceLevels.toRecords(event.getBids(), levelSchema))
                .build();
    }
}
