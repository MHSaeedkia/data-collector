package io.tibobit.normalizer.serde;

import io.tibobit.normalizer.avro.AvroSchemaLoader;
import io.tibobit.normalizer.model.RejectedOrderBookEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;

/**
 * Encodes a {@link RejectedOrderBookEvent} to Confluent-wire-format Avro bytes (schema
 * schemas/rejected_order_book_event.avsc, subject {@code rejected-order-book-event}) — the
 * value shape of the job-2 dead-letter topics. Serializer only: nothing in the pipeline
 * consumes dead-letter topics (they are audit points read via kafka-ui). The nested event
 * record is mapped by {@link RawOrderBookEventSerializer#toGenericRecord} — the inline
 * RawOrderBookEvent definition in this schema is field-for-field identical to
 * raw_order_book_event.avsc (see memory/project_avro_schema.md).
 */
public class RejectedOrderBookEventSerializer implements SerializationSchema<RejectedOrderBookEvent> {

    static final String SUBJECT = "rejected-order-book-event";

    private final String schemaRegistryUrl;

    // Not Serializable — initialize lazily after Flink ships this instance to the task.
    private transient SerializationSchema<GenericRecord> avroSerializer;
    private transient Schema schema;

    public RejectedOrderBookEventSerializer(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public byte[] serialize(RejectedOrderBookEvent element) {
        if (avroSerializer == null) {
            schema = AvroSchemaLoader.loadLatest(schemaRegistryUrl, SUBJECT);
            avroSerializer = ConfluentRegistryAvroSerializationSchema.forGeneric(SUBJECT, schema, schemaRegistryUrl);
        }
        return avroSerializer.serialize(toGenericRecord(element, schema));
    }

    static GenericRecord toGenericRecord(RejectedOrderBookEvent rejection, Schema schema) {
        Schema eventSchema = schema.getField("event").schema();

        return new GenericRecordBuilder(schema)
                .set("event", RawOrderBookEventSerializer.toGenericRecord(rejection.getEvent(), eventSchema))
                .set("reject_reason", rejection.getRejectReason())
                .set("rejected_at", rejection.getRejectedAt())
                .build();
    }
}
