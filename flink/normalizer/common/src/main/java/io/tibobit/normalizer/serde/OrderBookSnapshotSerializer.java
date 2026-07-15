package io.tibobit.normalizer.serde;

import io.tibobit.normalizer.avro.AvroSchemaLoader;
import io.tibobit.normalizer.model.OrderBookSnapshot;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;

/**
 * Encodes an {@link OrderBookSnapshot} to Confluent-wire-format Avro bytes (schema
 * schemas/order_book_snapshot.avsc, subject {@code order-book-snapshot}) — the value shape of
 * the job-5 output topics. The write schema is fetched from the Schema Registry at first use —
 * never from a local/bundled copy.
 */
public class OrderBookSnapshotSerializer implements SerializationSchema<OrderBookSnapshot> {

    static final String SUBJECT = "order-book-snapshot";

    private final String schemaRegistryUrl;

    // Not Serializable — initialize lazily after Flink ships this instance to the task.
    private transient SerializationSchema<GenericRecord> avroSerializer;
    private transient Schema schema;

    public OrderBookSnapshotSerializer(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public byte[] serialize(OrderBookSnapshot element) {
        if (avroSerializer == null) {
            schema = AvroSchemaLoader.loadLatest(schemaRegistryUrl, SUBJECT);
            avroSerializer = ConfluentRegistryAvroSerializationSchema.forGeneric(SUBJECT, schema, schemaRegistryUrl);
        }
        return avroSerializer.serialize(toGenericRecord(element, schema));
    }

    static GenericRecord toGenericRecord(OrderBookSnapshot snapshot, Schema schema) {
        Schema levelSchema = PriceLevels.elementType(schema.getField("asks").schema());

        return new GenericRecordBuilder(schema)
                .set("exchange_id", snapshot.getExchangeId())
                .set("pair_id", snapshot.getPairId())
                .set("event_time", snapshot.getEventTime())
                .set("last_sequence_id", snapshot.getLastSequenceId())
                .set("asks", PriceLevels.toRecords(snapshot.getAsks(), levelSchema))
                .set("bids", PriceLevels.toRecords(snapshot.getBids(), levelSchema))
                .build();
    }
}
