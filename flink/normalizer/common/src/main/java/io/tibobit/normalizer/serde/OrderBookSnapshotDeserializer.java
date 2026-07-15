package io.tibobit.normalizer.serde;

import io.tibobit.normalizer.avro.AvroSchemaLoader;
import io.tibobit.normalizer.model.OrderBookSnapshot;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;

import java.io.IOException;

/**
 * Decodes Confluent-wire-format Avro bytes (schema schemas/order_book_snapshot.avsc, subject
 * {@code order-book-snapshot}) into an {@link OrderBookSnapshot} — the value shape of the
 * job-6 input topics. The reader schema is fetched from the Schema Registry at first use —
 * never from a local/bundled copy.
 */
public class OrderBookSnapshotDeserializer implements DeserializationSchema<OrderBookSnapshot> {

    private final String schemaRegistryUrl;

    // Not Serializable — initialize lazily after Flink ships this instance to the task.
    private transient DeserializationSchema<GenericRecord> avroDeserializer;

    public OrderBookSnapshotDeserializer(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public OrderBookSnapshot deserialize(byte[] message) throws IOException {
        if (avroDeserializer == null) {
            Schema schema = AvroSchemaLoader.loadLatest(schemaRegistryUrl, OrderBookSnapshotSerializer.SUBJECT);
            avroDeserializer = ConfluentRegistryAvroDeserializationSchema.forGeneric(schema, schemaRegistryUrl);
        }
        return fromGenericRecord(avroDeserializer.deserialize(message));
    }

    static OrderBookSnapshot fromGenericRecord(GenericRecord record) {
        OrderBookSnapshot snapshot = new OrderBookSnapshot(
                (int) record.get("exchange_id"),
                (int) record.get("pair_id"),
                (long) record.get("event_time"),
                (Long) record.get("last_sequence_id"),
                PriceLevels.fromRecords(record.get("asks")),
                PriceLevels.fromRecords(record.get("bids")));
        snapshot.setPipelineTimings(PipelineTimingsRecords.fromRecord(record.get("pipeline_timings")));
        return snapshot;
    }

    @Override
    public boolean isEndOfStream(OrderBookSnapshot nextElement) {
        return false;
    }

    @Override
    public TypeInformation<OrderBookSnapshot> getProducedType() {
        return TypeInformation.of(OrderBookSnapshot.class);
    }
}
