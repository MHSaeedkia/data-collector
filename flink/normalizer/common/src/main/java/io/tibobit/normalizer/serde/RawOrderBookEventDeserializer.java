package io.tibobit.normalizer.serde;

import io.tibobit.normalizer.avro.AvroSchemaLoader;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;

import java.io.IOException;

/**
 * Decodes Confluent-wire-format Avro bytes (schema schemas/raw_order_book_event.avsc, subject
 * {@code raw-order-book-event}) into a {@link RawOrderBookEvent} — the value shape of every
 * job 2–5 input topic. The reader schema is fetched from the Schema Registry at first use —
 * never from a local/bundled copy.
 */
public class RawOrderBookEventDeserializer implements DeserializationSchema<RawOrderBookEvent> {

    private final String schemaRegistryUrl;

    // Not Serializable — initialize lazily after Flink ships this instance to the task.
    private transient DeserializationSchema<GenericRecord> avroDeserializer;

    public RawOrderBookEventDeserializer(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public RawOrderBookEvent deserialize(byte[] message) throws IOException {
        if (avroDeserializer == null) {
            Schema schema = AvroSchemaLoader.loadLatest(schemaRegistryUrl, RawOrderBookEventSerializer.SUBJECT);
            avroDeserializer = ConfluentRegistryAvroDeserializationSchema.forGeneric(schema, schemaRegistryUrl);
        }
        return fromGenericRecord(avroDeserializer.deserialize(message));
    }

    static RawOrderBookEvent fromGenericRecord(GenericRecord record) {
        RawOrderBookEvent event = new RawOrderBookEvent(
                (int) record.get("exchange_id"),
                (int) record.get("pair_id"),
                record.get("type").toString(),
                (Long) record.get("sequence_id"),
                (long) record.get("sequence_jump"),
                (long) record.get("event_time"),
                PriceLevels.fromRecords(record.get("asks")),
                PriceLevels.fromRecords(record.get("bids")));
        event.setPipelineTimings(PipelineTimingsRecords.fromRecord(record.get("pipeline_timings")));
        return event;
    }

    @Override
    public boolean isEndOfStream(RawOrderBookEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RawOrderBookEvent> getProducedType() {
        return TypeInformation.of(RawOrderBookEvent.class);
    }
}
