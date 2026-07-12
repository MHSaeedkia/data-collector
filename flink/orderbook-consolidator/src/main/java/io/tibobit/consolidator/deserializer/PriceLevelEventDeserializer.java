package io.tibobit.consolidator.deserializer;

import io.tibobit.consolidator.avro.AvroSchemaLoader;
import io.tibobit.consolidator.model.PriceLevelEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;

import java.io.IOException;

/**
 * Decodes the Confluent-wire-format Avro bytes of an input Kafka record (schema
 * schemas/price_level_event.avsc, subject {@code price-level-event}) into a
 * {@link PriceLevelEvent}. Used by the price-level source (see PriceLevelSourceFactory).
 * The reader schema is fetched from the Schema Registry at first use — never from a
 * local/bundled copy — so decoding is always validated against whatever is registered there.
 */
public class PriceLevelEventDeserializer implements DeserializationSchema<PriceLevelEvent> {

    private static final String SCHEMA_SUBJECT = "price-level-event";

    private final String schemaRegistryUrl;

    // Not Serializable — initialize lazily after Flink ships this instance to the task.
    private transient DeserializationSchema<GenericRecord> avroDeserializer;

    public PriceLevelEventDeserializer(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public PriceLevelEvent deserialize(byte[] message) throws IOException {
        if (avroDeserializer == null) {
            Schema schema = AvroSchemaLoader.loadLatest(schemaRegistryUrl, SCHEMA_SUBJECT);
            avroDeserializer = ConfluentRegistryAvroDeserializationSchema.forGeneric(schema, schemaRegistryUrl);
        }
        return toPriceLevelEvent(avroDeserializer.deserialize(message));
    }

    static PriceLevelEvent toPriceLevelEvent(GenericRecord record) {
        return new PriceLevelEvent(
                (int) record.get("exchange_id"),
                (int) record.get("pair_id"),
                record.get("side").toString(),
                (long) record.get("event_time"),
                record.get("price").toString(),
                record.get("quantity").toString());
    }

    @Override
    public boolean isEndOfStream(PriceLevelEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<PriceLevelEvent> getProducedType() {
        return TypeInformation.of(PriceLevelEvent.class);
    }
}
