package io.tibobit.normalizer.serde;

import io.tibobit.normalizer.avro.AvroSchemaLoader;
import io.tibobit.normalizer.model.PriceLevelEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;

/**
 * Encodes a {@link PriceLevelEvent} to Confluent-wire-format Avro bytes (schema
 * schemas/price_level_event.avsc, subject {@code price-level-event}) — the job-6 output, i.e. the
 * pipeline's final product.
 *
 * <p>This subject is FROZEN: the consolidator already consumes it and defines the contract, so
 * bytes written here must be indistinguishable from what NiFi's normalized output writes today.
 * {@code side} is an Avro enum, not a string, hence the {@link GenericData.EnumSymbol}.
 */
public class PriceLevelEventSerializer implements SerializationSchema<PriceLevelEvent> {

    static final String SUBJECT = "price-level-event";

    private final String schemaRegistryUrl;

    // Not Serializable — initialize lazily after Flink ships this instance to the task.
    private transient SerializationSchema<GenericRecord> avroSerializer;
    private transient Schema schema;

    public PriceLevelEventSerializer(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public byte[] serialize(PriceLevelEvent element) {
        if (avroSerializer == null) {
            schema = AvroSchemaLoader.loadLatest(schemaRegistryUrl, SUBJECT);
            avroSerializer = ConfluentRegistryAvroSerializationSchema.forGeneric(SUBJECT, schema, schemaRegistryUrl);
        }
        return avroSerializer.serialize(toGenericRecord(element, schema));
    }

    static GenericRecord toGenericRecord(PriceLevelEvent event, Schema schema) {
        Schema sideSchema = schema.getField("side").schema();

        return new GenericRecordBuilder(schema)
                .set("exchange_id", event.getExchangeId())
                .set("pair_id", event.getPairId())
                .set("side", new GenericData.EnumSymbol(sideSchema, event.getSide()))
                .set("event_time", event.getEventTime())
                .set("price", event.getPrice())
                .set("quantity", event.getQuantity())
                .set("pipeline_timings", PipelineTimingsRecords.toRecord(
                        event.getPipelineTimings(), schema.getField("pipeline_timings").schema()))
                .build();
    }
}
