package io.tibobit.normalizer.serde;

import io.tibobit.normalizer.avro.AvroSchemaLoader;
import io.tibobit.normalizer.model.ControlCommand;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;

import java.util.ArrayList;

/**
 * Encodes a ControlCommand to Confluent-wire-format Avro bytes (schema
 * schemas/control_command.avsc, subject control-command). The write schema is fetched from the
 * Schema Registry at first use — never from a local/bundled copy. Same pattern as the data-plane
 * serializers (see AggregatedOrderBookSerializer): not Serializable, so the Avro serializer and
 * schema are built lazily on first use, after Flink ships this instance to the task.
 */
public class ControlCommandSerializer implements SerializationSchema<ControlCommand> {

    static final String SUBJECT = "control-command";

    private final String schemaRegistryUrl;

    private transient SerializationSchema<GenericRecord> avroSerializer;
    private transient Schema schema;

    public ControlCommandSerializer(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public byte[] serialize(ControlCommand element) {
        if (avroSerializer == null) {
            schema = AvroSchemaLoader.loadLatest(schemaRegistryUrl, SUBJECT);
            avroSerializer = ConfluentRegistryAvroSerializationSchema.forGeneric(SUBJECT, schema, schemaRegistryUrl);
        }
        return avroSerializer.serialize(toGenericRecord(element, schema));
    }

    static GenericRecord toGenericRecord(ControlCommand command, Schema schema) {
        return new GenericRecordBuilder(schema)
                .set("action", command.getAction())
                .set("reason", command.getReason())
                .set("exchange_id", command.getExchangeId())
                .set("pair_id", command.getPairId())
                .set("simulation", command.getSimulation())
                .set("id", command.getId())
                .set("source_ids", new ArrayList<>(command.getSourceIds()))
                .build();
    }
}
