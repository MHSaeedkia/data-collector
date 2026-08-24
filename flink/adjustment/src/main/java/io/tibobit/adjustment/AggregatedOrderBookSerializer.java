package io.tibobit.adjustment;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;

/**
 * Encodes an {@link AggregatedOrderBook} back to Confluent-wire-format Avro
 * bytes on the SAME subject it was read from,
 * {@code aggregated-order-book-event}.
 *
 * <p>
 * Same subject, deliberately: the adjusted topic carries the same record type
 * as {@code p{id}-{side}}, so it needs no schema of its own and no registry
 * work to deploy. A consumer resolves the schema by the id in the wire header,
 * so anything that already reads the aggregated topics reads the adjusted ones
 * unchanged. If a later step ever adds a field that only exists after
 * adjustment, that is the point at which this needs its own subject — not
 * before.
 *
 * <p>
 * The write schema is fetched from the Schema Registry at first use, never from
 * a local copy.
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

        List<AggregatedLevel> source = book.getLevels() == null ? List.of() : book.getLevels();
        List<GenericRecord> levels = new ArrayList<>(source.size());
        for (AggregatedLevel level : source) {
            levels.add(
                    new GenericRecordBuilder(levelSchema)
                            .set("exchange_id", level.getExchangeId())
                            .set("simulation", level.getSimulation())
                            .set("source_id", level.getSourceId())
                            .set("price", level.getPrice())
                            .set("quantity", level.getQuantity())
                            .build());
        }

        // `side` is an Avro ENUM, not a free string — a plain String here NPEs inside the
        // serializer at first emit, which is how the `reset` symbol broke job 2 live once.
        return new GenericRecordBuilder(schema)
                .set("pair_id", book.getPairId())
                .set("side", new GenericData.EnumSymbol(sideSchema, book.getSide()))
                .set("id", book.getId())
                .set("event_time", book.getEventTime())
                .set("levels", levels)
                .build();
    }
}
