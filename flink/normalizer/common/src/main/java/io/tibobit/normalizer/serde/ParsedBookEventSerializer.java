package io.tibobit.normalizer.serde;

import io.tibobit.normalizer.avro.AvroSchemaLoader;
import io.tibobit.normalizer.model.ParsedBookEvent;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;

/**
 * Encodes a {@link ParsedBookEvent} to Confluent-wire-format Avro bytes (schema
 * schemas/parsed_book_event.avsc, subject {@code parsed-book-event}) — the value shape of the
 * {@code ex{id}-parsed-flink} topics job 1 writes. The write schema is fetched from the Schema
 * Registry at first use — never from a local/bundled copy.
 *
 * <p>Field-for-field this is {@link RawOrderBookEventSerializer} with {@code market} where
 * {@code pair_id} goes; the two cannot share a mapper because a GenericRecordBuilder throws on a
 * field its schema does not have.
 */
public class ParsedBookEventSerializer implements SerializationSchema<ParsedBookEvent> {

    static final String SUBJECT = "parsed-book-event";

    private final String schemaRegistryUrl;

    // Not Serializable — initialize lazily after Flink ships this instance to the task.
    private transient SerializationSchema<GenericRecord> avroSerializer;
    private transient Schema schema;

    public ParsedBookEventSerializer(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public byte[] serialize(ParsedBookEvent element) {
        if (avroSerializer == null) {
            schema = AvroSchemaLoader.loadLatest(schemaRegistryUrl, SUBJECT);
            avroSerializer = ConfluentRegistryAvroSerializationSchema.forGeneric(SUBJECT, schema, schemaRegistryUrl);
        }
        return avroSerializer.serialize(toGenericRecord(element, schema));
    }

    static GenericRecord toGenericRecord(ParsedBookEvent parsed, Schema schema) {
        RawOrderBookEvent event = parsed.getEvent();
        Schema typeSchema = schema.getField("type").schema();
        Schema levelSchema = PriceLevels.elementType(schema.getField("asks").schema());

        return new GenericRecordBuilder(schema)
                .set("exchange_id", event.getExchangeId())
                .set("market", parsed.getMarket())
                .set("type", new GenericData.EnumSymbol(typeSchema, event.getType()))
                .set("simulation", event.getSimulation())
                .set("id", event.getId())
                .set("source_ids", event.getSourceIds())
                .set("sequence_id", event.getSequenceId())
                .set("sequence_jump", event.getSequenceJump())
                .set("event_time", event.getEventTime())
                .set("exchange_event_time", event.getExchangeEventTime())
                .set("asks", PriceLevels.toRecords(event.getAsks(), levelSchema))
                .set("bids", PriceLevels.toRecords(event.getBids(), levelSchema))
                .set("pipeline_timings", PipelineTimingsRecords.toRecord(
                        event.getPipelineTimings(), schema.getField("pipeline_timings").schema()))
                .build();
    }
}
