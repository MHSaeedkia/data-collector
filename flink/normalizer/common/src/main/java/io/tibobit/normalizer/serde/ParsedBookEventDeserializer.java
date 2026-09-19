package io.tibobit.normalizer.serde;

import io.tibobit.normalizer.avro.AvroSchemaLoader;
import io.tibobit.normalizer.model.ParsedBookEvent;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;

import java.io.IOException;

/**
 * Decodes Confluent-wire-format Avro bytes (schema schemas/parsed_book_event.avsc, subject
 * {@code parsed-book-event}) into a {@link ParsedBookEvent} — the value shape of job 2's
 * {@code ex{id}-parsed-flink} input topics. The reader schema is fetched from the Schema Registry
 * at first use — never from a local/bundled copy.
 */
public class ParsedBookEventDeserializer implements DeserializationSchema<ParsedBookEvent> {

    private final String schemaRegistryUrl;

    // Not Serializable — initialize lazily after Flink ships this instance to the task.
    private transient DeserializationSchema<GenericRecord> avroDeserializer;

    public ParsedBookEventDeserializer(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public ParsedBookEvent deserialize(byte[] message) throws IOException {
        if (avroDeserializer == null) {
            Schema schema = AvroSchemaLoader.loadLatest(schemaRegistryUrl, ParsedBookEventSerializer.SUBJECT);
            avroDeserializer = ConfluentRegistryAvroDeserializationSchema.forGeneric(schema, schemaRegistryUrl);
        }
        return fromGenericRecord(avroDeserializer.deserialize(message));
    }

    static ParsedBookEvent fromGenericRecord(GenericRecord record) {
        // pair_id stays 0: resolving it is the entire job of the operator reading this.
        RawOrderBookEvent event = new RawOrderBookEvent(
                (int) record.get("exchange_id"),
                0,
                record.get("type").toString(),
                (Long) record.get("sequence_id"),
                (long) record.get("sequence_jump"),
                (long) record.get("event_time"),
                PriceLevels.fromRecords(record.get("asks")),
                PriceLevels.fromRecords(record.get("bids")));
        event.setExchangeEventTime((Long) record.get("exchange_event_time"));
        event.setSimulation((int) record.get("simulation"));
        event.setId(LineageRecords.id(record.get("id")));
        event.setSourceIds(LineageRecords.sourceIds(record.get("source_ids")));
        event.setPipelineTimings(PipelineTimingsRecords.fromRecord(record.get("pipeline_timings")));
        return new ParsedBookEvent(record.get("market").toString(), event);
    }

    @Override
    public boolean isEndOfStream(ParsedBookEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<ParsedBookEvent> getProducedType() {
        return TypeInformation.of(ParsedBookEvent.class);
    }
}
