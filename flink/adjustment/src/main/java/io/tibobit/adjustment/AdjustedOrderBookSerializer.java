package io.tibobit.adjustment;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * Encodes an {@link AdjustedOrderBook} to Confluent-wire-format Avro bytes, schema
 * schemas/adjusted_order_book_event.avsc, subject {@code adjusted-order-book-event}.
 *
 * <p><b>Its own subject, as of step 3.</b> The pass-through version reused
 * {@code aggregated-order-book-event}, which was right while the record was byte-identical to job
 * 6's. It is not any more: the adjusted event carries the three rates that were applied, so it is a
 * different shape and needs a schema of its own. That is a NEW subject, not an evolution of the
 * aggregated one — job 6's contract with {@code web/} is frozen and must not grow fields because a
 * downstream job wanted them.
 *
 * <p>The write schema is fetched from the Schema Registry at first use, never from a local copy —
 * so {@code scripts/warmup.sh} has to register the subject before this job can emit anything.
 */
public class AdjustedOrderBookSerializer implements SerializationSchema<AdjustedOrderBook> {

    static final String SUBJECT = "adjusted-order-book-event";

    private final String schemaRegistryUrl;

    // Not Serializable — initialize lazily after Flink ships this instance to the task.
    private transient SerializationSchema<GenericRecord> avroSerializer;
    private transient Schema schema;

    public AdjustedOrderBookSerializer(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public byte[] serialize(AdjustedOrderBook element) {
        if (avroSerializer == null) {
            schema = AvroSchemaLoader.loadLatest(schemaRegistryUrl, SUBJECT);
            avroSerializer = ConfluentRegistryAvroSerializationSchema.forGeneric(SUBJECT, schema, schemaRegistryUrl);
        }
        return avroSerializer.serialize(toGenericRecord(element, schema));
    }

    static GenericRecord toGenericRecord(AdjustedOrderBook book, Schema schema) {
        Schema sideSchema = schema.getField("side").schema();
        Schema levelSchema = schema.getField("levels").schema().getElementType();

        List<AdjustedLevel> source = book.getLevels() == null ? List.of() : book.getLevels();
        List<GenericRecord> levels = new ArrayList<>(source.size());
        for (AdjustedLevel level : source) {
            levels.add(new GenericRecordBuilder(levelSchema)
                    .set("exchange_id", level.getExchangeId())
                    .set("simulation", level.getSimulation())
                    .set("source_id", level.getSourceId())
                    .set("our_profit_percent", level.getOurProfitPercent())
                    .set("slippage_percent", level.getSlippagePercent())
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
                .set("buy_sell_commission_percent", book.getBuySellCommissionPercent())
                .set("levels", levels)
                .build();
    }
}
