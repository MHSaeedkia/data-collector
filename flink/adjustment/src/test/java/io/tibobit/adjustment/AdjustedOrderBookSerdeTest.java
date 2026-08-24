package io.tibobit.adjustment;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Both wire boundaries, against the CANONICAL schemas in schemas/ (copied onto the test classpath
 * by the pom), without a live Schema Registry: the aggregated record coming IN, and the adjusted
 * record — a different schema and a different subject since step 3 — going OUT.
 *
 * <p>Fixture values are deliberately non-default, because a field the model forgot would come back
 * as its schema default and a fixture built from defaults would agree with the bug.
 */
class AdjustedOrderBookSerdeTest {

    private static final Schema AGGREGATED = AvroSchemaLoader.load("/avro/aggregated_order_book_event.avsc");
    private static final Schema ADJUSTED = AvroSchemaLoader.load("/avro/adjusted_order_book_event.avsc");

    /** Strings are deliberately Utf8, exactly as Avro hands them back off the wire. */
    private static GenericRecord aggregatedRecord() {
        Schema levelSchema = AGGREGATED.getField("levels").schema().getElementType();
        GenericRecord level = new GenericRecordBuilder(levelSchema)
                .set("exchange_id", 6)
                .set("simulation", 1)
                .set("source_id", new Utf8("snapshot-A"))
                .set("price", new Utf8("62650.00"))
                .set("quantity", new Utf8("0.50000000"))
                .build();

        return new GenericRecordBuilder(AGGREGATED)
                .set("pair_id", 3)
                .set("side", new GenericData.EnumSymbol(AGGREGATED.getField("side").schema(), "asks"))
                .set("id", new Utf8("agg-id-1"))
                .set("event_time", 1750680000000L)
                .set("levels", List.of(level))
                .build();
    }

    private static AdjustedOrderBook throughTheChain() {
        AdjustedOrderBook book = AdjustedOrderBook.from(
                AggregatedOrderBookDeserializer.fromGenericRecord(aggregatedRecord()));
        return new SlippageFunction().map(new OurProfitFunction().map(new BuySellCommissionFunction().map(book)));
    }

    @Test
    void decodesTheAggregatedInputConvertingUtf8ToString() {
        AggregatedOrderBook book = AggregatedOrderBookDeserializer.fromGenericRecord(aggregatedRecord());

        assertThat(book.getPairId()).isEqualTo(3);
        assertThat(book.getSide()).isEqualTo("asks");
        assertThat(book.getId()).isEqualTo("agg-id-1");
        AggregatedLevel level = book.getLevels().get(0);
        // Utf8 prints identically but compares unequal to every String — assert the real type.
        assertThat(level.getSourceId()).isInstanceOf(String.class).isEqualTo("snapshot-A");
        assertThat(level.getPrice()).isInstanceOf(String.class).isEqualTo("62650.00");
    }

    @Test
    void theAdjustedRecordCarriesTheRatesAndTheAdjustedPrice() {
        GenericRecord out = AdjustedOrderBookSerializer.toGenericRecord(throughTheChain(), ADJUSTED);

        assertThat(GenericData.get().validate(ADJUSTED, out)).isTrue();
        assertThat(out.get("pair_id")).isEqualTo(3);
        assertThat(out.get("side")).hasToString("asks");
        assertThat(out.get("id")).hasToString("agg-id-1");
        assertThat(out.get("event_time")).isEqualTo(1750680000000L);

        // The whole point of step 3: the event says what was charged, not just the result.
        assertThat(out.get("buy_sell_commission_percent")).hasToString("0.35");
        assertThat(out.get("our_profit_percent")).hasToString("0.1");
        assertThat(out.get("slippage_percent")).hasToString("1");

        @SuppressWarnings("unchecked")
        List<GenericRecord> levels = (List<GenericRecord>) out.get("levels");
        assertThat(levels).hasSize(1);
        assertThat(levels.get(0).get("price")).hasToString("63561.46571775");
        assertThat(levels.get(0).get("quantity")).hasToString("0.50000000");
        assertThat(levels.get(0).get("exchange_id")).isEqualTo(6);
        assertThat(levels.get(0).get("simulation")).isEqualTo(1);
        assertThat(levels.get(0).get("source_id")).hasToString("snapshot-A");
    }

    /** {@code side} is an Avro ENUM; a plain String there NPEs in the encoder at first emit. */
    @Test
    void sideIsWrittenAsAnEnumSymbolNotAString() {
        GenericRecord out = AdjustedOrderBookSerializer.toGenericRecord(throughTheChain(), ADJUSTED);
        assertThat(out.get("side")).isInstanceOf(GenericData.EnumSymbol.class);
    }

    /**
     * A real Avro binary round-trip. {@code GenericData.validate} is a shape check and would still
     * pass for a record the encoder rejects, so the bytes are actually written and read back.
     */
    @Test
    void theAdjustedRecordSurvivesABinaryRoundTrip() throws Exception {
        GenericRecord written = AdjustedOrderBookSerializer.toGenericRecord(throughTheChain(), ADJUSTED);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(ADJUSTED).write(written, encoder);
        encoder.flush();

        GenericRecord read = new GenericDatumReader<GenericRecord>(ADJUSTED)
                .read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));

        assertThat(read.get("pair_id")).isEqualTo(3);
        assertThat(read.get("buy_sell_commission_percent")).hasToString("0.35");
        assertThat(read.get("slippage_percent")).hasToString("1");
        @SuppressWarnings("unchecked")
        List<GenericRecord> levels = (List<GenericRecord>) read.get("levels");
        assertThat(levels.get(0).get("price")).hasToString("63561.46571775");
        assertThat(levels.get(0).get("source_id")).hasToString("snapshot-A");
    }

    /**
     * The adjusted schema must stay a superset of what this job writes. If someone adds a field to
     * the avsc and not to the model, the record still validates (Avro fills the default) — so this
     * checks the field LIST explicitly, which is the only thing that catches it.
     */
    @Test
    void theModelCoversEveryFieldOfTheSchema() {
        assertThat(ADJUSTED.getFields().stream().map(Schema.Field::name))
                .containsExactly("pair_id", "side", "id", "event_time",
                        "buy_sell_commission_percent", "our_profit_percent", "slippage_percent", "levels");
        assertThat(ADJUSTED.getField("levels").schema().getElementType()
                .getFields().stream().map(Schema.Field::name))
                .containsExactly("exchange_id", "simulation", "source_id", "price", "quantity");
    }
}
