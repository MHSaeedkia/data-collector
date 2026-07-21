package io.tibobit.consolidator.deserializer;

import io.tibobit.consolidator.avro.AvroSchemaLoader;
import io.tibobit.consolidator.model.PriceLevelEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link PriceLevelEventDeserializer#toPriceLevelEvent(GenericRecord)} against the wire
 * contract: an Avro record decoded from an input Kafka record (see
 * schemas/price_level_event.avsc) must map onto a fully populated event. This pins the field
 * mapping and the exact decimal strings for price/quantity — any drift here silently breaks
 * every downstream stage. The Confluent registry round-trip itself (bytes -> GenericRecord) is
 * Flink/Confluent library code, not asserted here — only our mapping from GenericRecord onward.
 */
class PriceLevelEventDeserializerTest {

    private static final Schema SCHEMA = AvroSchemaLoader.load("/avro/price_level_event.avsc");
    private static final Schema SIDE_SCHEMA = SCHEMA.getField("side").schema();

    private final PriceLevelEventDeserializer deserializer = new PriceLevelEventDeserializer("http://unused:8082");

    private static GenericRecord record(int exchangeId, int pairId, String side, long eventTime,
                                         String price, String quantity) {
        return new GenericRecordBuilder(SCHEMA)
                .set("exchange_id", exchangeId)
                .set("pair_id", pairId)
                .set("side", new GenericData.EnumSymbol(SIDE_SCHEMA, side))
                .set("event_time", eventTime)
                .set("price", price)
                .set("quantity", quantity)
                .build();
    }

    /**
     * Given a full price-level Avro record as produced upstream, When mapped, Then every wire
     * field maps onto its POJO field and price/quantity keep their exact decimal strings. The
     * happy-path contract the whole job depends on.
     */
    @Test
    @DisplayName("maps every wire field and preserves exact decimal strings")
    void mapsFullEvent() {
        GenericRecord record = record(3, 7, "asks", 1717171717000L, "97240.50", "1.5");

        PriceLevelEvent event = PriceLevelEventDeserializer.toPriceLevelEvent(record);

        assertThat(event.getExchangeId()).isEqualTo(3);
        assertThat(event.getPairId()).isEqualTo(7);
        assertThat(event.getSide()).isEqualTo("asks");
        assertThat(event.getEventTime()).isEqualTo(1717171717000L);
        assertThat(event.getPrice()).isEqualTo("97240.50");   // scale preserved, not normalized
        assertThat(event.getQuantity()).isEqualTo("1.5");
    }

    /**
     * Given a quantity 0 record (the R2 "remove this level" signal), When mapped, Then the "0"
     * is preserved as its exact string for the operator to test via BigDecimal.signum. Pins
     * that the delete signal survives the wire decode intact.
     */
    @Test
    @DisplayName("preserves a quantity 0 (the R2 remove signal)")
    void preservesZeroQuantity() {
        GenericRecord record = record(5, 7, "bids", 1717171718000L, "97000", "0");

        PriceLevelEvent event = PriceLevelEventDeserializer.toPriceLevelEvent(record);

        assertThat(event.getSide()).isEqualTo("bids");
        assertThat(event.getPrice()).isEqualTo("97000");
        assertThat(event.getQuantity()).isEqualTo("0");
    }

    /**
     * Given any element, When {@code isEndOfStream} is queried, Then it is always false — this is
     * an unbounded live stream that never signals completion. Pinned so the contract can't be
     * flipped accidentally (a true here would tear down the source mid-stream).
     */
    @Test
    @DisplayName("never reports end of stream")
    void isNeverEndOfStream() {
        assertThat(deserializer.isEndOfStream(new PriceLevelEvent())).isFalse();
    }

    /**
     * Given the deserializer, When its produced type is queried, Then it advertises
     * {@link PriceLevelEvent} so Flink builds the correct serializer for the source's output type.
     */
    @Test
    @DisplayName("advertises PriceLevelEvent as its produced type")
    void producesPriceLevelEventType() {
        assertThat(deserializer.getProducedType().getTypeClass()).isEqualTo(PriceLevelEvent.class);
    }
}
