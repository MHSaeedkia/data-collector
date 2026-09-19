package io.tibobit.normalizer.serde;

import io.tibobit.normalizer.avro.AvroSchemaLoader;
import io.tibobit.normalizer.model.ParsedBookEvent;
import io.tibobit.normalizer.model.PriceLevel;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the {@code parsed-book-event} mapping in both directions against the wire contract
 * (schemas/parsed_book_event.avsc) — the topic job 1 writes and job 2 reads. What is worth
 * pinning here beyond the shared field mapping: the market string goes out and comes back
 * verbatim, pair_id is absent from the wire entirely, and the round trip does not quietly
 * invent one.
 */
class ParsedBookEventSerdeTest {

    private static final Schema SCHEMA = AvroSchemaLoader.load("/avro/parsed_book_event.avsc");

    private static ParsedBookEvent parsed(String market, RawOrderBookEvent event) {
        return new ParsedBookEvent(market, event);
    }

    /**
     * Given a parsed delta event, When mapped, Then every field lands on its wire name, the type
     * enum is a real Avro EnumSymbol, and level decimal strings are preserved exactly.
     */
    @Test
    @DisplayName("maps a full parsed event onto the wire record")
    void mapsFullEvent() {
        RawOrderBookEvent event = new RawOrderBookEvent(6, 0, "update", 126776812L, 1L,
                1752473005123L,
                List.of(new PriceLevel("62775.5", "0.031418")),
                List.of(new PriceLevel("62774.9", "0"), new PriceLevel("62770.10", "1.5")));
        event.setExchangeEventTime(1752473005123L);

        GenericRecord record = ParsedBookEventSerializer.toGenericRecord(parsed("BTCUSDT", event), SCHEMA);

        assertThat(record.get("exchange_id")).isEqualTo(6);
        assertThat(record.get("market")).isEqualTo("BTCUSDT");
        assertThat(record.get("type")).isInstanceOf(GenericData.EnumSymbol.class)
                .hasToString("update");
        assertThat(record.get("sequence_id")).isEqualTo(126776812L);
        assertThat(record.get("sequence_jump")).isEqualTo(1L);
        assertThat(record.get("event_time")).isEqualTo(1752473005123L);
        assertThat(record.get("exchange_event_time")).isEqualTo(1752473005123L);
        List<?> asks = (List<?>) record.get("asks");
        assertThat(((GenericRecord) asks.get(0)).get("quantity")).isEqualTo("0.031418");
        List<?> bids = (List<?>) record.get("bids");
        assertThat(((GenericRecord) bids.get(0)).get("quantity")).isEqualTo("0"); // delete signal survives
        assertThat(((GenericRecord) bids.get(1)).get("price")).isEqualTo("62770.10"); // scale preserved
    }

    /**
     * The whole point of this schema: pair_id is not on it. If it ever creeps back, the split has
     * been undone somewhere and this fails before anything reaches a broker.
     */
    @Test
    @DisplayName("declares no pair_id on the wire")
    void declaresNoPairId() {
        assertThat(SCHEMA.getField("pair_id")).isNull();
        assertThat(SCHEMA.getField("market")).isNotNull();
    }

    /**
     * Given an ex4-style numeric market id, When round-tripped, Then it comes back as the exact
     * same string. Markets are the exchange's own symbols and several exchanges use bare digits;
     * anything that "helpfully" parsed them would break the exchange_markets lookup downstream.
     */
    @Test
    @DisplayName("round-trips the market string verbatim, digits included")
    void roundTripsMarketVerbatim() {
        RawOrderBookEvent event = new RawOrderBookEvent(4, 0, "snapshot", null, 0L,
                1752473005123L, List.of(new PriceLevel("62775.5", "1")), List.of());

        GenericRecord record = ParsedBookEventSerializer.toGenericRecord(parsed("14", event), SCHEMA);
        ParsedBookEvent back = ParsedBookEventDeserializer.fromGenericRecord(record);

        assertThat(record.get("market")).isEqualTo("14");
        assertThat(back.getMarket()).isEqualTo("14");
        assertThat(back.getEvent().getExchangeId()).isEqualTo(4);
        assertThat(back.getEvent().getPairId()).isZero();
    }

    /**
     * Given an ex3-style per-side snapshot (bids absent, no ordering field), When mapped, Then the
     * absent side and sequence_id stay null on the wire and survive the read back — null means
     * "not part of this event", which job 2 onwards must distinguish from an empty side.
     */
    @Test
    @DisplayName("keeps an absent side and absent sequence_id null through the round trip")
    void keepsAbsentSideNull() {
        RawOrderBookEvent event = new RawOrderBookEvent(3, 0, "snapshot", null, 0L,
                1752473005123L, List.of(new PriceLevel("62775.5", "1")), null);

        GenericRecord record = ParsedBookEventSerializer.toGenericRecord(parsed("BTC-IRT", event), SCHEMA);
        ParsedBookEvent back = ParsedBookEventDeserializer.fromGenericRecord(record);

        assertThat(record.get("sequence_id")).isNull();
        assertThat(record.get("bids")).isNull();
        assertThat(back.getEvent().getSequenceId()).isNull();
        assertThat(back.getEvent().getAsks()).isNotNull();
        assertThat(back.getEvent().getBids()).isNull();
        assertThat(back.getEvent().getExchangeEventTime()).isNull();
    }

    /**
     * Given an event stamped by job 1 only, When round-tripped, Then the parse pair survives and
     * every downstream stage is still null — the parsed stream is where parse_in/parse_out are
     * the ONLY timings set.
     */
    @Test
    @DisplayName("carries the parse timings and leaves every later stage null")
    void mapsParseTimings() {
        RawOrderBookEvent event = new RawOrderBookEvent(6, 0, "update", 1L, 1L, 1752473005123L,
                List.of(), List.of());
        event.getPipelineTimings().setParseIn(1752473005131L);
        event.getPipelineTimings().setParseOut(1752473005133L);

        GenericRecord record = ParsedBookEventSerializer.toGenericRecord(parsed("BTCUSDT", event), SCHEMA);
        ParsedBookEvent back = ParsedBookEventDeserializer.fromGenericRecord(record);

        GenericRecord timings = (GenericRecord) record.get("pipeline_timings");
        assertThat(timings).isNotNull();
        assertThat(timings.get("parse_in")).isEqualTo(1752473005131L);
        assertThat(timings.get("parse_out")).isEqualTo(1752473005133L);
        assertThat(timings.get("pair_extract_in")).isNull();
        assertThat(back.getEvent().getPipelineTimings().getParseIn()).isEqualTo(1752473005131L);
        assertThat(back.getEvent().getPipelineTimings().getParseOut()).isEqualTo(1752473005133L);
        assertThat(back.getEvent().getPipelineTimings().getPairExtractIn()).isNull();
    }

    /**
     * Given a record carrying NiFi's id as its source, When round-tripped, Then both lineage
     * fields come back as real Strings. Avro hands these back as Utf8, which prints identically
     * but compares unequal — an unconverted id would fail job 2's chain silently.
     */
    @Test
    @DisplayName("round-trips lineage ids as Strings, not Utf8")
    void roundTripsLineage() {
        RawOrderBookEvent event = new RawOrderBookEvent(1, 0, "snapshot", 42L, 0L,
                1752473005123L, List.of(), List.of());
        event.setId("22222222-2222-4222-8222-222222222222");
        event.setSourceIds(List.of("11111111-1111-4111-8111-111111111111"));
        event.setSimulation(1);

        GenericRecord record = ParsedBookEventSerializer.toGenericRecord(parsed("btc_usdt", event), SCHEMA);
        ParsedBookEvent back = ParsedBookEventDeserializer.fromGenericRecord(record);

        assertThat(back.getEvent().getId()).isInstanceOf(String.class)
                .isEqualTo("22222222-2222-4222-8222-222222222222");
        assertThat(back.getEvent().getSourceIds())
                .containsExactly("11111111-1111-4111-8111-111111111111");
        assertThat(back.getEvent().getSimulation()).isEqualTo(1);
    }

    /**
     * PriceLevel is shared with the snapshot schema, which is the only one declaring source_id.
     * A level still carrying that value must map onto THIS schema without it — a
     * GenericRecordBuilder throws on a field its schema does not have.
     */
    @Test
    @DisplayName("ignores a level's source_id, which this schema does not declare")
    void ignoresSourceIdNotInThisSchema() {
        RawOrderBookEvent event = new RawOrderBookEvent(1, 0, "snapshot", 42L, 0L,
                1752473005123L, List.of(new PriceLevel("62775.5", "1", "ev-a")), List.of());

        GenericRecord record = ParsedBookEventSerializer.toGenericRecord(parsed("btc_usdt", event), SCHEMA);

        GenericRecord level = (GenericRecord) ((List<?>) record.get("asks")).get(0);
        assertThat(level.getSchema().getField("source_id")).isNull();
        assertThat(level.get("price")).isEqualTo("62775.5");
    }
}
