package io.tibobit.normalizer.pairextract;

import io.tibobit.normalizer.lookup.RefreshingLookup;
import io.tibobit.normalizer.model.ParsedBookEvent;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link PairExtractFunction} drop/emit rules through a real operator harness (in-memory
 * lookup — the JDBC loader is exercised by the live smoke, not here). The parse-side rules moved
 * to {@code ParseFunctionTest} in job-parser on 2026-09-19.
 */
class PairExtractFunctionTest {

    /** The id job 1 minted onto the parsed record — this job's parent. */
    private static final String PARSER_ID = "22222222-2222-4222-8222-222222222222";

    private static ParsedBookEvent parsed(int exchangeId, String market) {
        RawOrderBookEvent event =
                new RawOrderBookEvent(exchangeId, 0, "snapshot", 7L, 0L, 123L, List.of(), List.of());
        event.setId(PARSER_ID);
        event.setSourceIds(List.of("11111111-1111-4111-8111-111111111111"));
        event.getPipelineTimings().setParseIn(100L);
        event.getPipelineTimings().setParseOut(102L);
        return new ParsedBookEvent(market, event);
    }

    private static OneInputStreamOperatorTestHarness<ParsedBookEvent, RawOrderBookEvent> harness(
            Map<String, Integer> marketRows) throws Exception {
        PairExtractFunction fn =
                new PairExtractFunction(new RefreshingLookup<>(() -> marketRows, 60_000L));
        OneInputStreamOperatorTestHarness<ParsedBookEvent, RawOrderBookEvent> harness =
                new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(fn));
        harness.open();
        return harness;
    }

    /**
     * Given a known market, When a parsed event flows through, Then it is emitted with pair_id
     * from exchange_markets and the exchange_id it arrived with.
     */
    @Test
    @DisplayName("emits the event with pair_id resolved")
    void emitsResolvedEvent() throws Exception {
        try (var harness = harness(Map.of("1|BTCUSDT", 42))) {
            harness.processElement(new StreamRecord<>(parsed(1, "BTCUSDT")));

            List<RawOrderBookEvent> out = harness.extractOutputValues();
            assertThat(out).hasSize(1);
            assertThat(out.get(0).getExchangeId()).isEqualTo(1);
            assertThat(out.get(0).getPairId()).isEqualTo(42);
            assertThat(out.get(0).getType()).isEqualTo("snapshot");
            assertThat(out.get(0).getSequenceId()).isEqualTo(7L);
        }
    }

    /**
     * Given a parsed record carrying job 1's id, When the event is emitted, Then that id is the
     * event's single source and the event carries a fresh id of its own — this job WRITES to the
     * raw topic, so it mints, and the parsed record it read is its only parent.
     */
    @Test
    @DisplayName("names the parsed record as its source and mints its own id")
    void stampsLineage() throws Exception {
        try (var harness = harness(Map.of("1|BTCUSDT", 42))) {
            harness.processElement(new StreamRecord<>(parsed(1, "BTCUSDT")));

            RawOrderBookEvent event = harness.extractOutputValues().get(0);
            assertThat(event.getSourceIds()).containsExactly(PARSER_ID);
            assertThat(event.getId()).isNotBlank().isNotEqualTo(PARSER_ID);
            assertThat(UUID.fromString(event.getId())).isNotNull();
        }
    }

    /**
     * Given a parsed event flowing through, When it is emitted, Then this job stamps its
     * pair-extract in/out timings (in ≤ out, both within the processing window), leaves job 1's
     * parse timings untouched, and leaves the downstream stages null.
     */
    @Test
    @DisplayName("stamps pair-extract in/out timings without disturbing the parse pair")
    void stampsPairExtractTimings() throws Exception {
        long before = System.currentTimeMillis();
        try (var harness = harness(Map.of("1|BTCUSDT", 42))) {
            harness.processElement(new StreamRecord<>(parsed(1, "BTCUSDT")));
            long after = System.currentTimeMillis();

            var timings = harness.extractOutputValues().get(0).getPipelineTimings();
            assertThat(timings.getParseIn()).isEqualTo(100L);
            assertThat(timings.getParseOut()).isEqualTo(102L);
            assertThat(timings.getPairExtractIn()).isNotNull().isBetween(before, after);
            assertThat(timings.getPairExtractOut()).isNotNull()
                    .isBetween(timings.getPairExtractIn(), after);
            assertThat(timings.getTypeValidateIn()).isNull();
        }
    }

    /**
     * Given a market string not in exchange_markets, When the event flows through, Then it
     * is dropped (log + counter) — NOT dead-lettered.
     */
    @Test
    @DisplayName("drops events whose market is unknown")
    void dropsUnknownMarket() throws Exception {
        try (var harness = harness(Map.of("1|ETHUSDT", 9))) {
            harness.processElement(new StreamRecord<>(parsed(1, "BTCUSDT")));

            assertThat(harness.extractOutputValues()).isEmpty();
        }
    }

    /**
     * Given a market that exists for a DIFFERENT exchange, When the event flows through, Then it
     * is still dropped — the lookup key is (exchange, market), and two exchanges naming the same
     * symbol are two different rows.
     */
    @Test
    @DisplayName("keys the lookup on exchange as well as market")
    void doesNotResolveAcrossExchanges() throws Exception {
        try (var harness = harness(Map.of("1|BTCUSDT", 42))) {
            harness.processElement(new StreamRecord<>(parsed(6, "BTCUSDT")));

            assertThat(harness.extractOutputValues()).isEmpty();
        }
    }
}
