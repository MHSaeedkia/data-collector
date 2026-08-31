package io.tibobit.normalizer.pairextract;

import io.tibobit.normalizer.lookup.RefreshingLookup;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import io.tibobit.normalizer.pairextract.parser.ParsedBookEvent;
import io.tibobit.normalizer.pairextract.parser.RawExchangeParser;

import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link PairExtractFunction} drop/emit rules through a real operator harness (fake
 * parser + in-memory lookup — the per-exchange parsers have their own fixture tests, and the
 * JDBC loader is exercised by the live smoke, not here).
 */
class PairExtractFunctionTest {

    private static final byte[] ANY_PAYLOAD = {1, 2, 3};

    private static final RawExchangeParser FIXED_PARSER = payload -> List.of(new ParsedBookEvent(
            "BTCUSDT",
            new RawOrderBookEvent(0, 0, "snapshot", 7L, 0L, 123L, List.of(), List.of())));

    private static OneInputStreamOperatorTestHarness<RawExchangeMessage, RawOrderBookEvent> harness(
            Map<Integer, RawExchangeParser> parsers, Map<String, Integer> marketRows)
            throws Exception {
        PairExtractFunction fn = new PairExtractFunction(parsers,
                new RefreshingLookup<>(() -> marketRows, 60_000L));
        OneInputStreamOperatorTestHarness<RawExchangeMessage, RawOrderBookEvent> harness =
                new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(fn));
        harness.open();
        return harness;
    }

    /**
     * Given a parser for the exchange and a known market, When a message flows through, Then
     * the event is emitted with exchange_id from the topic and pair_id from exchange_markets.
     */
    @Test
    @DisplayName("emits the parsed event with resolved ids")
    void emitsResolvedEvent() throws Exception {
        try (var harness = harness(Map.of(1, FIXED_PARSER), Map.of("1|BTCUSDT", 42))) {
            harness.processElement(new StreamRecord<>(new RawExchangeMessage(1, ANY_PAYLOAD)));

            List<RawOrderBookEvent> out = harness.extractOutputValues();
            assertThat(out).hasSize(1);
            assertThat(out.get(0).getExchangeId()).isEqualTo(1);
            assertThat(out.get(0).getPairId()).isEqualTo(42);
            assertThat(out.get(0).getType()).isEqualTo("snapshot");
            assertThat(out.get(0).getSequenceId()).isEqualTo(7L);
        }
    }

    /**
     * Given a message flowing through, When the event is emitted, Then job 1 stamps its
     * pair-extract in/out timings (in ≤ out, both within the processing window) and leaves the
     * downstream stages null — this is the "came from raw topic" anchor for latency tracking.
     */
    @Test
    @DisplayName("stamps pair-extract in/out timings on the emitted event")
    void stampsPairExtractTimings() throws Exception {
        long before = System.currentTimeMillis();
        try (var harness = harness(Map.of(1, FIXED_PARSER), Map.of("1|BTCUSDT", 42))) {
            harness.processElement(new StreamRecord<>(new RawExchangeMessage(1, ANY_PAYLOAD)));
            long after = System.currentTimeMillis();

            var timings = harness.extractOutputValues().get(0).getPipelineTimings();
            assertThat(timings.getPairExtractIn()).isNotNull().isBetween(before, after);
            assertThat(timings.getPairExtractOut()).isNotNull()
                    .isBetween(timings.getPairExtractIn(), after);
            assertThat(timings.getTypeValidateIn()).isNull();
        }
    }

    /**
     * Given a market string not in exchange_markets, When the message flows through, Then it
     * is dropped (log + counter) — NOT dead-lettered.
     */
    @Test
    @DisplayName("drops events whose market is unknown")
    void dropsUnknownMarket() throws Exception {
        try (var harness = harness(Map.of(1, FIXED_PARSER), Map.of("1|ETHUSDT", 9))) {
            harness.processElement(new StreamRecord<>(new RawExchangeMessage(1, ANY_PAYLOAD)));

            assertThat(harness.extractOutputValues()).isEmpty();
        }
    }

    /**
     * Given a message from an exchange with no registered parser (postponed ex7), When it
     * flows through, Then it is dropped without crashing.
     */
    @Test
    @DisplayName("drops messages from exchanges without a parser")
    void dropsWhenNoParser() throws Exception {
        try (var harness = harness(Map.of(1, FIXED_PARSER), Map.of("7|BTCUSDT", 42))) {
            harness.processElement(new StreamRecord<>(new RawExchangeMessage(7, ANY_PAYLOAD)));

            assertThat(harness.extractOutputValues()).isEmpty();
        }
    }

    /**
     * Given a parser that throws on a malformed frame, When the message flows through, Then
     * the frame is dropped and the job keeps running — the whitelist never-crash rule.
     */
    @Test
    @DisplayName("drops unparseable frames instead of crashing")
    void dropsUnparseableFrame() throws Exception {
        RawExchangeParser throwing = payload -> {
            throw new IllegalStateException("malformed frame");
        };
        try (var harness = harness(Map.of(1, throwing), Map.of("1|BTCUSDT", 42))) {
            harness.processElement(new StreamRecord<>(new RawExchangeMessage(1, ANY_PAYLOAD)));

            assertThat(harness.extractOutputValues()).isEmpty();
        }
    }
}
