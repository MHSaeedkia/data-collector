package io.tibobit.normalizer.parse;

import io.tibobit.normalizer.model.ParsedBookEvent;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import io.tibobit.normalizer.parse.parser.RawExchangeParser;

import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link ParseFunction} drop/emit rules through a real operator harness (fake parser — the
 * per-exchange parsers have their own fixture tests). Split out of PairExtractFunctionTest on
 * 2026-09-19 along with the parsers themselves.
 */
class ParseFunctionTest {

    private static final byte[] ANY_PAYLOAD = {1, 2, 3};

    /** Stands in for the id NiFi injects; the real parsers read it off the payload. */
    private static final String NIFI_ID = "11111111-1111-4111-8111-111111111111";

    private static final RawExchangeParser FIXED_PARSER = payload -> {
        RawOrderBookEvent event =
                new RawOrderBookEvent(0, 0, "snapshot", 7L, 0L, 123L, List.of(), List.of());
        event.setSourceIds(List.of(NIFI_ID));
        return List.of(new ParsedBookEvent("BTCUSDT", event));
    };

    /** A parser whose payload carried no id — what Json.sourceIds returns for that case. */
    private static final RawExchangeParser NO_ID_PARSER = payload -> List.of(new ParsedBookEvent(
            "BTCUSDT",
            new RawOrderBookEvent(0, 0, "snapshot", 7L, 0L, 123L, List.of(), List.of())));

    private static OneInputStreamOperatorTestHarness<RawExchangeMessage, ParsedBookEvent> harness(
            Map<Integer, RawExchangeParser> parsers) throws Exception {
        OneInputStreamOperatorTestHarness<RawExchangeMessage, ParsedBookEvent> harness =
                new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(new ParseFunction(parsers)));
        harness.open();
        return harness;
    }

    /**
     * Given a parser for the exchange, When a message flows through, Then the event is emitted
     * with exchange_id taken from the topic name and the market string left as the exchange
     * wrote it — pair_id is deliberately still unset.
     */
    @Test
    @DisplayName("emits the parsed event with exchange_id stamped and the market unresolved")
    void emitsParsedEvent() throws Exception {
        try (var harness = harness(Map.of(1, FIXED_PARSER))) {
            harness.processElement(new StreamRecord<>(new RawExchangeMessage(1, ANY_PAYLOAD)));

            List<ParsedBookEvent> out = harness.extractOutputValues();
            assertThat(out).hasSize(1);
            assertThat(out.get(0).getMarket()).isEqualTo("BTCUSDT");
            assertThat(out.get(0).getEvent().getExchangeId()).isEqualTo(1);
            assertThat(out.get(0).getEvent().getPairId()).isZero();
            assertThat(out.get(0).getEvent().getType()).isEqualTo("snapshot");
            assertThat(out.get(0).getEvent().getSequenceId()).isEqualTo(7L);
        }
    }

    /**
     * Given a payload NiFi stamped with an id, When the event is emitted, Then that id is the
     * event's single source and the event carries a fresh id of its own — the first link of
     * the lineage chain.
     */
    @Test
    @DisplayName("takes NiFi's id as the source and mints its own")
    void stampsLineage() throws Exception {
        try (var harness = harness(Map.of(1, FIXED_PARSER))) {
            harness.processElement(new StreamRecord<>(new RawExchangeMessage(1, ANY_PAYLOAD)));

            RawOrderBookEvent event = harness.extractOutputValues().get(0).getEvent();
            assertThat(event.getSourceIds()).containsExactly(NIFI_ID);
            assertThat(event.getId()).isNotBlank().isNotEqualTo(NIFI_ID);
            assertThat(UUID.fromString(event.getId())).isNotNull();
        }
    }

    /**
     * Given one payload that fans out to several events (ex8's data array, ex3's per-side frames),
     * When they are emitted, Then they share the one NiFi source but each gets a DISTINCT id —
     * they are separate records on the parsed topic, so they cannot share an identity.
     */
    @Test
    @DisplayName("gives each fanned-out event its own id")
    void mintsDistinctIdPerFannedOutEvent() throws Exception {
        RawExchangeParser fanOut = payload -> {
            List<ParsedBookEvent> events = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                RawOrderBookEvent event =
                        new RawOrderBookEvent(0, 0, "snapshot", 7L, 0L, 123L, List.of(), List.of());
                event.setSourceIds(List.of(NIFI_ID));
                events.add(new ParsedBookEvent("BTCUSDT", event));
            }
            return events;
        };
        try (var harness = harness(Map.of(1, fanOut))) {
            harness.processElement(new StreamRecord<>(new RawExchangeMessage(1, ANY_PAYLOAD)));

            List<ParsedBookEvent> out = harness.extractOutputValues();
            assertThat(out).hasSize(3);
            assertThat(out).allSatisfy(
                    p -> assertThat(p.getEvent().getSourceIds()).containsExactly(NIFI_ID));
            assertThat(out.stream().map(p -> p.getEvent().getId())).doesNotHaveDuplicates();
        }
    }

    /**
     * Given a payload with no id (NiFi not yet updated, or a pre-change replay), When it flows
     * through, Then it is dropped rather than emitted with no parent — user decision 2026-08-03.
     * This is the rule that makes NiFi a hard dependency of job 1, and it has to be checked HERE:
     * the very next line overwrites the id field with this job's own.
     */
    @Test
    @DisplayName("drops payloads carrying no id")
    void dropsWhenNoId() throws Exception {
        try (var harness = harness(Map.of(1, NO_ID_PARSER))) {
            harness.processElement(new StreamRecord<>(new RawExchangeMessage(1, ANY_PAYLOAD)));

            assertThat(harness.extractOutputValues()).isEmpty();
        }
    }

    /**
     * Given a message flowing through, When the event is emitted, Then job 1 stamps its parse
     * in/out timings (in ≤ out, both within the processing window) and leaves the downstream
     * stages null — this is the "came from raw topic" anchor for latency tracking.
     */
    @Test
    @DisplayName("stamps parse in/out timings on the emitted event")
    void stampsParseTimings() throws Exception {
        long before = System.currentTimeMillis();
        try (var harness = harness(Map.of(1, FIXED_PARSER))) {
            harness.processElement(new StreamRecord<>(new RawExchangeMessage(1, ANY_PAYLOAD)));
            long after = System.currentTimeMillis();

            var timings = harness.extractOutputValues().get(0).getEvent().getPipelineTimings();
            assertThat(timings.getParseIn()).isNotNull().isBetween(before, after);
            assertThat(timings.getParseOut()).isNotNull().isBetween(timings.getParseIn(), after);
            assertThat(timings.getPairExtractIn()).isNull();
        }
    }

    /**
     * Given a message from an exchange with no registered parser (no seeded exchange is in
     * that state since ex9 lbank landed 2026-08-25 — this is the future-topic case), When it
     * flows through, Then it is dropped without crashing.
     */
    @Test
    @DisplayName("drops messages from exchanges without a parser")
    void dropsWhenNoParser() throws Exception {
        try (var harness = harness(Map.of(1, FIXED_PARSER))) {
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
        try (var harness = harness(Map.of(1, throwing))) {
            harness.processElement(new StreamRecord<>(new RawExchangeMessage(1, ANY_PAYLOAD)));

            assertThat(harness.extractOutputValues()).isEmpty();
        }
    }
}
