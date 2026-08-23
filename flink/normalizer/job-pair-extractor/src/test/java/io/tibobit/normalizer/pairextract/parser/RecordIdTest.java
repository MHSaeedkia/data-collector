package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the second rule every parser shares: NiFi injects {@code id} on the raw payload — the
 * uuid it minted when it wrote that payload — and job 1 lifts it onto the event as its
 * {@code source_ids}. Same shape of rule as {@link SimulationFlagTest}, same two carriers (root
 * field for the six object-root exchanges, the trailing metadata object for ex3/wallex), so it lives
 * in its own class for the same reason: it is not per-exchange wire format.
 *
 * <p>The one thing that differs from the simulation flag is what ABSENCE means. A missing
 * {@code simulation} is benign and reads as 0 (live). A missing {@code id} is not benign — it
 * yields empty {@code source_ids}, which {@code PairExtractFunction} turns into a drop. Parsers
 * themselves still parse such a payload happily; the drop is the function's decision, and
 * {@code PairExtractFunctionTest.dropsWhenNoId} covers it.
 */
class RecordIdTest {

    private static final String NIFI_ID = "11111111-1111-4111-8111-111111111111";

    /** Re-serializes a fixture with {@code id} injected at the root, as NiFi does. */
    private static byte[] withId(String fixture, String value) throws Exception {
        ObjectNode root = (ObjectNode) Json.MAPPER.readTree(Fixtures.bytes(fixture));
        root.put("id", value);
        return Json.MAPPER.writeValueAsBytes(root);
    }

    private static List<ParsedBookEvent> parse(RawExchangeParser parser, byte[] payload)
            throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(payload);
        assertThat(parsed).as("fixture should still parse").isNotEmpty();
        return parsed;
    }

    private static RawExchangeParser parserFor(int exchangeId) {
        return Parsers.byExchangeId().get(exchangeId);
    }

    @ParameterizedTest(name = "ex{0} {1}")
    @DisplayName("the root id becomes source_ids on every event the parser emits")
    @CsvSource({
            "1, ex1-snapshot.json",
            "1, ex1-update.json",
            "2, ex2-snapshot.json",
            "2, ex2-update.json",
            "4, ex4-snapshot.json",
            "5, ex5-snapshot.json",
            "5, ex5-update.json",
            "5, ex5-rest-snapshot.json",
            "6, ex6-snapshot.json",
            "6, ex6-delta.json",
            "8, ex8-snapshot.json",
            "8, ex8-update.json",
    })
    void idReachesEveryEvent(int exchangeId, String fixture) throws Exception {
        List<ParsedBookEvent> parsed =
                parse(parserFor(exchangeId), withId(fixture, NIFI_ID));

        assertThat(parsed).allSatisfy(p ->
                assertThat(p.getEvent().getSourceIds()).containsExactly(NIFI_ID));
    }

    /**
     * A payload with no id still PARSES — it just comes out with no source, which is what the
     * function reads as "drop this". Parsers do not enforce the rule; conflating "unparseable" with
     * "untraceable" would lose the distinction between a malformed frame and a mis-configured NiFi.
     */
    @ParameterizedTest(name = "ex{0} {1}")
    @DisplayName("a payload without id parses but carries no source")
    @CsvSource({
            "1, ex1-snapshot.json",
            "2, ex2-update.json",
            "4, ex4-snapshot.json",
            "5, ex5-snapshot.json",
            "5, ex5-update.json",
            "5, ex5-rest-snapshot.json",
            "6, ex6-snapshot.json",
            "8, ex8-update.json",
    })
    void missingIdLeavesNoSource(int exchangeId, String fixture) throws Exception {
        List<ParsedBookEvent> parsed = parse(parserFor(exchangeId), Fixtures.bytes(fixture));

        assertThat(parsed).allSatisfy(p -> assertThat(p.getEvent().getSourceIds()).isEmpty());
    }

    /** A blank id is as useless as an absent one and must not become a source. */
    @Test
    @DisplayName("a blank id is treated as absent, not as a real id")
    void blankIdLeavesNoSource() throws Exception {
        List<ParsedBookEvent> parsed = parse(parserFor(6), withId("ex6-snapshot.json", "   "));

        assertThat(parsed.get(0).getEvent().getSourceIds()).isEmpty();
    }

    /**
     * ex3/wallex carries id in the SAME trailing object as simulation, not as a fourth element —
     * its payload root is an array, so there is no root field to inject into.
     */
    @ParameterizedTest
    @CsvSource({"ex3-buy-depth.json", "ex3-sell-depth.json"})
    @DisplayName("ex3 reads id from the trailing element of its array envelope")
    void ex3ReadsTrailingElement(String fixture) throws Exception {
        List<ParsedBookEvent> parsed = parse(parserFor(3), Fixtures.bytes(fixture));

        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getSourceIds()).hasSize(1);
        assertThat(event.getSourceIds().get(0)).startsWith("33333333-");
        // The levels must survive the longer envelope untouched.
        assertThat(event.getAsks() == null ? event.getBids() : event.getAsks()).isNotEmpty();
    }

    /**
     * ex3's pre-change 2-element frame still parses, but now yields no source — so in practice it
     * gets dropped downstream. Kept as an explicit test because it is the one case where ex3 behaves
     * differently from the six object-root exchanges' "absent field" path.
     */
    @ParameterizedTest
    @CsvSource({"ex3-buy-depth.json", "ex3-sell-depth.json"})
    @DisplayName("ex3's older 2-element envelope parses with no source")
    void ex3TwoElementEnvelopeHasNoSource(String fixture) throws Exception {
        ArrayNode root = (ArrayNode) Json.MAPPER.readTree(Fixtures.bytes(fixture));
        root.remove(2);

        List<ParsedBookEvent> parsed =
                parse(parserFor(3), Json.MAPPER.writeValueAsBytes(root));

        assertThat(parsed.get(0).getEvent().getSourceIds()).isEmpty();
    }
}
