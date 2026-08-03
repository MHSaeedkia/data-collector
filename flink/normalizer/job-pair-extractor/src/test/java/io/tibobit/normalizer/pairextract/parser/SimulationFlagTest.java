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
 * Tests the ONE rule every parser shares: NiFi injects {@code simulation} at the raw payload ROOT
 * (0 = live, 1 = simulation, other values undefined) and job 1 lifts it onto the event. It lives in
 * its own class rather than being repeated across the seven parser tests because the rule is not
 * per-exchange wire format — it is the same field in the same place for all of them.
 *
 * <p>There are two carriers, because there are two payload shapes. The six object-root exchanges get
 * the flag as a root FIELD. ex3/wallex's root is an array, so NiFi appends a trailing
 * {@code {"simulation": N}} element instead. Same field, same meaning, different place.
 *
 * <p>Each case takes a captured fixture (sample-raw-data.md), attaches the flag the way NiFi would,
 * and checks it comes out on every event the parser emits — "every" matters for ex5/ex8, whose
 * {@code data} array can produce more than one event from a single payload.
 */
class SimulationFlagTest {

    /** Re-serializes a fixture with {@code simulation} injected at the root, as NiFi does. */
    private static byte[] withSimulation(String fixture, int value) throws Exception {
        ObjectNode root = (ObjectNode) Json.MAPPER.readTree(Fixtures.bytes(fixture));
        root.put("simulation", value);
        return Json.MAPPER.writeValueAsBytes(root);
    }

    /**
     * ex3 form: strips the trailing metadata element, reproducing the 2-element frame wallex sent
     * before the flag existed. (The captured ex3 fixtures now carry the 3-element form, which is
     * what NiFi publishes.)
     */
    private static byte[] withoutTrailingElement(String fixture) throws Exception {
        ArrayNode root = (ArrayNode) Json.MAPPER.readTree(Fixtures.bytes(fixture));
        root.remove(2);
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
    @DisplayName("the root simulation flag reaches every event the parser emits")
    @CsvSource({
            "1, ex1-snapshot.json",
            "1, ex1-update.json",
            "2, ex2-snapshot.json",
            "2, ex2-update.json",
            "4, ex4-snapshot.json",
            "5, ex5-snapshot.json",
            "6, ex6-snapshot.json",
            "6, ex6-delta.json",
            "8, ex8-snapshot.json",
            "8, ex8-update.json",
    })
    void flagReachesEveryEvent(int exchangeId, String fixture) throws Exception {
        List<ParsedBookEvent> parsed = parse(parserFor(exchangeId), withSimulation(fixture, 1));

        assertThat(parsed).extracting(p -> p.getEvent().getSimulation()).containsOnly(1);
    }

    @ParameterizedTest(name = "ex{0} {1}")
    @DisplayName("a payload without the flag is live data (0), not a parse failure")
    @CsvSource({
            "1, ex1-snapshot.json",
            "2, ex2-update.json",
            "4, ex4-snapshot.json",
            "5, ex5-snapshot.json",
            "6, ex6-snapshot.json",
            "8, ex8-update.json",
    })
    void missingFlagIsZero(int exchangeId, String fixture) throws Exception {
        List<ParsedBookEvent> parsed = parse(parserFor(exchangeId), Fixtures.bytes(fixture));

        assertThat(parsed).extracting(p -> p.getEvent().getSimulation()).containsOnly(0);
    }

    /**
     * Values other than 0/1 are undefined but must still ride through untouched — job 1 does not get
     * to decide what a future value means.
     */
    @Test
    @DisplayName("an undefined flag value is carried verbatim, not clamped")
    void undefinedValueCarriedVerbatim() throws Exception {
        List<ParsedBookEvent> parsed =
                parse(parserFor(6), withSimulation("ex6-snapshot.json", 7));

        assertThat(parsed.get(0).getEvent().getSimulation()).isEqualTo(7);
    }

    /**
     * ex3/wallex carries the flag in a THIRD array element rather than a root field, because its
     * payload root is an array. Both sides of the half-book feed must pick it up.
     */
    @ParameterizedTest
    @CsvSource({"ex3-buy-depth.json", "ex3-sell-depth.json"})
    @DisplayName("ex3 reads the flag from the trailing element of its array envelope")
    void ex3ReadsTrailingElement(String fixture) throws Exception {
        List<ParsedBookEvent> parsed = parse(parserFor(3), Fixtures.bytes(fixture));

        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getSimulation()).isEqualTo(1);
        // The levels must survive the longer envelope untouched.
        assertThat(event.getAsks() == null ? event.getBids() : event.getAsks()).isNotEmpty();
    }

    /**
     * The 2-element ex3 frame predates the flag and must keep parsing — as live data, not as a
     * rejected frame.
     */
    @ParameterizedTest
    @CsvSource({"ex3-buy-depth.json", "ex3-sell-depth.json"})
    @DisplayName("ex3's older 2-element envelope still parses and reads as live")
    void ex3TwoElementEnvelopeIsLive(String fixture) throws Exception {
        List<ParsedBookEvent> parsed = parse(parserFor(3), withoutTrailingElement(fixture));

        assertThat(parsed.get(0).getEvent().getSimulation()).isZero();
    }

    /** A fourth element is not a shape we publish — the whitelist rule drops it rather than guess. */
    @Test
    @DisplayName("an ex3 envelope with more than three elements is dropped")
    void ex3OversizedEnvelopeDropped() throws Exception {
        ArrayNode root = (ArrayNode) Json.MAPPER.readTree(Fixtures.bytes("ex3-buy-depth.json"));
        root.add(Json.MAPPER.createObjectNode().put("unexpected", true));

        assertThat(parserFor(3).parse(Json.MAPPER.writeValueAsBytes(root))).isEmpty();
    }
}
