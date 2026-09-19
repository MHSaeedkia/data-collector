package io.tibobit.normalizer.parse.parser;

import io.tibobit.normalizer.model.ParsedBookEvent;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the rule behind {@code exchange_event_time}: it holds the exchange's OWN clock when the
 * wire carries one, and NULL when it does not. Its own class, next to {@link SimulationFlagTest}
 * and {@link RecordIdTest}, because the rule is not per-exchange wire format — it is the same
 * question asked of every parser.
 *
 * <p>{@code event_time} answers that question differently and always has: where a feed sends no
 * clock, job 1 substitutes its own processing time there. That substitution is invisible in
 * {@code event_time} alone — it is a plausible-looking epoch millis either way — and reading it as
 * an exchange timestamp makes the feed's true lag measure as ~0. The null here is what tells the
 * two apart, so an accidentally-filled value is the bug worth catching.
 *
 * <p>ex7/ompfinex is the one exchange that would need BOTH cases (its snapshots carry a clock, its
 * updates do not) and it still has no captured fixture, the same gap {@link SimulationFlagTest}
 * records.
 */
class ExchangeEventTimeTest {

    private static RawExchangeParser parserFor(int exchangeId) {
        return Parsers.byExchangeId().get(exchangeId);
    }

    /**
     * The six feeds that timestamp their own frames. Every event a parser emits must carry that
     * timestamp, and it must be the SAME value job 1 put in {@code event_time} — both are filled
     * from one number off the wire, so a difference means one of them was computed instead of read.
     */
    @ParameterizedTest(name = "ex{0} {1}")
    @DisplayName("a feed with its own clock carries it, equal to event_time")
    @CsvSource({
            "1, ex1-snapshot.json",
            "1, ex1-update.json",
            "2, ex2-snapshot.json",
            "2, ex2-update.json",
            "5, ex5-snapshot.json",
            "5, ex5-snapshot-2.json",
            "6, ex6-snapshot.json",
            "6, ex6-delta.json",
            "6, ex6-rest-snapshot.json",
            "8, ex8-snapshot.json",
            "8, ex8-update.json",
            "9, ex9-snapshot.json",
    })
    void wireClockIsCarried(int exchangeId, String fixture) throws Exception {
        List<ParsedBookEvent> parsed = parserFor(exchangeId).parse(Fixtures.bytes(fixture));

        assertThat(parsed).as("fixture should still parse").isNotEmpty();
        for (ParsedBookEvent each : parsed) {
            RawOrderBookEvent event = each.getEvent();
            assertThat(event.getExchangeEventTime())
                    .as("ex%d sends a timestamp, so it must reach exchange_event_time", exchangeId)
                    .isNotNull()
                    .isEqualTo(event.getEventTime());
        }
    }

    /**
     * ex3/wallex and ex4/ramzinex put no timestamp on the wire at all. {@code event_time} is
     * therefore job 1's processing time — asserted here so the two fields are seen differing on
     * purpose — and {@code exchange_event_time} must stay null rather than repeat it.
     */
    @ParameterizedTest(name = "ex{0} {1}")
    @DisplayName("a feed with no clock keeps a null, and does not inherit the processing time")
    @CsvSource({
            "3, ex3-buy-depth.json",
            "3, ex3-sell-depth.json",
            "4, ex4-snapshot.json",
    })
    void missingWireClockStaysNull(int exchangeId, String fixture) throws Exception {
        long before = System.currentTimeMillis();

        List<ParsedBookEvent> parsed = parserFor(exchangeId).parse(Fixtures.bytes(fixture));

        assertThat(parsed).as("fixture should still parse").isNotEmpty();
        for (ParsedBookEvent each : parsed) {
            RawOrderBookEvent event = each.getEvent();
            assertThat(event.getExchangeEventTime())
                    .as("ex%d sends no timestamp, so exchange_event_time must stay null", exchangeId)
                    .isNull();
            assertThat(event.getEventTime())
                    .as("event_time is job 1's processing time for ex%d", exchangeId)
                    .isGreaterThanOrEqualTo(before);
        }
    }
}
