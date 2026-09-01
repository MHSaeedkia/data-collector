package io.tibobit.normalizer.typevalidate;

import io.tibobit.normalizer.lookup.RefreshingLookup;

import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The fan-out's whole job is "one pulse in, one tick per watched market out",
 * including for markets that have never sent data — which is the only reason
 * the cold-start case is detectable at all.
 */
class StalenessTickFanOutTest {

    /** Collector that just records what it was given. */
    private static final class Captured implements Collector<StalenessTick> {
        private final List<StalenessTick> ticks = new ArrayList<>();

        @Override
        public void collect(StalenessTick tick) {
            ticks.add(tick);
        }

        @Override
        public void close() {
        }
    }

    private static RefreshingLookup<String, StalenessTick> lookupOf(StalenessTick... ticks) {
        Map<String, StalenessTick> map = new LinkedHashMap<>();
        for (StalenessTick tick : ticks) {
            map.put(tick.key(), tick);
        }
        // Refresh interval large enough that no background reload runs during a test.
        return new RefreshingLookup<>(() -> map, 3_600_000L);
    }

    private static StalenessTickFanOut openedOn(RefreshingLookup<String, StalenessTick> lookup)
            throws Exception {
        StalenessTickFanOut fanOut = new StalenessTickFanOut(lookup);
        fanOut.open(null);
        return fanOut;
    }

    @Test
    void onePulseEmitsOneTickPerWatchedMarket() throws Exception {
        StalenessTickFanOut fanOut = openedOn(lookupOf(
                new StalenessTick(6, 1, 60),
                new StalenessTick(8, 1, 30)));
        Captured captured = new Captured();

        fanOut.flatMap(1L, captured);

        assertThat(captured.ticks)
                .extracting(StalenessTick::key, StalenessTick::getThresholdSeconds)
                .containsExactlyInAnyOrder(
                        org.assertj.core.groups.Tuple.tuple("6|1", 60),
                        org.assertj.core.groups.Tuple.tuple("8|1", 30));
    }

    @Test
    void eachPulseEmitsTheFullWatchListAgain() throws Exception {
        StalenessTickFanOut fanOut = openedOn(lookupOf(new StalenessTick(6, 1, 60)));
        Captured captured = new Captured();

        fanOut.flatMap(1L, captured);
        fanOut.flatMap(2L, captured);
        fanOut.flatMap(3L, captured);

        // Staleness is re-evaluated on every pulse, so the watch list is re-sent every
        // time rather than diffed — a market that fell silent between pulses is only
        // noticed because its tick keeps arriving.
        assertThat(captured.ticks).hasSize(3);
    }

    @Test
    void emitsCopiesSoReferenceDataIsNeverAliasedIntoTheStream() throws Exception {
        StalenessTick watched = new StalenessTick(6, 1, 60);
        StalenessTickFanOut fanOut = openedOn(lookupOf(watched));
        Captured captured = new Captured();

        fanOut.flatMap(1L, captured);

        assertThat(captured.ticks).hasSize(1);
        assertThat(captured.ticks.get(0)).isNotSameAs(watched);
    }

    @Test
    void anEmptyWatchListEmitsNothing() throws Exception {
        StalenessTickFanOut fanOut = openedOn(lookupOf());
        Captured captured = new Captured();

        fanOut.flatMap(1L, captured);

        // No subscribed markets means nothing to watch — NOT "everything is stale".
        assertThat(captured.ticks).isEmpty();
    }

    @Test
    void keyMatchesTheFormatTheMainStreamIsPartitionedBy() {
        // If these ever diverge, connect() silently pairs ticks with the wrong market's
        // state and every staleness verdict is wrong. TypeValidatorJob's ExchangePairKey
        // is exchange_id + "|" + pair_id.
        assertThat(new StalenessTick(6, 1, 60).key()).isEqualTo("6|1");
    }

    @Test
    void thresholdIsExposedInMillisForComparisonAgainstProcessingTime() {
        assertThat(new StalenessTick(6, 1, 60).thresholdMs()).isEqualTo(60_000L);
    }
}
