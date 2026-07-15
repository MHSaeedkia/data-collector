package io.tibobit.normalizer.lookup;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link RefreshingLookup} with a fake loader (no JDBC): the reference-data contract is
 * fail-fast on the initial load, swap-on-successful-refresh, and keep-last-good on a failed
 * refresh. {@code refresh()} is driven directly for determinism — the schedule itself is a
 * one-line Executors call not worth a timing-dependent test.
 */
class RefreshingLookupTest {

    private RefreshingLookup<String, Integer> lookup;

    @AfterEach
    void tearDown() {
        if (lookup != null) {
            lookup.close();
        }
    }

    /**
     * Given a loader with reference data, When opened, Then lookups serve the loaded map and
     * unknown keys return null (callers log-and-drop unknowns, see todo.md M2).
     */
    @Test
    @DisplayName("serves the initially loaded map")
    void servesInitialLoad() throws Exception {
        lookup = new RefreshingLookup<>(() -> Map.of("BTCUSDT", 1), 60_000);

        lookup.open();

        assertThat(lookup.get("BTCUSDT")).isEqualTo(1);
        assertThat(lookup.get("UNKNOWN")).isNull();
    }

    /**
     * Given a loader that fails on first load, When opened, Then the failure propagates — the
     * job must fail fast rather than run with an empty lookup and silently drop every event.
     */
    @Test
    @DisplayName("propagates an initial load failure")
    void failsFastOnInitialLoadFailure() {
        lookup = new RefreshingLookup<>(() -> {
            throw new IllegalStateException("db down");
        }, 60_000);

        assertThatThrownBy(lookup::open).isInstanceOf(IllegalStateException.class)
                .hasMessage("db down");
    }

    /**
     * Given reference data that changes between loads, When a refresh runs, Then lookups serve
     * the new snapshot — new DB subscriptions become visible without a job restart.
     */
    @Test
    @DisplayName("swaps in the new map on a successful refresh")
    void refreshSwapsSnapshot() throws Exception {
        AtomicReference<Map<String, Integer>> data = new AtomicReference<>(Map.of("BTCUSDT", 1));
        lookup = new RefreshingLookup<>(data::get, 60_000);
        lookup.open();
        data.set(Map.of("BTCUSDT", 1, "ETHUSDT", 2));

        lookup.refresh();

        assertThat(lookup.get("ETHUSDT")).isEqualTo(2);
    }

    /**
     * Given a loader that starts failing after a good load, When a refresh fails, Then lookups
     * keep serving the last-good snapshot — briefly stale reference data is better than
     * dropping live events.
     */
    @Test
    @DisplayName("keeps the last-good snapshot when a refresh fails")
    void refreshFailureKeepsLastGood() throws Exception {
        AtomicReference<Boolean> fail = new AtomicReference<>(false);
        lookup = new RefreshingLookup<>(() -> {
            if (fail.get()) {
                throw new IllegalStateException("db down");
            }
            return Map.of("BTCUSDT", 1);
        }, 60_000);
        lookup.open();
        fail.set(true);

        lookup.refresh();

        assertThat(lookup.get("BTCUSDT")).isEqualTo(1);
    }
}
