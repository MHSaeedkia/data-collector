package io.tibobit.normalizer.typevalidate;

import io.tibobit.normalizer.lookup.RefreshingLookup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Loads the watch list — every SUBSCRIBED market and its staleness threshold —
 * from {@code exchange_markets} as {@code "{exchange_id}|{market_id}" ->
 * StalenessTick}, keyed the way job 2 sees events (market_id IS the pipeline's
 * pair_id). Plugged into {@link RefreshingLookup} so both a threshold edit and a
 * new subscription are picked up without a job restart.
 *
 * <p>
 * The value is the finished {@link StalenessTick} rather than a bare threshold
 * so nothing downstream has to parse an exchange id back out of the key string.
 * Parsing the key was one of the three specific defects that sank the timer
 * implementation of the control plane — it coupled the operator to the key
 * format and turned a wrong key selector into a crash rather than a bad record.
 *
 * <p>
 * <b>Only {@code status = 'subscribe'}.</b> The two pending statuses mean the
 * collector has not settled the row yet, so a market being subscribed right now
 * has no feed to be stale and asking for a snapshot would be asking for
 * something nobody has started collecting. Unsubscribed markets are not watched
 * at all: silence is the correct state for them.
 */
public class StalenessThresholdLoader implements RefreshingLookup.Loader<String, StalenessTick> {

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public StalenessThresholdLoader(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    @Override
    public Map<String, StalenessTick> load() throws Exception {
        // Same child-first classloader dance as ExchangeMarketsLoader (see [[pair-extractor]]):
        // Flink's child-first classloading hides the shaded driver from DriverManager unless it
        // is loaded explicitly first.
        Class.forName("org.postgresql.Driver");
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(
                        "SELECT exchange_id, market_id, staleness_threshold_seconds"
                                + " FROM exchange_markets"
                                + " WHERE market_id IS NOT NULL AND status = 'subscribe'")) {
            Map<String, StalenessTick> watched = new HashMap<>();
            while (rs.next()) {
                StalenessTick tick = new StalenessTick(rs.getInt(1), rs.getInt(2), rs.getInt(3));
                watched.put(tick.key(), tick);
            }
            return watched;
        }
    }
}
