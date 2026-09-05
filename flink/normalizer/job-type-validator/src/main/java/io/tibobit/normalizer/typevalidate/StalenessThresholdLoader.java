package io.tibobit.normalizer.typevalidate;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import io.tibobit.normalizer.lookup.RefreshingLookup;

/**
 * Loads the watch list — every SUBSCRIBED market and its staleness threshold —
 * from {@code exchange_markets} as {@code "{exchange_id}|{market_id}" ->
 * WatchedMarket}, keyed the way job 2 sees events (market_id IS the pipeline's
 * pair_id). Plugged into {@link RefreshingLookup} so both a threshold edit and
 * a new subscription are picked up without a job restart.
 *
 * <p>
 * A market absent from this map is simply not watched: {@link
 * TypeValidateFunction} arms no silence timer for it. That is what makes
 * unsubscribing take effect on its own, and it is also why a market that has
 * never sent an event is invisible here — it has no keyed state to watch.
 * Never- received markets are the staleness exporter's job, not job 2's (see
 * memory/project_staleness_exporter.md).
 *
 * <p>
 * <b>Only {@code status = 'subscribe'}.</b> The two pending statuses mean the
 * collector has not settled the row yet, so a market being subscribed right now
 * has no feed to be stale and asking for a snapshot would be asking for
 * something nobody has started collecting. Unsubscribed markets are not watched
 * at all: silence is the correct state for them.
 */
public class StalenessThresholdLoader implements RefreshingLookup.Loader<String, WatchedMarket> {

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public StalenessThresholdLoader(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    @Override
    public Map<String, WatchedMarket> load() throws Exception {
        // Same child-first classloader dance as ExchangeMarketsLoader (see [[pair-extractor]]):
        // Flink's child-first classloading hides the shaded driver from DriverManager unless it
        // is loaded explicitly first.
        Class.forName("org.postgresql.Driver");
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password); Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(
                "SELECT exchange_id, market_id, staleness_threshold_seconds"
                + " FROM exchange_markets"
                + " WHERE market_id IS NOT NULL AND status = 'subscribe'")) {
            Map<String, WatchedMarket> watched = new HashMap<>();
            while (rs.next()) {
                WatchedMarket market = new WatchedMarket(rs.getInt(1), rs.getInt(2), rs.getInt(3));
                watched.put(market.key(), market);
            }
            return watched;
        }
    }
}
