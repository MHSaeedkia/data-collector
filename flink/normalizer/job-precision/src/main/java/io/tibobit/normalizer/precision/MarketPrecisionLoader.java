package io.tibobit.normalizer.precision;

import io.tibobit.normalizer.lookup.RefreshingLookup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Loads the per-pair decimal places from markets as {@code pair_id → MarketPrecision}. Keyed by
 * pair alone — precision is a property of the market, not of the exchange reporting it (unlike
 * job 3's rebase exponents, which are per exchange_markets row). Plugged into RefreshingLookup so
 * precision edits are picked up without a job restart.
 */
public class MarketPrecisionLoader implements RefreshingLookup.Loader<Integer, MarketPrecision> {

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public MarketPrecisionLoader(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    @Override
    public Map<Integer, MarketPrecision> load() throws Exception {
        // Same child-first classloader dance as ExchangeMarketsLoader (see [[pair-extractor]]).
        Class.forName("org.postgresql.Driver");
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "SELECT id, price_precision, quantity_precision FROM markets")) {
            Map<Integer, MarketPrecision> precisions = new HashMap<>();
            while (rs.next()) {
                // Both precision columns are nullable — getObject, not getInt, which would turn
                // a null into a 0 and silently truncate everything to whole numbers.
                precisions.put(rs.getInt(1), new MarketPrecision(
                        (Integer) rs.getObject(2), (Integer) rs.getObject(3)));
            }
            return precisions;
        }
    }
}
