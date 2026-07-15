package io.tibobit.normalizer.pairextract;

import io.tibobit.normalizer.lookup.RefreshingLookup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Loads the whole exchange_markets table as {@code "{exchange_id}|{market}" → market_id}
 * (market_id IS the pipeline's pair_id). Plugged into RefreshingLookup so new/changed rows
 * are picked up without a job restart.
 */
public class ExchangeMarketsLoader implements RefreshingLookup.Loader<String, Integer> {

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public ExchangeMarketsLoader(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    @Override
    public Map<String, Integer> load() throws Exception {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "SELECT exchange_id, market, market_id FROM exchange_markets")) {
            Map<String, Integer> markets = new HashMap<>();
            while (rs.next()) {
                markets.put(key(rs.getInt(1), rs.getString(2)), rs.getInt(3));
            }
            return markets;
        }
    }

    public static String key(int exchangeId, String market) {
        return exchangeId + "|" + market;
    }
}
