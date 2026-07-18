package io.tibobit.normalizer.rebase;

import io.tibobit.normalizer.lookup.RefreshingLookup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Loads the rebase exponents from exchange_markets as
 * {@code "{exchange_id}|{market_id}" → RebaseFactors} — keyed the way job 3 sees events
 * (market_id IS the pipeline's pair_id), not by the exchange's market string. Plugged into
 * RefreshingLookup so rebase edits are picked up without a job restart.
 */
public class RebaseFactorsLoader implements RefreshingLookup.Loader<String, RebaseFactors> {

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public RebaseFactorsLoader(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    @Override
    public Map<String, RebaseFactors> load() throws Exception {
        // Same child-first classloader dance as ExchangeMarketsLoader (see [[pair-extractor]]).
        Class.forName("org.postgresql.Driver");
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "SELECT exchange_id, market_id, price_amount_rebase, volume_amount_rebase"
                             + " FROM exchange_markets WHERE market_id IS NOT NULL")) {
            Map<String, RebaseFactors> factors = new HashMap<>();
            while (rs.next()) {
                factors.put(key(rs.getInt(1), rs.getInt(2)),
                        new RebaseFactors(rs.getInt(3), rs.getInt(4)));
            }
            return factors;
        }
    }

    public static String key(int exchangeId, int pairId) {
        return exchangeId + "|" + pairId;
    }
}
