package io.tibobit.adjustment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Loads {@code our_profit_percent}/{@code slippage_percent} from exchange_markets as
 * {@code "{exchange_id}|{pair_id}" → AdjustmentFactors}, keyed the way this job sees levels
 * (a level's own {@code exchange_id} against the book's {@code pair_id} — market_id IS the
 * pipeline's pair_id, same as job 3's RebaseFactorsLoader). Plugged into RefreshingLookup so a
 * rate edit is picked up without a job restart.
 */
public class AdjustmentFactorsLoader implements RefreshingLookup.Loader<String, AdjustmentFactors> {

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public AdjustmentFactorsLoader(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    @Override
    public Map<String, AdjustmentFactors> load() throws Exception {
        // Same child-first classloader dance as ExchangeMarketsLoader / RebaseFactorsLoader.
        Class.forName("org.postgresql.Driver");
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "SELECT exchange_id, market_id, our_profit_percent, slippage_percent"
                             + " FROM exchange_markets WHERE market_id IS NOT NULL")) {
            Map<String, AdjustmentFactors> factors = new HashMap<>();
            while (rs.next()) {
                factors.put(key(rs.getInt(1), rs.getInt(2)),
                        new AdjustmentFactors(rs.getBigDecimal(3), rs.getBigDecimal(4)));
            }
            return factors;
        }
    }

    public static String key(int exchangeId, int pairId) {
        return exchangeId + "|" + pairId;
    }
}
