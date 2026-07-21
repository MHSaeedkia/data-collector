package io.tibobit.orderbook.source;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Loads the set of pairs the job should consume, once at startup, from postgres.
 * A pair counts as subscribed if any exchange_markets row for it has status
 * 'subscribe'; DISTINCT collapses the per-exchange rows down to one entry per pair.
 */
public class PairsLoader {

    private static final String QUERY = "SELECT DISTINCT m.id " +
            "FROM exchange_markets em " +
            "JOIN markets m ON em.market_id = m.id " +
            "WHERE em.status = 'subscribe'";

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public record Pair(int id) {
        @Override
        public String toString() {
            return "p" + id;
        }
    }

    public PairsLoader(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    public List<Pair> load() throws SQLException {
        List<Pair> pairs = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(QUERY)) {
            while (rs.next()) {
                pairs.add(new Pair(rs.getInt("id")));
            }
        }
        return pairs;
    }
}
