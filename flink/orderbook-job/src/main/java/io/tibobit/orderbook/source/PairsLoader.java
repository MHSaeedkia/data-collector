package io.tibobit.orderbook.source;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class PairsLoader {

    private static final String QUERY = "SELECT DISTINCT m.base , m.quote " +
            "FROM exchange_markets em " +
            "JOIN markets m ON em.market_id = m.id " +
            "WHERE em.status = 'subscribe'";

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public record Pair(String base, String quote) {
        @Override
        public String toString() {
            return base + ", " + quote;
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
                pairs.add(new Pair(rs.getString("base"), rs.getString("quote")));
            }
        }
        return pairs;
    }
}
