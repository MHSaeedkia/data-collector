package io.tibobit.adjustment;

import java.math.BigDecimal;

/**
 * Decimal helpers. Prices are {@link BigDecimal} built from the wire STRING, never a double — the
 * platform-wide rule (memory/project_bigdecimal_rules.md): a double cannot hold 0.1 exactly, and an
 * order book that drifts is corrupt.
 *
 * <p>Deliberately duplicated from normalizer-common rather than depended upon: see the rationale in
 * this module's pom.xml. Only what this job uses is copied.
 */
final class Decimals {

    private Decimals() {
    }

    /**
     * One spelling per numeric value: {@code 62869.275000} and {@code 62869.275} are the same price
     * and must not appear as two. Also dodges {@code toString}'s scientific notation
     * ({@code 1E+3}), which is a valid decimal but not a price string anyone wants on a topic.
     */
    static String canonicalize(BigDecimal value) {
        if (value.signum() == 0) {
            return "0";
        }
        return value.stripTrailingZeros().toPlainString();
    }
}
