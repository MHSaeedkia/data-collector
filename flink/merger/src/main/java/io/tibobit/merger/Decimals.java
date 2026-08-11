package io.tibobit.merger;

import java.math.BigDecimal;

/**
 * BigDecimal helpers — prices/quantities are BigDecimal-from-wire-string, never double
 * (memory/project_bigdecimal_rules.md). Pure functions.
 *
 * <p>Only {@code canonicalize} is carried over from normalizer-common's version; this job does no
 * rebasing and no truncation, so those two helpers are not copied.
 */
public final class Decimals {

    private Decimals() {
    }

    /**
     * Canonical wire string for a value: trailing zeros stripped, plain notation (never
     * scientific — toPlainString undoes stripTrailingZeros turning 1000 into 1E+3), and any
     * flavour of zero (0, 0.00) collapsed to "0".
     */
    public static String canonicalize(BigDecimal value) {
        if (value.signum() == 0) {
            return "0";
        }
        return value.stripTrailingZeros().toPlainString();
    }
}
