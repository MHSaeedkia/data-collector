package io.tibobit.normalizer.decimal;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * BigDecimal helpers for the normalizer jobs — prices/quantities are BigDecimal-from-wire-string,
 * never double (memory/project_bigdecimal_rules.md). All pure functions.
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

    /** Job-3 rebase: shift the decimal point by {@code rebase} powers of ten (exact, no rounding). */
    public static BigDecimal rebase(BigDecimal value, int rebase) {
        return value.scaleByPowerOfTen(rebase);
    }

    /** Job-4 precision: truncate (round DOWN, never half-up) to {@code precision} decimal places. */
    public static BigDecimal truncate(BigDecimal value, int precision) {
        return value.setScale(precision, RoundingMode.DOWN);
    }
}
