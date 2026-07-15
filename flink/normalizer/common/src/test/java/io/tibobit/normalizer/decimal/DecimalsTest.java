package io.tibobit.normalizer.decimal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the pure BigDecimal helpers the rebase (job 3) and precision (job 4) stages are built
 * on. Values are always BigDecimal-from-wire-string (memory/project_bigdecimal_rules.md); these
 * pin the exact string/scale behaviour so a library-behaviour surprise (stripTrailingZeros
 * producing 1E+3, setScale rounding half-up) can never reach the wire.
 */
class DecimalsTest {

    /**
     * Given wire strings with trailing zeros, When canonicalized, Then trailing zeros are
     * stripped, plain notation is kept for round numbers (never 1E+3), and every flavour of
     * zero collapses to "0".
     */
    @Test
    @DisplayName("canonicalize strips trailing zeros, avoids scientific notation, collapses zero")
    void canonicalizes() {
        assertThat(Decimals.canonicalize(new BigDecimal("1.500"))).isEqualTo("1.5");
        assertThat(Decimals.canonicalize(new BigDecimal("62770.10"))).isEqualTo("62770.1");
        assertThat(Decimals.canonicalize(new BigDecimal("1000"))).isEqualTo("1000");
        assertThat(Decimals.canonicalize(new BigDecimal("0.000"))).isEqualTo("0");
        assertThat(Decimals.canonicalize(new BigDecimal("0"))).isEqualTo("0");
    }

    /**
     * Given a value and a rebase exponent from exchange_markets.*_rebase, When rebased, Then the
     * decimal point shifts exactly — negative divides, positive multiplies, zero is identity —
     * with no rounding ever.
     */
    @Test
    @DisplayName("rebase shifts the decimal point exactly by powers of ten")
    void rebases() {
        assertThat(Decimals.rebase(new BigDecimal("62775.5"), -1)).isEqualByComparingTo("6277.55");
        assertThat(Decimals.rebase(new BigDecimal("62775.5"), 3)).isEqualByComparingTo("62775500");
        assertThat(Decimals.rebase(new BigDecimal("62775.5"), 0)).isEqualByComparingTo("62775.5");
    }

    /**
     * Given a value with more decimals than the market precision, When truncated, Then digits
     * are cut (round DOWN) — never rounded half-up, which would fabricate a price the exchange
     * never quoted. Values already within precision gain trailing zeros only in scale, and
     * truncation can legitimately reach exactly zero (the job-4 truncate-to-zero hazard).
     */
    @Test
    @DisplayName("truncate cuts digits toward zero, never rounds up")
    void truncates() {
        assertThat(Decimals.truncate(new BigDecimal("0.031418"), 4)).isEqualByComparingTo("0.0314");
        assertThat(Decimals.truncate(new BigDecimal("0.039999"), 4)).isEqualByComparingTo("0.0399");
        assertThat(Decimals.truncate(new BigDecimal("1.5"), 4)).isEqualByComparingTo("1.5");
        assertThat(Decimals.truncate(new BigDecimal("0.00004"), 4)).isEqualByComparingTo("0");
    }
}
