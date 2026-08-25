package io.tibobit.adjustment;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * The two flat percent rates one {@code exchange_markets} row carries for one (exchange, market):
 * our profit margin and the slippage allowance. Mirrors {@code RebaseFactors} in
 * flink/normalizer/job-rebaser — same shape, one row giving two related numbers.
 */
public class AdjustmentFactors implements Serializable {

    private final BigDecimal profitPercent;
    private final BigDecimal slippagePercent;

    public AdjustmentFactors(BigDecimal profitPercent, BigDecimal slippagePercent) {
        this.profitPercent = profitPercent;
        this.slippagePercent = slippagePercent;
    }

    public BigDecimal getProfitPercent() {
        return profitPercent;
    }

    public BigDecimal getSlippagePercent() {
        return slippagePercent;
    }
}
