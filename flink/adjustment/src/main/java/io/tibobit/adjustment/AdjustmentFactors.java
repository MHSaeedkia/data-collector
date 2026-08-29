package io.tibobit.adjustment;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * The three flat percent rates one {@code exchange_markets} row carries for one (exchange, market):
 * our profit margin, the slippage allowance, and the buy/sell commission. Mirrors {@code
 * RebaseFactors} in flink/normalizer/job-rebaser — same shape, one row giving several related
 * numbers.
 */
public class AdjustmentFactors implements Serializable {

    private final BigDecimal profitPercent;
    private final BigDecimal slippagePercent;
    private final BigDecimal commissionPercent;

    public AdjustmentFactors(BigDecimal profitPercent, BigDecimal slippagePercent, BigDecimal commissionPercent) {
        this.profitPercent = profitPercent;
        this.slippagePercent = slippagePercent;
        this.commissionPercent = commissionPercent;
    }

    public BigDecimal getProfitPercent() {
        return profitPercent;
    }

    public BigDecimal getSlippagePercent() {
        return slippagePercent;
    }

    public BigDecimal getCommissionPercent() {
        return commissionPercent;
    }
}
