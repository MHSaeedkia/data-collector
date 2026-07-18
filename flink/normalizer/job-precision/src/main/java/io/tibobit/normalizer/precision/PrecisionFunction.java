package io.tibobit.normalizer.precision;

import io.tibobit.normalizer.decimal.Decimals;
import io.tibobit.normalizer.lookup.RefreshingLookup;
import io.tibobit.normalizer.model.PriceLevel;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Job 4 — precision. Stateless: every level's price is truncated to
 * {@code markets.price_precision} decimal places and every quantity to
 * {@code markets.quantity_precision}, always rounding DOWN (never up — an order book must not
 * claim more size or a better price than the exchange reported). Precisions come per pair from a
 * {@link RefreshingLookup} over markets.
 *
 * <p>A null precision column means "not configured" and leaves that value untouched. A pair with
 * no markets row at all is treated the same way — passthrough, not dead-letter. This is
 * deliberately unlike job 3: an un-rebased amount is silently corrupt (orders of magnitude off),
 * whereas an un-truncated one is merely more precise than we asked for, so there is nothing to
 * quarantine.
 *
 * <p><b>Truncate-to-zero (design flag, user decision 2026-07-18, revised same day):</b> a nonzero
 * quantity that truncates to exactly 0 is emitted as "0" — no level is ever dropped here. Job 5
 * reads {@code quantity == 0} as "delete this level", which is the intended consequence: a size
 * below the market's lot precision is not representable liquidity, so the book should not hold a
 * level for it. This makes the function a pure per-level transform — level count in equals level
 * count out.
 *
 * <p>A null side is NOT an empty side: ex3 wallex sends one side per message and the other stays
 * null, so null in ⇒ null out (see [[rebaser]]).
 */
public class PrecisionFunction extends RichMapFunction<RawOrderBookEvent, RawOrderBookEvent> {

    private final RefreshingLookup<Integer, MarketPrecision> precisions;

    public PrecisionFunction(RefreshingLookup<Integer, MarketPrecision> precisions) {
        this.precisions = precisions;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        precisions.open();
    }

    @Override
    public RawOrderBookEvent map(RawOrderBookEvent event) {
        event.getPipelineTimings().setPrecisionIn(System.currentTimeMillis());

        MarketPrecision precision = precisions.get(event.getPairId());
        if (precision == null) {
            precision = UNCONFIGURED;
        }
        event.setAsks(applyLevels(event.getAsks(), precision));
        event.setBids(applyLevels(event.getBids(), precision));

        event.getPipelineTimings().setPrecisionOut(System.currentTimeMillis());
        return event;
    }

    @Override
    public void close() {
        precisions.close();
    }

    /** Stand-in for a pair with no markets row: both precisions null ⇒ everything passes through. */
    private static final MarketPrecision UNCONFIGURED = new MarketPrecision(null, null);

    /** Null side (ex3's absent half) passes through as null; an empty list stays empty. */
    private static List<PriceLevel> applyLevels(List<PriceLevel> levels, MarketPrecision precision) {
        if (levels == null) {
            return null;
        }
        List<PriceLevel> applied = new ArrayList<>(levels.size());
        for (PriceLevel level : levels) {
            applied.add(new PriceLevel(
                    Decimals.canonicalize(apply(new BigDecimal(level.getPrice()), precision.getPrice())),
                    Decimals.canonicalize(apply(new BigDecimal(level.getQuantity()), precision.getQuantity()))));
        }
        return applied;
    }

    private static BigDecimal apply(BigDecimal value, Integer precision) {
        return precision == null ? value : Decimals.truncate(value, precision);
    }
}
