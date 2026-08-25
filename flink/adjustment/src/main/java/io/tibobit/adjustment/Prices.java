package io.tibobit.adjustment;

import java.math.BigDecimal;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * The one place a percentage is turned into a price move, shared by all three
 * adjustment stages so the direction rule is stated once rather than three
 * times.
 */
final class Prices {

    private Prices() {
    }

    /**
     * ⚠ <b>The sign convention, and the single most important line in this
     * job.</b>
     *
     * <p>
     * {@code asks} is what a user BUYS at, so every charge moves that price UP;
     * {@code bids} is what a user SELLS at, so every charge moves it DOWN. Both
     * directions take money from the same side of the trade — inverting either
     * one would publish a book you could buy from and sell back into at a
     * profit.
     *
     * <p>
     * This is the standard convention and it is what the stages were written
     * against, but it was never explicitly confirmed. If it is wrong, it is
     * wrong HERE and nowhere else.
     */
    private static BigDecimal applySigned(String side, BigDecimal price, BigDecimal amount) {
        return "asks".equals(side) ? price.add(amount) : price.subtract(amount);
    }

    /**
     * Applies one stage's percentage to every level, in place — looked up PER
     * LEVEL rather than shared by the whole record, since all three rates
     * (commission, profit, slippage) are keyed by
     * {@code (exchange_id, market_id)} and one book is job 6's UNION across
     * exchanges. Two levels in the same book can get two different rates from
     * two different exchanges.
     *
     * <p>
     * <b>The amount is sized off the price the level ARRIVED with, then added
     * to the running price</b> (user, 2026-08-24, correcting the first
     * implementation). So three stages of ~0.35 / ~0.1 / ~1 percent move an ask
     * by roughly base x 0.0145 in total — they SUM rather than compound,
     * because none of them ever sees another's output as its input.
     *
     * <p>
     * One consequence worth knowing: <b>the chain order no longer changes the
     * result.</b> The stages still run commission → profit → slippage, but
     * addition commutes, so reordering them would produce identical prices. The
     * order is now presentation, not arithmetic — which it was NOT under the
     * compounding version.
     *
     * <p>
     * Quantities are untouched: these stages move what a unit costs, not how
     * many units are there. Prices are canonicalized on the way out so exact
     * arithmetic's growing scale does not accumulate as trailing zeros down the
     * chain.
     *
     * <p>
     * Nothing is rounded to the market's tick size. Job 4 already applied
     * {@code markets.price_precision} upstream and this pushes past it — but
     * re-truncating needs the per-market precision this job does not read, and
     * picking a rounding direction is a decision with money in it. Left exact
     * and flagged rather than guessed (memory/project_adjustment.md).
     *
     * <p>
     * {@code rateForExchangeAndPair} takes {@code (pairId, exchangeId)} and
     * returns the percent to apply to that one level — the caller decides what
     * a missing lookup row means (a fallback, in every current caller).
     * {@code rateSetter} writes the percent this level actually got back onto
     * the level, so the published event always says what was charged.
     */
    static void applyPerLevelPercent(AdjustedOrderBook book,
            BiFunction<Integer, Integer, BigDecimal> rateForExchangeAndPair,
            BiConsumer<AdjustedLevel, String> rateSetter) {
        List<AdjustedLevel> levels = book.getLevels();
        if (levels == null || levels.isEmpty()) {
            return;
        }
        for (AdjustedLevel level : levels) {
            BigDecimal percent = rateForExchangeAndPair.apply(book.getPairId(), level.getExchangeId());
            BigDecimal fraction = percent.movePointLeft(2);
            BigDecimal amount = new BigDecimal(level.getBasePrice()).multiply(fraction);
            BigDecimal adjusted = applySigned(book.getSide(), new BigDecimal(level.getPrice()), amount);
            level.setPrice(Decimals.canonicalize(adjusted));
            rateSetter.accept(level, percent.toPlainString());
        }
    }
}
