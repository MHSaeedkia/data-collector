package io.tibobit.adjustment;

import java.math.BigDecimal;
import java.util.List;

/**
 * The one place a percentage is turned into a price move, shared by all three adjustment stages so
 * the direction rule is stated once rather than three times.
 */
final class Prices {

    private Prices() {
    }

    /**
     * ⚠ <b>The sign convention, and the single most important line in this job.</b>
     *
     * <p>{@code asks} is what a user BUYS at, so every charge moves that price UP; {@code bids} is
     * what a user SELLS at, so every charge moves it DOWN. Both directions take money from the same
     * side of the trade — inverting either one would publish a book you could buy from and sell
     * back into at a profit.
     *
     * <p>This is the standard convention and it is what the stages were written against, but it was
     * never explicitly confirmed. If it is wrong, it is wrong HERE and nowhere else.
     */
    static BigDecimal multiplier(String side, BigDecimal percent) {
        // movePointLeft, not divide(100): a scale shift is exact and cannot throw, whereas divide
        // needs an explicit rounding mode to be safe (memory/project_bigdecimal_rules.md).
        BigDecimal fraction = percent.movePointLeft(2);
        return "asks".equals(side)
                ? BigDecimal.ONE.add(fraction)
                : BigDecimal.ONE.subtract(fraction);
    }

    /**
     * Applies one stage's percentage to every level's price, in place.
     *
     * <p>Quantities are untouched: these stages move what a unit costs, not how many units are
     * there. Prices are canonicalized on the way out, so the growing scale that exact multiplication
     * produces ({@code 62650.00} x 1.0035 = {@code 62869.275000}) does not accumulate down the
     * chain as trailing zeros.
     *
     * <p>Nothing is rounded to the market's tick size. Job 4 already applied
     * {@code markets.price_precision} upstream, and multiplying re-introduces decimals past it —
     * but re-truncating needs the per-market precision this job does not read, and picking a
     * rounding direction is a decision with money in it. Left exact and flagged rather than guessed
     * (memory/project_adjustment.md).
     */
    static void applyPercent(AdjustedOrderBook book, BigDecimal percent) {
        List<AdjustedLevel> levels = book.getLevels();
        if (levels == null || levels.isEmpty()) {
            return;
        }
        BigDecimal multiplier = multiplier(book.getSide(), percent);
        for (AdjustedLevel level : levels) {
            BigDecimal adjusted = new BigDecimal(level.getPrice()).multiply(multiplier);
            level.setPrice(Decimals.canonicalize(adjusted));
        }
    }
}
