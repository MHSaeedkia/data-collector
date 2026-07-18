package io.tibobit.normalizer.rebase;

import java.io.Serializable;

/**
 * The two powers-of-ten exponents an {@code exchange_markets} row carries for one
 * (exchange, pair): how far to shift a price and how far to shift a quantity.
 */
public class RebaseFactors implements Serializable {

    private final int price;
    private final int volume;

    public RebaseFactors(int price, int volume) {
        this.price = price;
        this.volume = volume;
    }

    public int getPrice() {
        return price;
    }

    public int getVolume() {
        return volume;
    }
}
