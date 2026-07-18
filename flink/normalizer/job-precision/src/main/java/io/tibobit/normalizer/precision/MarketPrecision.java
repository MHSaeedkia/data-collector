package io.tibobit.normalizer.precision;

import java.io.Serializable;

/**
 * The decimal places a {@code markets} row allows for one pair. Both columns are nullable in the
 * schema and a null means "no precision configured" — the corresponding value is left untouched
 * rather than truncated to some guessed default.
 */
public class MarketPrecision implements Serializable {

    private final Integer price;
    private final Integer quantity;

    public MarketPrecision(Integer price, Integer quantity) {
        this.price = price;
        this.quantity = quantity;
    }

    public Integer getPrice() {
        return price;
    }

    public Integer getQuantity() {
        return quantity;
    }
}
