package io.tibobit.orderbook.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One level of a {@link ConsolidatedOrderBook}. Unlike {@link PriceLevel}, it carries
 * its own {@code exchange_id}: because the merge unions levels rather than summing them,
 * each level must remember which exchange it came from. Price/quantity stay as decimal
 * strings for the same precision reason as PriceLevel.
 */
public class ConsolidatedLevel {

    @JsonProperty("exchange_id")
    private int exchangeId;
    private String price;
    private String quantity;

    public ConsolidatedLevel() {}

    public ConsolidatedLevel(int exchangeId, String price, String quantity) {
        this.exchangeId = exchangeId;
        this.price = price;
        this.quantity = quantity;
    }

    public int getExchangeId() { return exchangeId; }
    public void setExchangeId(int exchangeId) { this.exchangeId = exchangeId; }

    public String getPrice() { return price; }
    public void setPrice(String price) { this.price = price; }

    public String getQuantity() { return quantity; }
    public void setQuantity(String quantity) { this.quantity = quantity; }

    @Override
    public String toString() {
        return "[" + exchangeId + " " + price + " x " + quantity + "]";
    }
}
