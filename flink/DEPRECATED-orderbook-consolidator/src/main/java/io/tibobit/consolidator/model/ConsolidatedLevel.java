package io.tibobit.consolidator.model;

/**
 * One level of a {@link ConsolidatedOrderBook}. It carries its own {@code exchange_id}:
 * because the cross-exchange merge (R4) unions levels rather than summing them, each level
 * must remember which exchange it came from — equal prices from different exchanges stay as
 * separate, adjacent entries. price/quantity stay decimal strings for exact precision
 * (see memory/project_bigdecimal_rules.md).
 */
public class ConsolidatedLevel {

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
