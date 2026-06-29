package io.tibobit.orderbook.model;

/**
 * One (price, quantity) rung within an exchange's {@link OrderBookEvent}.
 * Both are kept as decimal strings (parsed to BigDecimal only when comparing)
 * to preserve exact precision and avoid binary float rounding error.
 */
public class PriceLevel {

    private String price;
    private String quantity;

    public PriceLevel() {}

    public PriceLevel(String price, String quantity) {
        this.price = price;
        this.quantity = quantity;
    }

    public String getPrice() { return price; }
    public void setPrice(String price) { this.price = price; }

    public String getQuantity() { return quantity; }
    public void setQuantity(String quantity) { this.quantity = quantity; }
}
