package io.tibobit.normalizer.model;

/**
 * One (price, quantity) rung of an order book side. price/quantity stay decimal strings for
 * exact precision (see memory/project_bigdecimal_rules.md) — jobs convert to BigDecimal at
 * processing time and re-canonicalize on write. Mirrors the PriceLevel record duplicated
 * identically across schemas/raw_order_book_event.avsc, order_book_snapshot.avsc and
 * rejected_order_book_event.avsc.
 */
public class PriceLevel {

    private String price;
    private String quantity;

    public PriceLevel() {
    }

    public PriceLevel(String price, String quantity) {
        this.price = price;
        this.quantity = quantity;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    public String getQuantity() {
        return quantity;
    }

    public void setQuantity(String quantity) {
        this.quantity = quantity;
    }
}
