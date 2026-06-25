package io.tibobit.orderbook.model;

public class ConsolidatedLevel {

    private String exchange;
    private String price;
    private String quantity;

    public ConsolidatedLevel() {}

    public ConsolidatedLevel(String exchange, String price, String quantity) {
        this.exchange = exchange;
        this.price = price;
        this.quantity = quantity;
    }

    public String getExchange() { return exchange; }
    public void setExchange(String exchange) { this.exchange = exchange; }

    public String getPrice() { return price; }
    public void setPrice(String price) { this.price = price; }

    public String getQuantity() { return quantity; }
    public void setQuantity(String quantity) { this.quantity = quantity; }

    @Override
    public String toString() {
        return "[" + exchange + " " + price + " x " + quantity + "]";
    }
}
