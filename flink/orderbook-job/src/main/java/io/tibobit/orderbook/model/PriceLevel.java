package io.tibobit.orderbook.model;

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
