package io.tibobit.normalizer.model;

/**
 * One (price, quantity) rung of an order book side. price/quantity stay decimal strings for
 * exact precision (see memory/project_bigdecimal_rules.md) — jobs convert to BigDecimal at
 * processing time and re-canonicalize on write. Mirrors the PriceLevel record duplicated
 * identically across schemas/raw_order_book_event.avsc, order_book_snapshot.avsc and
 * rejected_order_book_event.avsc.
 *
 * <p>{@code sourceId} is the one field those three declarations do NOT share: only
 * order_book_snapshot.avsc carries it, because job 5 is the only step whose levels come from
 * different parents. Everywhere else a level belongs to the event carrying it, so the field stays
 * null and is never written — {@code PriceLevels} decides that from the schema, not from the value.
 */
public class PriceLevel {

    private String price;
    private String quantity;
    private String sourceId;

    public PriceLevel() {
    }

    public PriceLevel(String price, String quantity) {
        this.price = price;
        this.quantity = quantity;
    }

    public PriceLevel(String price, String quantity, String sourceId) {
        this.price = price;
        this.quantity = quantity;
        this.sourceId = sourceId;
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

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }
}
