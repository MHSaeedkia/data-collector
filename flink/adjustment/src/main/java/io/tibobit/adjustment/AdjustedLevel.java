package io.tibobit.adjustment;

/**
 * One level on the way out, mirroring {@code AdjustedLevel} in
 * schemas/adjusted_order_book_event.avsc — field-for-field identical to the aggregated level it was
 * built from, because the adjustment stages move the price VALUE and add nothing to a level's shape.
 *
 * <p>A separate class from {@link AggregatedLevel} even so, because the two mirror two different
 * schemas and this job re-encodes: keeping "one model per schema" is what makes a dropped field a
 * failing test rather than a silent default on the wire.
 */
public class AdjustedLevel {

    private int exchangeId;
    private int simulation;
    private String sourceId = "";
    private String price;
    private String quantity;

    public AdjustedLevel() {
    }

    public AdjustedLevel(int exchangeId, int simulation, String sourceId, String price, String quantity) {
        this.exchangeId = exchangeId;
        this.simulation = simulation;
        this.sourceId = sourceId;
        this.price = price;
        this.quantity = quantity;
    }

    public int getExchangeId() { return exchangeId; }
    public void setExchangeId(int exchangeId) { this.exchangeId = exchangeId; }

    public int getSimulation() { return simulation; }
    public void setSimulation(int simulation) { this.simulation = simulation; }

    public String getSourceId() { return sourceId; }
    public void setSourceId(String sourceId) { this.sourceId = sourceId; }

    public String getPrice() { return price; }
    public void setPrice(String price) { this.price = price; }

    public String getQuantity() { return quantity; }
    public void setQuantity(String quantity) { this.quantity = quantity; }

    @Override
    public String toString() {
        return "[" + exchangeId + " sim" + simulation + " " + price + " x " + quantity + "]";
    }
}
