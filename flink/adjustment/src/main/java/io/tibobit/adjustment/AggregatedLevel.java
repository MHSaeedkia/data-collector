package io.tibobit.adjustment;

/**
 * One level of job 6's aggregated book, subject {@code aggregated-order-book-event}. Carries its
 * own {@code exchangeId} because job 6 unions rather than sums — equal prices from different
 * exchanges arrive as separate, adjacent entries.
 *
 * <p>Every field of the schema's {@code AggregatedLevel} is modelled, and that is load-bearing
 * here in a way it is not for a job that only reads: this job writes the SAME record type back
 * out, so any field missing from this class would be silently replaced by its schema default on
 * the way to {@code p{id}-{side}-adjusted}. {@code losslessRoundTrip} is the guard.
 */
public class AggregatedLevel {

    private int exchangeId;
    private int simulation;
    private String sourceId = "";
    private String price;
    private String quantity;

    public AggregatedLevel() {
    }

    public AggregatedLevel(int exchangeId, int simulation, String sourceId, String price, String quantity) {
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
