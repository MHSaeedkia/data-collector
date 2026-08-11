package io.tibobit.merger;

/**
 * INPUT model: one level of job 6's aggregated book, decoded from subject
 * {@code aggregated-order-book-event}. Carries its own {@code exchangeId} because job 6 unions
 * rather than sums — equal prices from different exchanges arrive here as separate, adjacent
 * entries. Turning exactly that into one summed entry is this job's whole purpose.
 *
 * <p>{@code sourceId} is the id of the job-5 snapshot the level came from; it is what ends up in
 * the merged level's {@code source_ids}.
 */
public class AggregatedLevel {

    private int exchangeId;
    private int simulation;
    private String sourceId;
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
