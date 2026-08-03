package io.tibobit.normalizer.aggregate;

/**
 * One level of a {@link AggregatedOrderBook}. It carries its own {@code exchange_id}: because the
 * cross-exchange merge unions levels rather than summing them, each level must remember which
 * exchange it came from — equal prices from different exchanges stay as separate, adjacent entries.
 * price/quantity stay decimal strings for exact precision (see memory/project_bigdecimal_rules.md).
 *
 * <p>{@code simulation} rides along for the same reason {@code exchangeId} does: one aggregated book
 * mixes exchanges, so the flag is only meaningful per level, never for the record as a whole. It is
 * the flag of the exchange book this level came from (0 = live, 1 = simulation).
 */
public class AggregatedLevel {

    private int exchangeId;
    private int simulation;
    private String price;
    private String quantity;

    public AggregatedLevel() {}

    public AggregatedLevel(int exchangeId, int simulation, String price, String quantity) {
        this.exchangeId = exchangeId;
        this.simulation = simulation;
        this.price = price;
        this.quantity = quantity;
    }

    public int getExchangeId() { return exchangeId; }
    public void setExchangeId(int exchangeId) { this.exchangeId = exchangeId; }

    public int getSimulation() { return simulation; }
    public void setSimulation(int simulation) { this.simulation = simulation; }

    public String getPrice() { return price; }
    public void setPrice(String price) { this.price = price; }

    public String getQuantity() { return quantity; }
    public void setQuantity(String quantity) { this.quantity = quantity; }

    @Override
    public String toString() {
        return "[" + exchangeId + " sim" + simulation + " " + price + " x " + quantity + "]";
    }
}
