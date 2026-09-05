package io.tibobit.normalizer.aggregate;

/**
 * One level of a {@link AggregatedOrderBook}. It carries its own {@code exchange_id}: because the
 * cross-exchange merge unions levels rather than summing them, each level must remember which
 * exchange it came from — equal prices from different exchanges stay as separate, adjacent entries.
 * price/quantity stay decimal strings for exact precision (see memory/project_bigdecimal_rules.md).
 *
 * <p>{@code simulation} and {@code sourceId} ride along for the same reason {@code exchangeId} does:
 * one aggregated book mixes exchanges, so both are only meaningful per level, never for the record
 * as a whole. {@code simulation} is the flag of the exchange book this level came from (0 = live,
 * 1 = simulation); {@code sourceId} is that book's {@code id} — singular, because a level comes
 * from exactly one job-5 snapshot. This is why the aggregated record has a {@code id} but no
 * {@code source_ids}: a single parent list on the record would have to flatten away which exchange
 * each parent belongs to.
 */
public class AggregatedLevel {

    private int exchangeId;
    private int simulation;
    private String sourceId;
    private String price;
    private String quantity;

    public AggregatedLevel() {}

    public AggregatedLevel(int exchangeId, int simulation, String sourceId,
                           String price, String quantity) {
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
