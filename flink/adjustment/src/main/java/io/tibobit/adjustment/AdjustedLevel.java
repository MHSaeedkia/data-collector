package io.tibobit.adjustment;

/**
 * One level on the way out, mirroring {@code AdjustedLevel} in
 * schemas/adjusted_order_book_event.avsc — field-for-field identical to the aggregated level it was
 * built from, because the adjustment stages move the price VALUE and add nothing to a level's shape.
 *
 * <p>A separate class from {@link AggregatedLevel} even so, because the two mirror two different
 * schemas and this job re-encodes: keeping "one model per schema" is what makes a dropped field a
 * failing test rather than a silent default on the wire.
 *
 * <p>It carries one field the schema does NOT have — {@link #getBasePrice()}, the price this level
 * arrived with. It exists so every stage can size its amount off the original price rather than the
 * running one, and it is deliberately not published.
 */
public class AdjustedLevel {

    private int exchangeId;
    private int simulation;
    private String sourceId = "";
    private String price;
    private String basePrice;
    private String quantity;

    public AdjustedLevel() {
    }

    /**
     * {@code price} is the price as it ARRIVED, so {@code basePrice} is seeded from it. Every stage
     * then moves {@code price} while {@code basePrice} stays put — which is what makes each stage's
     * amount a function of the original rather than of whatever the stage before it produced.
     */
    public AdjustedLevel(int exchangeId, int simulation, String sourceId, String price, String quantity) {
        this.exchangeId = exchangeId;
        this.simulation = simulation;
        this.sourceId = sourceId;
        this.price = price;
        this.basePrice = price;
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

    /**
     * The price this level arrived with, before any stage touched it. Every stage sizes its amount
     * from THIS, never from {@link #getPrice()} — see {@link Prices#applyPercent}.
     *
     * <p>Not on the wire: the published event carries the adjusted price and the three rates, and
     * the original is recoverable from those. It is a getter/setter pair rather than a final field
     * because Flink needs a POJO it can construct and populate when it ships a record between
     * operators.
     */
    public String getBasePrice() { return basePrice; }
    public void setBasePrice(String basePrice) { this.basePrice = basePrice; }

    public String getQuantity() { return quantity; }
    public void setQuantity(String quantity) { this.quantity = quantity; }

    @Override
    public String toString() {
        return "[" + exchangeId + " sim" + simulation + " " + price + " x " + quantity + "]";
    }
}
