package io.tibobit.merger;

import java.util.List;

/**
 * OUTPUT model: one price across all exchanges, quantity summed. There is no single
 * {@code exchangeId} here — that is precisely the field the merge destroys — so the contributing
 * exchanges are kept as a list, positionally aligned with {@code sourceIds}: {@code exchangeIds[i]}
 * contributed the level whose id is {@code sourceIds[i]}.
 *
 * <p>{@code simulation} survives as a scalar because it is part of the grouping key, not something
 * merged across: a live level and a simulated level at the same price stay two separate
 * MergedLevels. Summing them would report simulated depth as real.
 */
public class MergedLevel {

    private int simulation;
    private List<Integer> exchangeIds;
    private List<String> sourceIds;
    private String price;
    private String quantity;

    public MergedLevel() {
    }

    public MergedLevel(int simulation, List<Integer> exchangeIds, List<String> sourceIds,
                       String price, String quantity) {
        this.simulation = simulation;
        this.exchangeIds = exchangeIds;
        this.sourceIds = sourceIds;
        this.price = price;
        this.quantity = quantity;
    }

    public int getSimulation() { return simulation; }
    public void setSimulation(int simulation) { this.simulation = simulation; }

    public List<Integer> getExchangeIds() { return exchangeIds; }
    public void setExchangeIds(List<Integer> exchangeIds) { this.exchangeIds = exchangeIds; }

    public List<String> getSourceIds() { return sourceIds; }
    public void setSourceIds(List<String> sourceIds) { this.sourceIds = sourceIds; }

    public String getPrice() { return price; }
    public void setPrice(String price) { this.price = price; }

    public String getQuantity() { return quantity; }
    public void setQuantity(String quantity) { this.quantity = quantity; }

    @Override
    public String toString() {
        return "[" + exchangeIds + " sim" + simulation + " " + price + " x " + quantity + "]";
    }
}
