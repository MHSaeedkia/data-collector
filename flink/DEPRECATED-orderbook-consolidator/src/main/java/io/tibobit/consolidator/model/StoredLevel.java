package io.tibobit.consolidator.model;

/**
 * Stage-1 keyed-state value stored in {@code MapState<price, StoredLevel>} (operator keyed by
 * {@code (pair_id, exchange_id, side)}). The price is the MapState key and pair/exchange/side
 * are the operator key, so a level only needs to remember its latest {@code quantity} and the
 * {@code eventTime} that set it. quantity stays a decimal string for exact precision
 * (see memory/project_bigdecimal_rules.md). eventTime drives R1 upsert-latest (a newer
 * event_time wins over an older one for the same price) and feeds the per-exchange book's
 * event_time (max across its levels).
 *
 * Plain POJO (no-arg ctor + getters/setters) so Flink can store it in MapState.
 */
public class StoredLevel {

    private String quantity;
    private long eventTime;

    public StoredLevel() {
    }

    public StoredLevel(String quantity, long eventTime) {
        this.quantity = quantity;
        this.eventTime = eventTime;
    }

    public String getQuantity() {
        return quantity;
    }

    public void setQuantity(String quantity) {
        this.quantity = quantity;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }
}
