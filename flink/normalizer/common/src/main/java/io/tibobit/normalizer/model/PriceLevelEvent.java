package io.tibobit.normalizer.model;

/**
 * Job-6 output: one price level of one side changed (schema schemas/price_level_event.avsc,
 * subject price-level-event). {@code quantity} "0" means the level is gone — that is the
 * consolidator's removal signal, not an actual resting size of zero.
 *
 * <p>This is the ONE model in the pipeline whose shape we do not own: the consolidator has been
 * consuming this subject since before the normalizer existed, so the wire format is frozen and
 * {@code side} is the string "asks"/"bids" it expects (an Avro enum on the wire).
 */
public class PriceLevelEvent {

    private int exchangeId;
    private int pairId;
    private String side;
    private long eventTime;
    private String price;
    private String quantity;
    private PipelineTimings pipelineTimings = new PipelineTimings();

    public PriceLevelEvent() {
    }

    public PriceLevelEvent(int exchangeId, int pairId, String side, long eventTime,
                           String price, String quantity) {
        this.exchangeId = exchangeId;
        this.pairId = pairId;
        this.side = side;
        this.eventTime = eventTime;
        this.price = price;
        this.quantity = quantity;
    }

    public int getExchangeId() {
        return exchangeId;
    }

    public void setExchangeId(int exchangeId) {
        this.exchangeId = exchangeId;
    }

    public int getPairId() {
        return pairId;
    }

    public void setPairId(int pairId) {
        this.pairId = pairId;
    }

    public String getSide() {
        return side;
    }

    public void setSide(String side) {
        this.side = side;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
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

    public PipelineTimings getPipelineTimings() {
        return pipelineTimings;
    }

    public void setPipelineTimings(PipelineTimings pipelineTimings) {
        this.pipelineTimings = pipelineTimings;
    }
}
