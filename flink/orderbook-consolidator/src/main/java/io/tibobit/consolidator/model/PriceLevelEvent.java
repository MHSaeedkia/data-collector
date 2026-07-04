package io.tibobit.consolidator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Input event: a single price level for one pair+side from one exchange, read from the
 * {side}-p{pair_id}-ex{exchange_id} Kafka topics (schema schemas/price_level_event.avsc).
 * Unlike the old orderbook-job OrderBookEvent this carries exactly one (price, quantity)
 * rung — there is no {@code type} / {@code sequence_id} / {@code sequence_jump} / {@code levels[]};
 * every message is the latest state of one level, so R1 upserts by {@code event_time} and R2
 * removes when {@code quantity == 0}. price/quantity stay decimal strings for exact precision
 * (see memory/project_bigdecimal_rules.md).
 *
 * The wire schema omits display-only exchange_name/base/quote; ignoreUnknown keeps
 * deserialization tolerant if any ever appear.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PriceLevelEvent {

    @JsonProperty("exchange_id")
    private int exchangeId;

    @JsonProperty("pair_id")
    private int pairId;

    private String side;

    @JsonProperty("event_time")
    private long eventTime;

    private String price;
    private String quantity;

    public PriceLevelEvent() {
    }

    public PriceLevelEvent(int exchangeId, int pairId, String side, long eventTime, String price, String quantity) {
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
}
