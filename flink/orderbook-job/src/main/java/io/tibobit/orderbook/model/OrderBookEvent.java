package io.tibobit.orderbook.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Input event: one exchange's order book snapshot for a single pair+side, as read
 * from the {side}-p{pair_id}-ex{exchange_id} Kafka topics. The merger stores the
 * latest of these per exchange (see OrderBookMerger).
 *
 * The wire event (see schemas/orderbook_event.avsc) also carries exchange_name, base and
 * quote; Flink works only with the IDs, so those extra fields are ignored on deserialization.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderBookEvent {

    @JsonProperty("exchange_id")
    private int exchangeId;

    @JsonProperty("pair_id")
    private int pairId;

    private String side;
    private OrderBookEventType type;

    @JsonProperty("event_time")
    private long eventTime;

    private List<PriceLevel> levels;

    public OrderBookEvent() {
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

    public OrderBookEventType getType() {
        return type;
    }

    public void setType(OrderBookEventType type) {
        this.type = type;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public List<PriceLevel> getLevels() {
        return levels;
    }

    public void setLevels(List<PriceLevel> levels) {
        this.levels = levels;
    }
}
