package io.tibobit.orderbook.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class OrderBookEvent {

    private String exchange;
    private String pair;
    private String side;

    @JsonProperty("event_time")
    private long eventTime;

    private List<PriceLevel> levels;

    public OrderBookEvent() {}

    public String getExchange() { return exchange; }
    public void setExchange(String exchange) { this.exchange = exchange; }

    public String getPair() { return pair; }
    public void setPair(String pair) { this.pair = pair; }

    public String getSide() { return side; }
    public void setSide(String side) { this.side = side; }

    public long getEventTime() { return eventTime; }
    public void setEventTime(long eventTime) { this.eventTime = eventTime; }

    public List<PriceLevel> getLevels() { return levels; }
    public void setLevels(List<PriceLevel> levels) { this.levels = levels; }
}
