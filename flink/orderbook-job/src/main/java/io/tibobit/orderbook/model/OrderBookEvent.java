package io.tibobit.orderbook.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class OrderBookEvent {

    @JsonProperty("exchange_id")
    private int exchangeId;

    @JsonProperty("exchange_name")
    private String exchangeName;
    private String base;
    private String quote;
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

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchange(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public String getBase() {
        return base;
    }

    public void setBase(String base) {
        this.base = base;
    }

    public String getQuote() {
        return quote;
    }

    public void setQuote(String quote) {
        this.quote = quote;
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

    public OrderBookEventType getType() {
        return type;
    }

    public void setType(OrderBookEventType type) {
        this.type = type;
    }

    public List<PriceLevel> getLevels() {
        return levels;
    }

    public void setLevels(List<PriceLevel> levels) {
        this.levels = levels;
    }
}
