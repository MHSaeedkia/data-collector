package io.tibobit.orderbook.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class ConsolidatedOrderBook {

    private String base;
    private String quote;
    private String side;
    private List<ConsolidatedLevel> levels;

    // Max event_time across the contributing exchange snapshots.
    @JsonProperty("event_time")
    private long eventTime;

    public ConsolidatedOrderBook() {
    }

    public ConsolidatedOrderBook(String base, String quote, String side, List<ConsolidatedLevel> levels,
            long eventTime) {
        this.base = base;
        this.quote = quote;
        this.side = side;
        this.levels = levels;
        this.eventTime = eventTime;
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

    public List<ConsolidatedLevel> getLevels() {
        return levels;
    }

    public void setLevels(List<ConsolidatedLevel> levels) {
        this.levels = levels;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return base + "/" + quote + " " + side + " (" + (levels == null ? 0 : levels.size()) + " levels) " + levels;
    }
}
