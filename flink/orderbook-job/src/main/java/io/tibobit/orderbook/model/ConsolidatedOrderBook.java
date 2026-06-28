package io.tibobit.orderbook.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class ConsolidatedOrderBook {

    @JsonProperty("pair_id")
    private int pairId;
    private String side;
    private List<ConsolidatedLevel> levels;

    // Max event_time across the contributing exchange snapshots.
    @JsonProperty("event_time")
    private long eventTime;

    public ConsolidatedOrderBook() {
    }

    public ConsolidatedOrderBook(int pairId, String side, List<ConsolidatedLevel> levels, long eventTime) {
        this.pairId = pairId;
        this.side = side;
        this.levels = levels;
        this.eventTime = eventTime;
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
        return "p" + pairId + " " + side + " (" + (levels == null ? 0 : levels.size()) + " levels) " + levels;
    }
}
