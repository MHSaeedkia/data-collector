package io.tibobit.consolidator.model;

import java.util.List;

/**
 * Output event: the consolidated book for one pair+side, all exchanges merged into a
 * single price-sorted {@code levels} list. Emitted to the {side}-p{pair_id} topic and
 * consumed by the web UI. This wire shape is fixed — the web UI depends on it — so it is
 * copied unchanged from orderbook-job; do NOT alter it.
 */
public class ConsolidatedOrderBook {

    private int pairId;
    private String side;
    private List<ConsolidatedLevel> levels;

    // Max event_time across the contributing exchange books.
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
