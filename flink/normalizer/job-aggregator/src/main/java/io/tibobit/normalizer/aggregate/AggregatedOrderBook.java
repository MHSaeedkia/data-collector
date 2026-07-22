package io.tibobit.normalizer.aggregate;

import java.util.List;

/**
 * Output event: the aggregated book for one pair+side, all exchanges merged into a single
 * price-sorted {@code levels} list. Emitted to the {@code p{pair_id}-{side}} topic (subject
 * {@code aggregated-order-book-event}) and consumed by the web UI. This wire shape is fixed — the
 * web UI depends on it — so it is copied unchanged from the deprecated orderbook-consolidator; do
 * NOT alter it.
 */
public class AggregatedOrderBook {

    private int pairId;
    private String side;
    private List<AggregatedLevel> levels;

    // Max event_time across the contributing exchange books.
    private long eventTime;

    public AggregatedOrderBook() {
    }

    public AggregatedOrderBook(int pairId, String side, List<AggregatedLevel> levels, long eventTime) {
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

    public List<AggregatedLevel> getLevels() {
        return levels;
    }

    public void setLevels(List<AggregatedLevel> levels) {
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
