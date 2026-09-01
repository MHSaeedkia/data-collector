package io.tibobit.adjustment;

import java.util.List;

/**
 * One job-6 aggregated record for a pair+side, read off {@code p{pair_id}-{side}} and — as of
 * step 1 — written straight back out to {@code p{pair_id}-{side}-adjusted} unchanged.
 *
 * <p>Levels arrive already sorted (asks ascending / bids descending, ties broken by quantity
 * descending), which is why equal-price levels are adjacent.
 *
 * <p>Unlike the merger's reader-only model, this class mirrors the schema in FULL on purpose: the
 * job re-encodes this same record type, so a field left out here would not merely go unread, it
 * would be dropped from the output and replaced by the schema default. Keep it field-for-field with
 * schemas/aggregated_order_book_event.avsc.
 */
public class AggregatedOrderBook {

    private int pairId;
    private String side;
    private String id = "";
    private long eventTime;
    private List<AggregatedLevel> levels;

    public AggregatedOrderBook() {
    }

    public AggregatedOrderBook(int pairId, String side, String id, long eventTime, List<AggregatedLevel> levels) {
        this.pairId = pairId;
        this.side = side;
        this.id = id;
        this.eventTime = eventTime;
        this.levels = levels;
    }

    public int getPairId() { return pairId; }
    public void setPairId(int pairId) { this.pairId = pairId; }

    public String getSide() { return side; }
    public void setSide(String side) { this.side = side; }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public long getEventTime() { return eventTime; }
    public void setEventTime(long eventTime) { this.eventTime = eventTime; }

    public List<AggregatedLevel> getLevels() { return levels; }
    public void setLevels(List<AggregatedLevel> levels) { this.levels = levels; }

    @Override
    public String toString() {
        return "p" + pairId + " " + side + " (" + (levels == null ? 0 : levels.size()) + " levels) " + levels;
    }
}
