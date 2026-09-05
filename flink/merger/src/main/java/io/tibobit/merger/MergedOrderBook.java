package io.tibobit.merger;

import java.util.List;

/**
 * OUTPUT model: the price-merged book for one pair+side, published to
 * {@code p{pair_id}-{side}-merged} (subject {@code merged-order-book-event}).
 *
 * <p>Unlike job 6's aggregated record, this one has a record-level {@code sourceId}: the merger
 * consumes exactly one aggregated record per output record, so there is a single unambiguous
 * parent. Job 6 could not say that — its record fans in from every exchange at once, which is why
 * its lineage lives per level.
 */
public class MergedOrderBook {

    private int pairId;
    private String side;
    private String id = "";
    private String sourceId = "";
    private long eventTime;
    private List<MergedLevel> levels;

    public MergedOrderBook() {
    }

    public MergedOrderBook(int pairId, String side, String id, String sourceId,
                           long eventTime, List<MergedLevel> levels) {
        this.pairId = pairId;
        this.side = side;
        this.id = id;
        this.sourceId = sourceId;
        this.eventTime = eventTime;
        this.levels = levels;
    }

    public int getPairId() { return pairId; }
    public void setPairId(int pairId) { this.pairId = pairId; }

    public String getSide() { return side; }
    public void setSide(String side) { this.side = side; }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getSourceId() { return sourceId; }
    public void setSourceId(String sourceId) { this.sourceId = sourceId; }

    public long getEventTime() { return eventTime; }
    public void setEventTime(long eventTime) { this.eventTime = eventTime; }

    public List<MergedLevel> getLevels() { return levels; }
    public void setLevels(List<MergedLevel> levels) { this.levels = levels; }

    @Override
    public String toString() {
        return "p" + pairId + " " + side + "-merged (" + (levels == null ? 0 : levels.size()) + " levels) " + levels;
    }
}
