package io.tibobit.merger;

import java.util.List;

/**
 * INPUT model: one job-7 aggregated record for a pair+side, read off {@code p{pair_id}-{side}}.
 * Levels arrive already sorted (asks ascending / bids descending, ties broken by quantity
 * descending), which is why equal-price levels are adjacent.
 *
 * <p>Only the fields this job actually consumes are modelled. That is the whole record today, but
 * the point stands if the aggregated schema grows: this is a reader, and it decodes what it needs.
 */
public class AggregatedOrderBook {

    private int pairId;
    private String side;
    private String id = "";
    private long maxEventTime;
    private Long minEventTime;
    private List<AggregatedLevel> levels;

    public AggregatedOrderBook() {
    }

    public AggregatedOrderBook(int pairId, String side, String id, long maxEventTime, Long minEventTime,
                               List<AggregatedLevel> levels) {
        this.pairId = pairId;
        this.side = side;
        this.id = id;
        this.maxEventTime = maxEventTime;
        this.minEventTime = minEventTime;
        this.levels = levels;
    }

    public int getPairId() { return pairId; }
    public void setPairId(int pairId) { this.pairId = pairId; }

    public String getSide() { return side; }
    public void setSide(String side) { this.side = side; }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public long getMaxEventTime() { return maxEventTime; }
    public void setMaxEventTime(long maxEventTime) { this.maxEventTime = maxEventTime; }

    public Long getMinEventTime() { return minEventTime; }
    public void setMinEventTime(Long minEventTime) { this.minEventTime = minEventTime; }

    public List<AggregatedLevel> getLevels() { return levels; }
    public void setLevels(List<AggregatedLevel> levels) { this.levels = levels; }

    @Override
    public String toString() {
        return "p" + pairId + " " + side + " (" + (levels == null ? 0 : levels.size()) + " levels) " + levels;
    }
}
