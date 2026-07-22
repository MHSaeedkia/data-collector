package io.tibobit.normalizer.aggregate;

import java.util.List;

/**
 * Split-to-union record: one exchange's book for a single pair+side. {@link SnapshotSplitter}
 * produces two of these (asks, bids) from each job-5 {@code OrderBookSnapshot}, and
 * {@link CrossExchangeAggregator} (keyed {@code (pair_id, side)}) keeps the latest ExchangeBook
 * per {@code exchange_id} in {@code MapState<exchange_id, ExchangeBook>} and unions them.
 *
 * <p>{@link #levels} are already {@link AggregatedLevel}s (each stamped with this book's
 * {@code exchange_id}), so the union is a straight concat. {@link #eventTime} is the snapshot's
 * event_time. An empty {@code levels} list (job 5's reset ⇒ empty book) replaces the stored entry
 * and contributes nothing, so that exchange drops out of the aggregated book.
 *
 * <p>Plain POJO (no-arg ctor + getters/setters) so Flink can ship it between operators and store it
 * in keyed MapState.
 */
public class ExchangeBook {

    private int pairId;
    private int exchangeId;
    private String side;
    private List<AggregatedLevel> levels;
    private long eventTime;

    public ExchangeBook() {
    }

    public ExchangeBook(int pairId, int exchangeId, String side, List<AggregatedLevel> levels, long eventTime) {
        this.pairId = pairId;
        this.exchangeId = exchangeId;
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

    public int getExchangeId() {
        return exchangeId;
    }

    public void setExchangeId(int exchangeId) {
        this.exchangeId = exchangeId;
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
        return "ex" + exchangeId + " p" + pairId + " " + side
                + " (" + (levels == null ? 0 : levels.size()) + " levels)";
    }
}
