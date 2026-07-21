package io.tibobit.consolidator.model;

import java.util.List;

/**
 * Stage-1 &rarr; stage-2 record: one exchange's maintained book for a pair+side, emitted by the
 * stage-1 operator (keyed {@code (pair_id, exchange_id, side)}) after every upsert (R1) or
 * remove (R2). Stage-2 (re-keyed {@code (pair_id, side)}) keeps the latest ExchangeBook per
 * {@code exchange_id} in {@code MapState<exchange_id, ExchangeBook>} and unions them (R4).
 *
 * {@link #levels} are already {@link ConsolidatedLevel}s (each stamped with this book's
 * {@code exchange_id} by stage-1), so the R4 union is a straight concat — no rung type of its
 * own is needed. {@link #eventTime} is the max event_time across this exchange's levels.
 *
 * Distinct from the old orderbook-job ExchangeBook (that one held a NavigableMap + sequence
 * bookkeeping); this is a flat inter-operator record. Plain POJO (no-arg ctor + getters/setters)
 * so Flink can ship it between operators and store it in stage-2 MapState.
 */
public class ExchangeBook {

    private int pairId;
    private int exchangeId;
    private String side;
    private List<ConsolidatedLevel> levels;
    private long eventTime;

    public ExchangeBook() {
    }

    public ExchangeBook(int pairId, int exchangeId, String side, List<ConsolidatedLevel> levels, long eventTime) {
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
        return "ex" + exchangeId + " p" + pairId + " " + side
                + " (" + (levels == null ? 0 : levels.size()) + " levels)";
    }
}
