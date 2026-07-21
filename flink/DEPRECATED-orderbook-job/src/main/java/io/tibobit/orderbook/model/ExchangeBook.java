package io.tibobit.orderbook.model;

import java.math.BigDecimal;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * The maintained order book for a single exchange (one pair+side), held in Flink keyed
 * state by OrderBookMerger. Unlike a raw {@link OrderBookEvent}, this is a *running* book
 * the merger mutates as events arrive:
 *   - a {@code snapshot} event replaces {@link #levels} wholesale;
 *   - an {@code update} event upserts (quantity &gt; 0) or deletes (quantity == 0)
 *     individual price levels.
 * {@link #lastSeq} is the highest {@code sequence_id} applied so far; the merger uses it
 * (together with the event's {@code sequence_jump}) to drop stale/duplicate events and to
 * detect gaps. {@link #awaitingSnapshot} is set when a gap (missed messages) is detected:
 * the book is cleared and {@code update}s are ignored until the next {@code snapshot}
 * resyncs it.
 *
 * Plain POJO (no-arg ctor + getters/setters) so Flink can store it in MapState.
 */
public class ExchangeBook {

    // price (BigDecimal) -> quantity string (decimal string, see PriceLevel). MUST stay a
    // TreeMap: it keys by numeric value via BigDecimal.compareTo, so "97240.50" and "97240.5"
    // collapse to one level. NiFi gives no guarantee that snapshot and update use the same
    // string formatting for a price, so we can't key by the raw string. (A HashMap would be
    // wrong here — BigDecimal.equals/hashCode are scale-sensitive.)
    private NavigableMap<BigDecimal, String> levels = new TreeMap<>();
    private long eventTime;
    private long lastSeq;
    private boolean awaitingSnapshot;

    public ExchangeBook() {
    }

    public NavigableMap<BigDecimal, String> getLevels() {
        return levels;
    }

    public void setLevels(NavigableMap<BigDecimal, String> levels) {
        this.levels = levels;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public long getLastSeq() {
        return lastSeq;
    }

    public void setLastSeq(long lastSeq) {
        this.lastSeq = lastSeq;
    }

    public boolean isAwaitingSnapshot() {
        return awaitingSnapshot;
    }

    public void setAwaitingSnapshot(boolean awaitingSnapshot) {
        this.awaitingSnapshot = awaitingSnapshot;
    }
}
