package io.tibobit.normalizer.model;

import java.util.List;

/**
 * The ONE shared event on all job 1–4 topics (schema schemas/raw_order_book_event.avsc,
 * subject raw-order-book-event). Semantics pinned in memory/project_avro_schema.md:
 *
 * <ul>
 *   <li>{@code asks}/{@code bids} nullable: null = this side is not part of this event (ex3
 *       wallex per-SIDE snapshots); an EMPTY list = the exchange reported the side empty.
 *       Never conflate the two.</li>
 *   <li>{@code sequenceId} nullable: null = the feed has no ordering field at all (ex3 only) —
 *       the type validator passes such events through unchecked.</li>
 *   <li>{@code sequenceJump}: &gt;0 = delta feed, gap rule {@code seq == last + jump}
 *       (ex6=1, ex8=300); 0 = snapshot feed — out-of-order check only.</li>
 * </ul>
 */
public class RawOrderBookEvent {

    private int exchangeId;
    private int pairId;
    private String type;
    private Long sequenceId;
    private long sequenceJump;
    private long eventTime;
    private List<PriceLevel> asks;
    private List<PriceLevel> bids;

    public RawOrderBookEvent() {
    }

    public RawOrderBookEvent(int exchangeId, int pairId, String type, Long sequenceId,
                             long sequenceJump, long eventTime,
                             List<PriceLevel> asks, List<PriceLevel> bids) {
        this.exchangeId = exchangeId;
        this.pairId = pairId;
        this.type = type;
        this.sequenceId = sequenceId;
        this.sequenceJump = sequenceJump;
        this.eventTime = eventTime;
        this.asks = asks;
        this.bids = bids;
    }

    public int getExchangeId() {
        return exchangeId;
    }

    public void setExchangeId(int exchangeId) {
        this.exchangeId = exchangeId;
    }

    public int getPairId() {
        return pairId;
    }

    public void setPairId(int pairId) {
        this.pairId = pairId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(Long sequenceId) {
        this.sequenceId = sequenceId;
    }

    public long getSequenceJump() {
        return sequenceJump;
    }

    public void setSequenceJump(long sequenceJump) {
        this.sequenceJump = sequenceJump;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public List<PriceLevel> getAsks() {
        return asks;
    }

    public void setAsks(List<PriceLevel> asks) {
        this.asks = asks;
    }

    public List<PriceLevel> getBids() {
        return bids;
    }

    public void setBids(List<PriceLevel> bids) {
        this.bids = bids;
    }
}
