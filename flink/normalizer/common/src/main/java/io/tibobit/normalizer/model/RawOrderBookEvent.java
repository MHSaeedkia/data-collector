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
 *       (ex6=1, ex8=300, ex5=600); 0 = snapshot feed — out-of-order check only.</li>
 *   <li>{@code sequenceJumpTolerance}: half-width of the accepted window around
 *       {@code sequenceJump}, so the rule is really
 *       {@code last + jump - tol <= seq <= last + jump + tol}. 0 everywhere except ex5/bitget,
 *       which stamps 10: its sequence is a millisecond TIMESTAMP on a nominal 600 ms cadence,
 *       not a counter, so it never lands on an exact multiple. At 0 the window collapses to the
 *       exact check the other delta feeds have always had.</li>
 *   <li>{@code simulation}: NiFi's flag from the raw payload — 0 = live, 1 = simulation, other
 *       values undefined, absent = 0. Set by job 1 and carried unchanged by jobs 2–4. It is NOT
 *       part of any keying or validation rule; it only rides along.</li>
 *   <li>{@code id}/{@code sourceIds}: record lineage. Unlike {@code simulation}, these are
 *       RE-STAMPED at every hop — each job mints a fresh {@code id} when it writes the event to
 *       its topic and sets {@code sourceIds} to the id(s) it read. Immediate parents only,
 *       never the accumulated chain, so this list has exactly one element everywhere on the raw
 *       stream. See memory/project_record_lineage.md.</li>
 * </ul>
 */
public class RawOrderBookEvent {

    private int exchangeId;
    private int pairId;
    private String type;
    private int simulation;
    private String id = "";
    private List<String> sourceIds = List.of();
    private Long sequenceId;
    private long sequenceJump;
    private long sequenceJumpTolerance;
    private long eventTime;
    private List<PriceLevel> asks;
    private List<PriceLevel> bids;
    private PipelineTimings pipelineTimings = new PipelineTimings();

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

    public int getSimulation() {
        return simulation;
    }

    public void setSimulation(int simulation) {
        this.simulation = simulation;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<String> getSourceIds() {
        return sourceIds;
    }

    public void setSourceIds(List<String> sourceIds) {
        this.sourceIds = sourceIds;
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

    public long getSequenceJumpTolerance() {
        return sequenceJumpTolerance;
    }

    public void setSequenceJumpTolerance(long sequenceJumpTolerance) {
        this.sequenceJumpTolerance = sequenceJumpTolerance;
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

    public PipelineTimings getPipelineTimings() {
        return pipelineTimings;
    }

    public void setPipelineTimings(PipelineTimings pipelineTimings) {
        this.pipelineTimings = pipelineTimings;
    }
}
