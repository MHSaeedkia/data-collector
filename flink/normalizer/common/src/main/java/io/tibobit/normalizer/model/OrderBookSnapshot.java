package io.tibobit.normalizer.model;

import java.util.List;

/**
 * Job-5 output: the full maintained book of one (exchange, pair), emitted on every accepted
 * event (schema schemas/order_book_snapshot.avsc, subject order-book-snapshot). Both sides are
 * required here — a built book always has both, possibly empty. {@code lastSequenceId} is null
 * only for feeds with no ordering field (ex3 wallex).
 *
 * <p>{@code simulation} is NiFi's flag carried up the pipeline (0 = live, 1 = simulation). The book
 * is state built from many events, so the emitted snapshot carries the flag of the LAST accepted
 * event for this (exchange, pair) — in practice a feed does not switch mid-stream. Job 6 stamps it
 * onto every level so the aggregated book stays attributable per exchange.
 *
 * <p>{@code sourceIds} is where the pipeline genuinely fans in. Everywhere else a record has ONE
 * parent; here the book is state accumulated over many events, so it lists the id of every
 * event that still holds a resting level, plus the event that triggered this emit. It therefore
 * grows with book depth, not with pipeline length. See memory/project_record_lineage.md.
 */
public class OrderBookSnapshot {

    private int exchangeId;
    private int pairId;
    private int simulation;
    private String id = "";
    private List<String> sourceIds = List.of();
    private long eventTime;
    private Long lastSequenceId;
    private List<PriceLevel> asks;
    private List<PriceLevel> bids;
    private PipelineTimings pipelineTimings = new PipelineTimings();

    public OrderBookSnapshot() {
    }

    public OrderBookSnapshot(int exchangeId, int pairId, long eventTime, Long lastSequenceId,
                             List<PriceLevel> asks, List<PriceLevel> bids) {
        this.exchangeId = exchangeId;
        this.pairId = pairId;
        this.eventTime = eventTime;
        this.lastSequenceId = lastSequenceId;
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

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public Long getLastSequenceId() {
        return lastSequenceId;
    }

    public void setLastSequenceId(Long lastSequenceId) {
        this.lastSequenceId = lastSequenceId;
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
