package io.tibobit.normalizer.model;

import java.util.List;

/**
 * Dead-letter envelope written by the type validator (schema
 * schemas/rejected_order_book_event.avsc, subject rejected-order-book-event): the rejected
 * event verbatim plus a human-readable reason and the validator's processing time.
 *
 * <p>The dead-letter topic is a topic, so this envelope gets its own {@code sinkId} like any other
 * emit, with {@code sourceIds} = the rejected event's sink id. Do not confuse it with
 * {@code event.getSinkId()}, which is the id job 1 minted for the event being rejected.
 */
public class RejectedOrderBookEvent {

    private RawOrderBookEvent event;
    private String sinkId = "";
    private List<String> sourceIds = List.of();
    private String rejectReason;
    private long rejectedAt;

    public RejectedOrderBookEvent() {
    }

    public RejectedOrderBookEvent(RawOrderBookEvent event, String rejectReason, long rejectedAt) {
        this.event = event;
        this.rejectReason = rejectReason;
        this.rejectedAt = rejectedAt;
    }

    public RawOrderBookEvent getEvent() {
        return event;
    }

    public void setEvent(RawOrderBookEvent event) {
        this.event = event;
    }

    public String getSinkId() {
        return sinkId;
    }

    public void setSinkId(String sinkId) {
        this.sinkId = sinkId;
    }

    public List<String> getSourceIds() {
        return sourceIds;
    }

    public void setSourceIds(List<String> sourceIds) {
        this.sourceIds = sourceIds;
    }

    public String getRejectReason() {
        return rejectReason;
    }

    public void setRejectReason(String rejectReason) {
        this.rejectReason = rejectReason;
    }

    public long getRejectedAt() {
        return rejectedAt;
    }

    public void setRejectedAt(long rejectedAt) {
        this.rejectedAt = rejectedAt;
    }
}
