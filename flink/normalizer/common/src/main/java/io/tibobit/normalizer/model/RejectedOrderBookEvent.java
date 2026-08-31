package io.tibobit.normalizer.model;

/**
 * Dead-letter envelope written by the type validator (schema
 * schemas/rejected_order_book_event.avsc, subject rejected-order-book-event): the rejected
 * event verbatim plus a human-readable reason and the validator's processing time.
 */
public class RejectedOrderBookEvent {

    private RawOrderBookEvent event;
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
