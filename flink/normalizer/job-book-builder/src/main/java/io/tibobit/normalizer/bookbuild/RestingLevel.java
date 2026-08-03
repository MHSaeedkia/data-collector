package io.tibobit.normalizer.bookbuild;

/**
 * One level resting in the book, as held in {@link BookBuildFunction}'s MapState: the quantity plus
 * the {@code sink_id} of the event that last set it. The sink id is what makes the emitted
 * snapshot's {@code source_ids} answer "which events is this book actually made of" rather than just
 * "which event triggered this emit".
 *
 * <p>{@code price} duplicates the MapState key. That redundancy is deliberate: it lets this one type
 * serve both as the stored value and as the thing that gets sorted on the way out, instead of a
 * second near-identical type existing only to carry the key alongside the value.
 *
 * <p>Plain POJO (no-arg ctor + getters/setters) so Flink can store it in keyed MapState. NOTE: this
 * changes the value type of the existing {@code asks}/{@code bids} state. No checkpointing is
 * configured on this platform, so there is no state to migrate — if that ever changes, this is a
 * breaking state-schema change.
 */
public class RestingLevel {

    private String price;
    private String quantity;
    private String sinkId;

    public RestingLevel() {
    }

    public RestingLevel(String price, String quantity, String sinkId) {
        this.price = price;
        this.quantity = quantity;
        this.sinkId = sinkId;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    public String getQuantity() {
        return quantity;
    }

    public void setQuantity(String quantity) {
        this.quantity = quantity;
    }

    public String getSinkId() {
        return sinkId;
    }

    public void setSinkId(String sinkId) {
        this.sinkId = sinkId;
    }
}
