package io.tibobit.normalizer.bookbuild;

/**
 * One level resting in the book, as held in {@link BookBuildFunction}'s MapState: the quantity plus
 * the {@code id} of the event that last set it. That id rides out on the emitted level as its
 * {@code source_id}, and is what makes a single price traceable back to the one raw event that
 * produced it — the emitted record itself names only the event that triggered the emit.
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
    private String id;

    public RestingLevel() {
    }

    public RestingLevel(String price, String quantity, String id) {
        this.price = price;
        this.quantity = quantity;
        this.id = id;
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

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
