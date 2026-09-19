package io.tibobit.normalizer.model;

/**
 * Job 1's output: a book event plus the exchange's own market string it belongs to. The event's
 * {@code pair_id} is still unset (0) — job 2 fills it after resolving the market string against
 * exchange_markets, and {@code exchange_id} is already set (job 1 reads it off the
 * {@code ex{id}-raw} topic name).
 *
 * <p>Lives in {@code common} rather than in the parser job because it is a WIRE type since the
 * 2026-09-19 split: job 1 writes it to {@code ex{id}-parsed-flink} (subject
 * {@code parsed-book-event}) and job 2 reads it back. Wrapping a {@link RawOrderBookEvent} rather
 * than redeclaring its fields is what keeps the two schemas from drifting — a field added to the
 * raw event is carried here for free, and the only thing the parsed schema says differently is
 * {@code market} in place of {@code pair_id}.
 */
public class ParsedBookEvent {

    private final String market;
    private final RawOrderBookEvent event;

    public ParsedBookEvent(String market, RawOrderBookEvent event) {
        this.market = market;
        this.event = event;
    }

    public String getMarket() {
        return market;
    }

    public RawOrderBookEvent getEvent() {
        return event;
    }
}
