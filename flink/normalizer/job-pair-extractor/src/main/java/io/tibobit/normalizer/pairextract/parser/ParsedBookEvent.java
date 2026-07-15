package io.tibobit.normalizer.pairextract.parser;

import io.tibobit.normalizer.model.RawOrderBookEvent;

/**
 * Parser output: a book event plus the exchange's market string it belongs to. The event's
 * exchange_id/pair_id are still unset (0) — PairExtractFunction fills both after resolving
 * the market string against exchange_markets.
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
