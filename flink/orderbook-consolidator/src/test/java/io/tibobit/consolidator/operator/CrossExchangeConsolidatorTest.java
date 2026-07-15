package io.tibobit.consolidator.operator;

import io.tibobit.consolidator.model.ConsolidatedLevel;
import io.tibobit.consolidator.model.ConsolidatedOrderBook;
import io.tibobit.consolidator.model.ExchangeBook;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/**
 * Tests {@link CrossExchangeConsolidator} (stage 2) against its documented contract: it keeps the
 * latest {@link ExchangeBook} per exchange, unions their levels (R4 — never summed, equal prices
 * from different exchanges stay separate), sorts by side (R5 — asks ascending, bids descending,
 * tie-broken by larger quantity, compared as BigDecimal), stamps the output event_time with the
 * max across contributing exchanges, and carries pair_id/side for downstream topic routing.
 *
 * Driven through Flink's {@link KeyedOneInputStreamOperatorTestHarness} — keyed exactly as the
 * job does, {@code (pair_id, side)} — so real keyed MapState and the {@code open(OpenContext)}
 * lifecycle (which builds both comparators) are exercised, not a mock backend.
 */
class CrossExchangeConsolidatorTest {

    private static final int PAIR = 7;

    private KeyedOneInputStreamOperatorTestHarness<String, ExchangeBook, ConsolidatedOrderBook> harness;

    @BeforeEach
    void openHarness() throws Exception {
        KeyedProcessOperator<String, ExchangeBook, ConsolidatedOrderBook> operator =
                new KeyedProcessOperator<>(new CrossExchangeConsolidator());
        // Same key as OrderBookConsolidatorJob: (pair_id, side).
        KeySelector<ExchangeBook, String> byKey = b -> b.getPairId() + "|" + b.getSide();
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                operator, byKey, TypeInformation.of(String.class));
        harness.open();
    }

    @AfterEach
    void closeHarness() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    // ---- helpers ----------------------------------------------------------------

    private static ConsolidatedLevel lvl(int exchangeId, String price, String qty) {
        return new ConsolidatedLevel(exchangeId, price, qty);
    }

    private static ExchangeBook book(int exchangeId, String side, long eventTime,
                                     ConsolidatedLevel... levels) {
        return new ExchangeBook(PAIR, exchangeId, side, List.of(levels), eventTime);
    }

    private void send(ExchangeBook b) throws Exception {
        harness.processElement(b, b.getEventTime());
    }

    private ConsolidatedOrderBook lastBook() {
        List<ConsolidatedOrderBook> all = harness.extractOutputValues();
        assertThat(all).as("expected at least one emitted book").isNotEmpty();
        return all.get(all.size() - 1);
    }

    private List<ConsolidatedLevel> lastLevels() {
        return lastBook().getLevels();
    }

    // ---- R5 sort ----------------------------------------------------------------

    /**
     * Given an asks book with unordered prices, When consolidated, Then the union is sorted by
     * price ascending (best ask = lowest price first).
     */
    @Test
    @DisplayName("R5: asks are sorted by price ascending")
    void asksAscending() throws Exception {
        send(book(1, "asks", 100, lvl(1, "102", "1"), lvl(1, "100", "1"), lvl(1, "101", "1")));

        assertThat(lastLevels()).extracting(ConsolidatedLevel::getPrice)
                .containsExactly("100", "101", "102");
    }

    /**
     * Given a bids book with unordered prices, When consolidated, Then the union is sorted by
     * price descending (best bid = highest price first). Same operator, opposite direction chosen
     * per record from the book's side.
     */
    @Test
    @DisplayName("R5: bids are sorted by price descending")
    void bidsDescending() throws Exception {
        send(book(1, "bids", 100, lvl(1, "102", "1"), lvl(1, "100", "1"), lvl(1, "101", "1")));

        assertThat(lastLevels()).extracting(ConsolidatedLevel::getPrice)
                .containsExactly("102", "101", "100");
    }

    /**
     * Given prices whose lexicographic order differs from their numeric order ("9", "10", "100"),
     * When sorted ascending, Then they come out numerically ordered — proving the comparator
     * compares as BigDecimal, not as strings (see memory/project_bigdecimal_rules.md).
     */
    @Test
    @DisplayName("R5: prices sort numerically (BigDecimal), not lexicographically")
    void sortsNumericallyNotLexicographically() throws Exception {
        send(book(1, "asks", 100, lvl(1, "100", "1"), lvl(1, "9", "1"), lvl(1, "10", "1")));

        assertThat(lastLevels()).extracting(ConsolidatedLevel::getPrice)
                .containsExactly("9", "10", "100"); // lexicographic would be 10, 100, 9
    }

    // ---- R4 union (never summed) ------------------------------------------------

    /**
     * Given two exchanges quoting the same price with different sizes, When consolidated, Then
     * both survive as separate entries tagged by exchange_id (quantities are NOT summed), and the
     * larger quantity is listed first (tie-break). The core "union, don't aggregate" rule (R4).
     */
    @Test
    @DisplayName("R4: equal price from different exchanges is kept separate (not summed), tie-broken by quantity desc")
    void equalPriceAcrossExchangesNotSummed() throws Exception {
        send(book(1, "asks", 100, lvl(1, "100", "5")));
        send(book(2, "asks", 100, lvl(2, "100", "8")));

        assertThat(lastLevels())
                .extracting(ConsolidatedLevel::getExchangeId,
                        ConsolidatedLevel::getPrice, ConsolidatedLevel::getQuantity)
                .containsExactly(
                        tuple(2, "100", "8"),  // larger quantity first
                        tuple(1, "100", "5")); // NOT summed to "13"
    }

    /**
     * Given two exchanges quoting the same price written at different decimal scales ("100.00"
     * vs "100"), When consolidated, Then the comparator treats them as equal price (BigDecimal,
     * scale-insensitive) and tie-breaks by quantity — the two entries land adjacent, larger
     * quantity first, with their original price strings preserved (stage 2 does not canonicalize).
     */
    @Test
    @DisplayName("R5: equal price of different scale is tie-broken by quantity (BigDecimal-equal)")
    void equalPriceDifferentScaleTieBrokenByQuantity() throws Exception {
        send(book(1, "asks", 100, lvl(1, "100.00", "5")));
        send(book(2, "asks", 100, lvl(2, "100", "8")));

        assertThat(lastLevels())
                .extracting(ConsolidatedLevel::getExchangeId,
                        ConsolidatedLevel::getPrice, ConsolidatedLevel::getQuantity)
                .containsExactly(
                        tuple(2, "100", "8"),
                        tuple(1, "100.00", "5"));
    }

    // ---- per-exchange replacement & event_time ----------------------------------

    /**
     * Given an exchange's book in state, When a newer book from the SAME exchange arrives, Then it
     * replaces that exchange's entry wholesale — the previous levels are gone, not accumulated.
     * Stage 1 emits the exchange's whole book on every change, so stage 2 must overwrite per
     * exchange_id, never append.
     */
    @Test
    @DisplayName("a newer book from the same exchange replaces its entry (not accumulated)")
    void newerBookReplacesExchangeEntry() throws Exception {
        send(book(1, "asks", 100, lvl(1, "100", "5")));
        send(book(1, "asks", 200, lvl(1, "200", "9"))); // same exchange, new book

        assertThat(lastLevels()).extracting(ConsolidatedLevel::getPrice)
                .containsExactly("200"); // old "100" replaced, not "100" + "200"
    }

    /**
     * Given two exchanges with event_times 100 and 50, When the older (t=50) triggers the rebuild,
     * Then the output event_time is the max across contributing books (100), not the triggering
     * event's. Output freshness reflects the newest contributing exchange.
     */
    @Test
    @DisplayName("output event_time is the max across contributing exchanges")
    void eventTimeIsMaxAcrossExchanges() throws Exception {
        send(book(1, "asks", 100, lvl(1, "100", "1")));
        send(book(2, "asks", 50, lvl(2, "101", "1"))); // triggering event_time = 50

        assertThat(lastBook().getEventTime()).isEqualTo(100); // max(100, 50)
    }

    /**
     * Given an exchange that has emptied its book (no levels) but carries a fresh event_time, When
     * it participates in the union, Then it contributes no levels but its event_time still counts
     * toward the output max. An emptied exchange must not drop the book's freshness.
     */
    @Test
    @DisplayName("an emptied exchange contributes its event_time but no levels")
    void emptyExchangeContributesEventTimeOnly() throws Exception {
        send(book(1, "asks", 100, lvl(1, "100", "1")));
        send(book(2, "asks", 200)); // ex2 emptied, newer event_time

        assertThat(lastLevels()).extracting(ConsolidatedLevel::getPrice)
                .containsExactly("100"); // only ex1's level
        assertThat(lastBook().getEventTime()).isEqualTo(200); // ex2's event_time still counts
    }

    // ---- routing fields ---------------------------------------------------------

    /**
     * Given a bids book for the keyed pair, When consolidated, Then the output carries the correct
     * pair_id and side so the sink routes it to p{pair_id}-{side} (R6).
     */
    @Test
    @DisplayName("output carries pair_id and side for topic routing")
    void carriesPairIdAndSide() throws Exception {
        send(book(1, "bids", 100, lvl(1, "100", "1")));

        assertThat(lastBook().getPairId()).isEqualTo(PAIR);
        assertThat(lastBook().getSide()).isEqualTo("bids");
    }
}
