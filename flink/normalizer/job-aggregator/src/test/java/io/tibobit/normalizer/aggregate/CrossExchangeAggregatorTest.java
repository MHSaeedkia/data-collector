package io.tibobit.normalizer.aggregate;

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
 * Tests {@link CrossExchangeAggregator} against its documented contract: it keeps the latest
 * {@link ExchangeBook} per exchange, unions their levels (never summed, equal prices from different
 * exchanges stay separate), sorts by side (asks ascending, bids descending, tie-broken by larger
 * quantity, compared as BigDecimal), stamps the output event_time with the max across contributing
 * exchanges, and carries pair_id/side for downstream topic routing.
 *
 * <p>Driven through Flink's {@link KeyedOneInputStreamOperatorTestHarness} — keyed exactly as the
 * job, {@code (pair_id, side)} — so real keyed MapState and the {@code open(OpenContext)} lifecycle
 * (which builds both comparators) run, not a mock. Covers union/sort plus the reset case
 * (empty book ⇒ exchange drops out).
 */
class CrossExchangeAggregatorTest {

    private static final int PAIR = 7;

    private KeyedOneInputStreamOperatorTestHarness<String, ExchangeBook, AggregatedOrderBook> harness;

    @BeforeEach
    void openHarness() throws Exception {
        KeyedProcessOperator<String, ExchangeBook, AggregatedOrderBook> operator =
                new KeyedProcessOperator<>(new CrossExchangeAggregator());
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

    private static AggregatedLevel lvl(int exchangeId, String price, String qty) {
        return new AggregatedLevel(exchangeId, price, qty);
    }

    private static ExchangeBook book(int exchangeId, String side, long eventTime,
                                     AggregatedLevel... levels) {
        return new ExchangeBook(PAIR, exchangeId, side, List.of(levels), eventTime);
    }

    private void send(ExchangeBook b) throws Exception {
        harness.processElement(b, b.getEventTime());
    }

    private AggregatedOrderBook lastBook() {
        List<AggregatedOrderBook> all = harness.extractOutputValues();
        assertThat(all).as("expected at least one emitted book").isNotEmpty();
        return all.get(all.size() - 1);
    }

    private List<AggregatedLevel> lastLevels() {
        return lastBook().getLevels();
    }

    // ---- sort -------------------------------------------------------------------

    @Test
    @DisplayName("asks are sorted by price ascending")
    void asksAscending() throws Exception {
        send(book(1, "asks", 100, lvl(1, "102", "1"), lvl(1, "100", "1"), lvl(1, "101", "1")));

        assertThat(lastLevels()).extracting(AggregatedLevel::getPrice)
                .containsExactly("100", "101", "102");
    }

    @Test
    @DisplayName("bids are sorted by price descending")
    void bidsDescending() throws Exception {
        send(book(1, "bids", 100, lvl(1, "102", "1"), lvl(1, "100", "1"), lvl(1, "101", "1")));

        assertThat(lastLevels()).extracting(AggregatedLevel::getPrice)
                .containsExactly("102", "101", "100");
    }

    @Test
    @DisplayName("prices sort numerically (BigDecimal), not lexicographically")
    void sortsNumericallyNotLexicographically() throws Exception {
        send(book(1, "asks", 100, lvl(1, "100", "1"), lvl(1, "9", "1"), lvl(1, "10", "1")));

        assertThat(lastLevels()).extracting(AggregatedLevel::getPrice)
                .containsExactly("9", "10", "100"); // lexicographic would be 10, 100, 9
    }

    // ---- union (never summed) ---------------------------------------------------

    @Test
    @DisplayName("equal price from different exchanges is kept separate (not summed), tie-broken by quantity desc")
    void equalPriceAcrossExchangesNotSummed() throws Exception {
        send(book(1, "asks", 100, lvl(1, "100", "5")));
        send(book(2, "asks", 100, lvl(2, "100", "8")));

        assertThat(lastLevels())
                .extracting(AggregatedLevel::getExchangeId,
                        AggregatedLevel::getPrice, AggregatedLevel::getQuantity)
                .containsExactly(
                        tuple(2, "100", "8"),  // larger quantity first
                        tuple(1, "100", "5")); // NOT summed to "13"
    }

    @Test
    @DisplayName("equal price of different scale is tie-broken by quantity (BigDecimal-equal)")
    void equalPriceDifferentScaleTieBrokenByQuantity() throws Exception {
        send(book(1, "asks", 100, lvl(1, "100.00", "5")));
        send(book(2, "asks", 100, lvl(2, "100", "8")));

        assertThat(lastLevels())
                .extracting(AggregatedLevel::getExchangeId,
                        AggregatedLevel::getPrice, AggregatedLevel::getQuantity)
                .containsExactly(
                        tuple(2, "100", "8"),
                        tuple(1, "100.00", "5"));
    }

    // ---- per-exchange replacement & event_time ----------------------------------

    @Test
    @DisplayName("a newer book from the same exchange replaces its entry (not accumulated)")
    void newerBookReplacesExchangeEntry() throws Exception {
        send(book(1, "asks", 100, lvl(1, "100", "5")));
        send(book(1, "asks", 200, lvl(1, "200", "9"))); // same exchange, new book

        assertThat(lastLevels()).extracting(AggregatedLevel::getPrice)
                .containsExactly("200"); // old "100" replaced, not "100" + "200"
    }

    @Test
    @DisplayName("output event_time is the max across contributing exchanges")
    void eventTimeIsMaxAcrossExchanges() throws Exception {
        send(book(1, "asks", 100, lvl(1, "100", "1")));
        send(book(2, "asks", 50, lvl(2, "101", "1"))); // triggering event_time = 50

        assertThat(lastBook().getEventTime()).isEqualTo(100); // max(100, 50)
    }

    // ---- reset: empty book ⇒ exchange drops out ---------------------------------

    @Test
    @DisplayName("an empty book (job 5 reset) drops that exchange from the union, others intact")
    void emptyBookDropsExchangeOthersIntact() throws Exception {
        send(book(1, "asks", 100, lvl(1, "100", "1")));
        send(book(2, "asks", 100, lvl(2, "101", "1")));
        assertThat(lastLevels()).extracting(AggregatedLevel::getPrice)
                .containsExactly("100", "101"); // both exchanges present

        send(book(2, "asks", 200)); // ex2 reset -> empty book, newer event_time

        assertThat(lastLevels())
                .extracting(AggregatedLevel::getExchangeId, AggregatedLevel::getPrice)
                .containsExactly(tuple(1, "100")); // only ex1 remains
        assertThat(lastBook().getEventTime()).isEqualTo(200); // ex2's event_time still counts
    }

    // ---- routing fields ---------------------------------------------------------

    @Test
    @DisplayName("output carries pair_id and side for topic routing")
    void carriesPairIdAndSide() throws Exception {
        send(book(1, "bids", 100, lvl(1, "100", "1")));

        assertThat(lastBook().getPairId()).isEqualTo(PAIR);
        assertThat(lastBook().getSide()).isEqualTo("bids");
    }
}
