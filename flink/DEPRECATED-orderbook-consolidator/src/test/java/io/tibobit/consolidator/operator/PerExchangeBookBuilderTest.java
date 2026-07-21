package io.tibobit.consolidator.operator;

import io.tibobit.consolidator.model.ConsolidatedLevel;
import io.tibobit.consolidator.model.ExchangeBook;
import io.tibobit.consolidator.model.PriceLevelEvent;
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

/**
 * Tests {@link PerExchangeBookBuilder} (stage 1) against its documented contract: per exchange
 * it upserts the latest quantity for a price by {@code event_time} (R1), removes on quantity 0
 * (R2), emits that exchange's whole book stamped with its {@code exchange_id}, whose
 * {@code event_time} is the max across the remaining levels, and keys prices by a canonical
 * decimal string so equal prices of different scale collapse to one level.
 *
 * Driven through Flink's {@link KeyedOneInputStreamOperatorTestHarness} — keyed exactly as the
 * job does, {@code (pair_id, exchange_id, side)} — so real keyed MapState and the
 * {@code open(OpenContext)} lifecycle are exercised, not a mock backend.
 */
class PerExchangeBookBuilderTest {

    private static final int PAIR = 7;
    private static final int EX = 1;

    private KeyedOneInputStreamOperatorTestHarness<String, PriceLevelEvent, ExchangeBook> harness;

    @BeforeEach
    void openHarness() throws Exception {
        KeyedProcessOperator<String, PriceLevelEvent, ExchangeBook> operator =
                new KeyedProcessOperator<>(new PerExchangeBookBuilder());
        // Same key as OrderBookConsolidatorJob: (pair_id, exchange_id, side).
        KeySelector<PriceLevelEvent, String> byKey =
                e -> e.getPairId() + "|" + e.getExchangeId() + "|" + e.getSide();
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

    private static PriceLevelEvent event(int exchangeId, String side, long eventTime,
                                         String price, String quantity) {
        return new PriceLevelEvent(exchangeId, PAIR, side, eventTime, price, quantity);
    }

    private void send(PriceLevelEvent e) throws Exception {
        harness.processElement(e, e.getEventTime());
    }

    private List<ExchangeBook> outputs() {
        return harness.extractOutputValues();
    }

    private ExchangeBook lastBook() {
        List<ExchangeBook> all = outputs();
        assertThat(all).as("expected at least one emitted book").isNotEmpty();
        return all.get(all.size() - 1);
    }

    private List<ConsolidatedLevel> lastLevels() {
        return lastBook().getLevels();
    }

    // ---- emit shape -------------------------------------------------------------

    /**
     * Given no prior state, When a single level arrives, Then a one-level book is emitted
     * carrying the pair/exchange/side and the level stamped with this exchange_id and its exact
     * price/quantity, with event_time equal to the event's. The cold-start baseline.
     */
    @Test
    @DisplayName("first level emits a one-level book stamped with pair/exchange/side")
    void firstLevelBuildsBook() throws Exception {
        send(event(EX, "asks", 100, "100", "5"));

        assertThat(lastBook().getPairId()).isEqualTo(PAIR);
        assertThat(lastBook().getExchangeId()).isEqualTo(EX);
        assertThat(lastBook().getSide()).isEqualTo("asks");
        assertThat(lastBook().getEventTime()).isEqualTo(100);
        assertThat(lastLevels()).singleElement()
                .extracting(ConsolidatedLevel::getExchangeId,
                        ConsolidatedLevel::getPrice, ConsolidatedLevel::getQuantity)
                .containsExactly(EX, "100", "5");
    }

    // ---- R1 upsert-latest -------------------------------------------------------

    /**
     * Given a level at price 100, When a newer event_time sends a new quantity for the same
     * price, Then the quantity is overwritten in place (upsert), not duplicated. The R1 happy
     * path.
     */
    @Test
    @DisplayName("R1: a newer event_time upserts the quantity of an existing price")
    void newerEventUpsertsQuantity() throws Exception {
        send(event(EX, "asks", 100, "100", "5"));
        send(event(EX, "asks", 200, "100", "9"));

        assertThat(lastLevels()).singleElement()
                .extracting(ConsolidatedLevel::getQuantity).isEqualTo("9");
    }

    /**
     * Given a level set at event_time 200, When an older event_time (100) arrives for the same
     * price, Then it is stale and dropped — nothing is emitted and the held quantity is unchanged.
     * Guards R1's "latest wins": out-of-order delivery must not resurrect an old quantity.
     */
    @Test
    @DisplayName("R1: an older event_time is stale — dropped, nothing emitted")
    void olderEventTimeIsIgnored() throws Exception {
        send(event(EX, "asks", 200, "100", "5"));
        int emitted = outputs().size();

        send(event(EX, "asks", 100, "100", "9")); // older -> stale

        assertThat(outputs()).hasSize(emitted); // no new emit
        assertThat(lastLevels()).singleElement()
                .extracting(ConsolidatedLevel::getQuantity).isEqualTo("5");
    }

    /**
     * Given a level set at event_time 200, When an event with the SAME event_time (200) arrives
     * for that price, Then it is applied (upserts) — only a strictly-older event_time is stale.
     * Pins the {@code >=} boundary of R1 (equal is not dropped).
     */
    @Test
    @DisplayName("R1: an equal event_time is applied (only strictly-older is stale)")
    void equalEventTimeApplies() throws Exception {
        send(event(EX, "asks", 200, "100", "5"));
        send(event(EX, "asks", 200, "100", "9")); // equal event_time

        assertThat(lastLevels()).singleElement()
                .extracting(ConsolidatedLevel::getQuantity).isEqualTo("9");
    }

    // ---- R2 remove --------------------------------------------------------------

    /**
     * Given a book with two prices, When a quantity 0 arrives for one price, Then that level is
     * removed and the remaining book is emitted. Quantity 0 means "delete this level" (compared
     * as BigDecimal via signum, so "0"/"0.0" all count).
     */
    @Test
    @DisplayName("R2: quantity 0 removes that price level")
    void quantityZeroRemovesLevel() throws Exception {
        send(event(EX, "asks", 100, "100", "1"));
        send(event(EX, "asks", 110, "101", "5"));
        send(event(EX, "asks", 120, "101", "0")); // delete 101

        assertThat(lastLevels()).extracting(ConsolidatedLevel::getPrice)
                .containsExactly("100");
    }

    /**
     * Given no level at a price, When a quantity 0 arrives for it, Then it is a no-op — nothing
     * is stored and nothing is emitted. Deleting an absent level must not emit a spurious
     * (unchanged) book downstream.
     */
    @Test
    @DisplayName("R2: quantity 0 for an absent price is a no-op (nothing emitted)")
    void quantityZeroOnAbsentPriceIsNoOp() throws Exception {
        send(event(EX, "asks", 100, "100", "0"));

        assertThat(outputs()).isEmpty();
    }

    // ---- per-exchange event_time = max across levels ----------------------------

    /**
     * Given levels set at different event_times, When the book is emitted, Then its event_time is
     * the max across the currently-held levels — even when the triggering (removal) event carries
     * a larger timestamp than any surviving level. Book freshness reflects its remaining levels,
     * not the last message.
     */
    @Test
    @DisplayName("book event_time is the max across remaining levels, not the triggering event's")
    void eventTimeIsMaxAcrossLevels() throws Exception {
        send(event(EX, "asks", 100, "100", "1"));
        send(event(EX, "asks", 200, "200", "1"));
        assertThat(lastBook().getEventTime()).isEqualTo(200); // max(100, 200)

        // Remove the newer price at t=250; the only survivor is the t=100 level.
        send(event(EX, "asks", 250, "200", "0"));
        assertThat(lastBook().getEventTime()).isEqualTo(100); // max across survivors, not 250
    }

    /**
     * Given a single level, When it is removed by a later quantity 0, Then an empty book is
     * emitted whose event_time falls back to the triggering event's time (there are no levels
     * left to take a max over). Pins the empty-book branch.
     */
    @Test
    @DisplayName("emptying the book falls back to the triggering event's event_time")
    void emptyBookFallsBackToEventTime() throws Exception {
        send(event(EX, "asks", 100, "100", "1"));
        send(event(EX, "asks", 300, "100", "0")); // removes the last level

        assertThat(lastLevels()).isEmpty();
        assertThat(lastBook().getEventTime()).isEqualTo(300);
    }

    // ---- canonical price key ----------------------------------------------------

    /**
     * Given a level written "0.10", When a later event references the same price written "0.1"
     * (different decimal scale), Then they collapse to one level keyed by the canonical decimal
     * string — MapState is hash-based, so the price key is normalized with
     * {@code BigDecimal.stripTrailingZeros().toPlainString()} to avoid formatting differences
     * splitting one price into two phantom levels (see memory/project_bigdecimal_rules.md).
     */
    @Test
    @DisplayName("equal price of different scale collapses to one canonical level")
    void equalPriceDifferentScaleCollapses() throws Exception {
        send(event(EX, "asks", 100, "0.10", "1"));
        send(event(EX, "asks", 110, "0.1", "2")); // same price, new quantity

        assertThat(lastLevels()).singleElement()
                .extracting(ConsolidatedLevel::getPrice, ConsolidatedLevel::getQuantity)
                .containsExactly("0.1", "2"); // canonical price, latest quantity
    }
}
