package io.tibobit.orderbook.aggregation;

import io.tibobit.orderbook.model.ConsolidatedLevel;
import io.tibobit.orderbook.model.ConsolidatedOrderBook;
import io.tibobit.orderbook.model.OrderBookEvent;
import io.tibobit.orderbook.model.OrderBookEventType;
import io.tibobit.orderbook.model.PriceLevel;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link OrderBookMerger} against the contract documented on the class
 * and on
 * {@link io.tibobit.orderbook.model.ExchangeBook}: snapshot replaces, in-order
 * update
 * upserts/deletes, stale/duplicate dropped, an out-of-order sequence resyncs
 * via the next
 * snapshot, and the union of all exchanges is emitted price-sorted (never
 * summed).
 *
 * Driven through Flink's {@link KeyedOneInputStreamOperatorTestHarness} so real
 * keyed
 * MapState and the {@code open(OpenContext)} lifecycle are exercised, not a
 * mock backend.
 */
class OrderBookMergerTest {

    private static final int PAIR = 7;

    private KeyedOneInputStreamOperatorTestHarness<Integer, OrderBookEvent, ConsolidatedOrderBook> harness;

    private void openHarness(String side) throws Exception {
        KeyedProcessOperator<Integer, OrderBookEvent, ConsolidatedOrderBook> operator = new KeyedProcessOperator<>(
                new OrderBookMerger(side));
        KeySelector<OrderBookEvent, Integer> byPair = OrderBookEvent::getPairId;
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                operator, byPair, TypeInformation.of(Integer.class));
        harness.open();
    }

    @AfterEach
    void closeHarness() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    // ---- helpers
    // ---------------------------------------------------------------------

    private static PriceLevel lvl(String price, String qty) {
        return new PriceLevel(price, qty);
    }

    private static OrderBookEvent event(
            OrderBookEventType type, int exchangeId, String side,
            long eventTime, long seq, long jump, List<PriceLevel> levels) {
        OrderBookEvent e = new OrderBookEvent();
        e.setType(type);
        e.setExchangeId(exchangeId);
        e.setPairId(PAIR);
        e.setSide(side);
        e.setEventTime(eventTime);
        e.setSequenceId(seq);
        e.setSequenceJump(jump);
        e.setLevels(levels);
        return e;
    }

    private static OrderBookEvent snapshot(int exchangeId, String side, long eventTime,
            long seq, List<PriceLevel> levels) {
        return event(OrderBookEventType.SNAPSHOT, exchangeId, side, eventTime, seq, 0, levels);
    }

    private static OrderBookEvent update(int exchangeId, String side, long eventTime,
            long seq, long jump, List<PriceLevel> levels) {
        return event(OrderBookEventType.UPDATE, exchangeId, side, eventTime, seq, jump, levels);
    }

    private void send(OrderBookEvent e) throws Exception {
        harness.processElement(e, e.getEventTime());
    }

    private List<ConsolidatedOrderBook> outputs() {
        return harness.extractOutputValues();
    }

    /**
     * The most recently emitted consolidated book (one is emitted per applied
     * event).
     */
    private ConsolidatedOrderBook lastBook() {
        List<ConsolidatedOrderBook> all = outputs();
        assertThat(all).as("expected at least one emitted book").isNotEmpty();
        return all.get(all.size() - 1);
    }

    private List<ConsolidatedLevel> lastLevels() {
        return lastBook().getLevels();
    }

    // ---- snapshot
    // --------------------------------------------------------------------

    /**
     * Snapshot events: a snapshot is the per-exchange baseline. It replaces that
     * exchange's
     * book wholesale and needs no sequence validation (it IS the truth, never a
     * delta).
     */
    @Nested
    class Snapshot {

        /**
         * Given no prior state, When a snapshot arrives, Then its levels become the
         * book and
         * are emitted price-sorted (asks ascending) carrying the snapshot's event_time.
         * Proves a snapshot is accepted as the baseline on a cold start, with no
         * sequence
         * pre-condition.
         */
        @Test
        @DisplayName("cold-start snapshot builds the book and emits its levels")
        void coldStartBuildsBook() throws Exception {
            openHarness("asks");
            send(snapshot(1, "asks", 100, 1000, List.of(lvl("101", "2"), lvl("100", "3"))));

            assertThat(lastLevels())
                    .extracting(ConsolidatedLevel::getPrice)
                    .containsExactly("100", "101"); // asks ascending
            assertThat(lastBook().getEventTime()).isEqualTo(100);
        }

        /**
         * Given an existing book, When a second snapshot arrives, Then it replaces the
         * book
         * entirely — the previous levels are gone, not merged in. Guards the
         * "snapshot = wholesale replace" rule.
         */
        @Test
        @DisplayName("a later snapshot replaces the previous book wholesale")
        void replacesPriorBookWholesale() throws Exception {
            openHarness("asks");
            send(snapshot(1, "asks", 100, 1000, List.of(lvl("100", "1"), lvl("101", "1"))));
            send(snapshot(1, "asks", 200, 5000, List.of(lvl("200", "9"))));

            assertThat(lastLevels())
                    .extracting(ConsolidatedLevel::getPrice)
                    .containsExactly("200"); // old 100/101 gone, not merged in
        }

        /**
         * Given a snapshot whose levels list is null (a legitimately empty book), When
         * processed, Then an empty consolidated book is emitted rather than throwing.
         * Guards
         * the null-levels branch of snapshot handling.
         */
        @Test
        @DisplayName("snapshot with null levels emits an empty book")
        void nullLevelsEmitsEmptyBook() throws Exception {
            openHarness("asks");
            send(snapshot(1, "asks", 100, 1000, null));

            assertThat(lastLevels()).isEmpty();
        }
    }

    // ---- update validation & application
    // --------------------------------------------

    /**
     * Update events: applied only on top of a trusted baseline, and only when the
     * sequence is
     * continuous. Covers cold start, upsert/delete/no-op deltas, the strict
     * in-order rule, and
     * gap handling (a gap discards the book until the next snapshot resyncs it).
     */
    @Nested
    class Update {

        /**
         * Given no snapshot has established a baseline, When an update arrives, Then it
         * is
         * ignored and nothing is emitted. An update can only mutate a trusted book; on
         * a cold
         * start there is nothing to apply onto.
         */
        @Test
        @DisplayName("an update before any snapshot is ignored (nothing emitted)")
        void beforeSnapshotIsIgnored() throws Exception {
            openHarness("asks");
            send(update(1, "asks", 100, 1, 1, List.of(lvl("100", "1"))));

            assertThat(outputs()).isEmpty();
        }

        /**
         * Given a baseline at seq 1000, When an exactly-in-order update (1000 + jump
         * 300 ==
         * 1300) adds a new price level, Then it is inserted and the merged book shows
         * both
         * levels. The happy path for applying a delta.
         */
        @Test
        @DisplayName("an in-order update (seq == lastSeq + jump) upserts a new level")
        void inOrderUpsertsNewLevel() throws Exception {
            openHarness("asks");
            send(snapshot(1, "asks", 100, 1000, List.of(lvl("100", "1"))));
            send(update(1, "asks", 110, 1300, 300, List.of(lvl("101", "5"))));

            assertThat(lastLevels())
                    .extracting(ConsolidatedLevel::getPrice, ConsolidatedLevel::getQuantity)
                    .containsExactly(
                            org.assertj.core.groups.Tuple.tuple("100", "1"),
                            org.assertj.core.groups.Tuple.tuple("101", "5"));
        }

        /**
         * Given a book with two levels, When an in-order update sends quantity 0 for
         * one
         * price, Then that level is removed. Quantity 0 means "delete this level", not
         * "a level
         * of size zero" (compared as BigDecimal, see ExchangeBook).
         */
        @Test
        @DisplayName("an in-order update with quantity 0 deletes that level")
        void inOrderQuantityZeroDeletesLevel() throws Exception {
            openHarness("asks");
            send(snapshot(1, "asks", 100, 1000, List.of(lvl("100", "1"), lvl("101", "5"))));
            send(update(1, "asks", 110, 1300, 300, List.of(lvl("101", "0"))));

            assertThat(lastLevels())
                    .extracting(ConsolidatedLevel::getPrice)
                    .containsExactly("100");
        }

        /**
         * Given a level at price 100, When an in-order update sends the same price with
         * a new
         * quantity, Then the quantity is overwritten in place (upsert), not duplicated.
         */
        @Test
        @DisplayName("an in-order update overwrites the quantity of an existing level")
        void inOrderModifiesExistingQuantity() throws Exception {
            openHarness("asks");
            send(snapshot(1, "asks", 100, 1000, List.of(lvl("100", "1"))));
            send(update(1, "asks", 110, 1300, 300, List.of(lvl("100", "8"))));

            assertThat(lastLevels())
                    .extracting(ConsolidatedLevel::getQuantity)
                    .containsExactly("8");
        }

        /**
         * Given a baseline, When an in-order update carries no level deltas (null
         * levels),
         * Then the book is unchanged but the sequence still advances — proven by the
         * *next*
         * exact-expected update (1600) being accepted. A heartbeat-style update must
         * move
         * lastSeq forward so following updates stay in order.
         */
        @Test
        @DisplayName("an in-order update with no level deltas leaves the book but advances the sequence")
        void inOrderWithNoLevelsAdvancesSequence() throws Exception {
            openHarness("asks");
            send(snapshot(1, "asks", 100, 1000, List.of(lvl("100", "1"))));
            send(update(1, "asks", 110, 1300, 300, null)); // no deltas

            assertThat(lastLevels())
                    .extracting(ConsolidatedLevel::getPrice, ConsolidatedLevel::getQuantity)
                    .containsExactly(org.assertj.core.groups.Tuple.tuple("100", "1"));
            assertThat(lastBook().getEventTime()).isEqualTo(110);

            // lastSeq advanced to 1300: the next exact-expected update (1600) applies.
            send(update(1, "asks", 120, 1600, 300, List.of(lvl("101", "5"))));
            assertThat(lastLevels()).extracting(ConsolidatedLevel::getPrice)
                    .containsExactly("100", "101");
        }

        /**
         * Given lastSeq == 1000, When an update repeats (seq == 1000) or rewinds
         * (seq == 999), Then both are dropped and nothing new is emitted. Protects the
         * book
         * against duplicate/replayed messages.
         */
        @Test
        @DisplayName("a stale/duplicate update (seq <= lastSeq) is dropped")
        void staleUpdateIsDropped() throws Exception {
            openHarness("asks");
            send(snapshot(1, "asks", 100, 1000, List.of(lvl("100", "1"))));
            int emittedAfterSnapshot = outputs().size();

            send(update(1, "asks", 110, 1000, 300, List.of(lvl("100", "8")))); // seq == lastSeq
            send(update(1, "asks", 110, 999, 300, List.of(lvl("100", "9")))); // seq < lastSeq

            assertThat(outputs()).hasSize(emittedAfterSnapshot); // nothing new emitted
        }

        /**
         * Given a baseline, When a forward gap appears (seq 2000 > expected 1300, i.e.
         * messages were missed), Then the book is discarded and stays in await-snapshot
         * —
         * subsequent updates are ignored until a snapshot resyncs it. Never serve a
         * book known
         * to have missed deltas.
         */
        @Test
        @DisplayName("a forward gap (seq > lastSeq + jump) discards the book until the next snapshot")
        void forwardGapDiscardsUntilSnapshot() throws Exception {
            openHarness("asks");
            send(snapshot(1, "asks", 100, 1000, List.of(lvl("100", "1"))));

            send(update(1, "asks", 110, 2000, 300, List.of(lvl("101", "5")))); // 2000 > 1300 => gap
            assertThat(lastLevels()).as("book discarded on gap").isEmpty();

            send(update(1, "asks", 120, 2300, 300, List.of(lvl("102", "5")))); // ignored: awaiting snapshot
            assertThat(lastLevels()).isEmpty();

            send(snapshot(1, "asks", 130, 9000, List.of(lvl("100", "1")))); // resync
            assertThat(lastLevels()).extracting(ConsolidatedLevel::getPrice).containsExactly("100");
        }

        /**
         * STRICT-CONTRACT regression test — this case caught a real bug (the code used
         * to
         * apply the whole band lastSeq &lt; seq &lt;= lastSeq+jump). Given expected
         * next seq ==
         * 1300, When an update arrives with an in-band but non-exact seq (1150), Then
         * it is NOT
         * applied: it discards the book and awaits a snapshot, exactly like a forward
         * gap. Only
         * seq == lastSeq + jump is in order; any other value cannot be trusted.
         */
        @Test
        @DisplayName("STRICT CONTRACT: seq below the expected next (lastSeq < seq < lastSeq+jump) is NOT applied — it resyncs")
        void seqBelowExpectedIsTreatedAsGap() throws Exception {
            openHarness("asks");
            send(snapshot(1, "asks", 100, 1000, List.of(lvl("100", "1"))));

            // expected next seq == 1000 + 300 == 1300; 1150 is in-band but not exact.
            send(update(1, "asks", 110, 1150, 300, List.of(lvl("101", "5"))));

            // Contract: only seq == lastSeq + jump is in-order; anything else discards the
            // book.
            assertThat(lastLevels())
                    .as("an unexpected intermediate sequence must not be applied")
                    .isEmpty();

            // And the book stays in await-snapshot: a following 'in-order' update is
            // ignored.
            send(update(1, "asks", 120, 1450, 300, List.of(lvl("102", "5"))));
            assertThat(lastLevels()).isEmpty();
        }
    }

    // ---- merge & sort across exchanges
    // ----------------------------------------------

    /**
     * The consolidated output: the union of every exchange's maintained book,
     * price-sorted by
     * side, with quantities never summed (equal-price levels from different
     * exchanges stay
     * separate, tagged by exchange_id).
     */
    @Nested
    class MergeAndSort {

        /**
         * Given an asks book with unordered prices, When emitted, Then levels are
         * sorted by
         * price ascending (best ask = lowest price first).
         */
        @Test
        @DisplayName("asks are sorted by price ascending")
        void asksAscending() throws Exception {
            openHarness("asks");
            send(snapshot(1, "asks", 100, 1000, List.of(lvl("102", "1"), lvl("100", "1"), lvl("101", "1"))));

            assertThat(lastLevels())
                    .extracting(ConsolidatedLevel::getPrice)
                    .containsExactly("100", "101", "102");
        }

        /**
         * Given a bids book with unordered prices, When emitted, Then levels are sorted
         * by
         * price descending (best bid = highest price first). Same merger, opposite sort
         * by
         * side.
         */
        @Test
        @DisplayName("bids are sorted by price descending")
        void bidsDescending() throws Exception {
            openHarness("bids");
            send(snapshot(1, "bids", 100, 1000, List.of(lvl("102", "1"), lvl("100", "1"), lvl("101", "1"))));

            assertThat(lastLevels())
                    .extracting(ConsolidatedLevel::getPrice)
                    .containsExactly("102", "101", "100");
        }

        /**
         * Given two exchanges quoting the same price with different sizes, When merged,
         * Then
         * both survive as separate entries tagged by exchange_id (quantities are NOT
         * summed),
         * and the larger quantity is listed first. The core "union, don't aggregate"
         * rule.
         */
        @Test
        @DisplayName("levels at equal price from different exchanges are kept separate (not summed) and tie-broken by quantity desc")
        void equalPriceAcrossExchangesNotSummed() throws Exception {
            openHarness("asks");
            send(snapshot(1, "asks", 100, 1000, List.of(lvl("100", "5"))));
            send(snapshot(2, "asks", 100, 1000, List.of(lvl("100", "8"))));

            assertThat(lastLevels())
                    .extracting(ConsolidatedLevel::getExchangeId,
                            ConsolidatedLevel::getPrice, ConsolidatedLevel::getQuantity)
                    .containsExactly(
                            org.assertj.core.groups.Tuple.tuple(2, "100", "8"), // larger qty first
                            org.assertj.core.groups.Tuple.tuple(1, "100", "5")); // not summed to "13"
        }

        /**
         * Given a level at "97240.50", When an update references the same price written
         * "97240.5" (different decimal scale), Then they collapse to a single level —
         * levels
         * are keyed by BigDecimal value in a TreeMap, not by raw string. Guards against
         * NiFi
         * formatting differences splitting one price into two phantom levels (see the
         * BigDecimal rules / ExchangeBook).
         */
        @Test
        @DisplayName("equal price with different decimal scale collapses to one level within an exchange")
        void equalPriceDifferentScaleCollapses() throws Exception {
            openHarness("asks");
            send(snapshot(1, "asks", 100, 1000, List.of(lvl("97240.50", "1"))));
            send(update(1, "asks", 110, 1300, 300, List.of(lvl("97240.5", "2")))); // same price, new qty

            assertThat(lastLevels()).hasSize(1);
            assertThat(lastLevels().get(0).getQuantity()).isEqualTo("2");
        }

        /**
         * Given two exchanges with event_times 100 and 50, When the older one (t=50)
         * triggers
         * the rebuild, Then the output event_time is the max across books (100), not
         * the
         * triggering event's. Output freshness reflects the newest contributing
         * exchange.
         */
        @Test
        @DisplayName("output event_time is the max across contributing exchanges, not the triggering event's")
        void eventTimeIsMaxAcrossExchanges() throws Exception {
            openHarness("asks");
            send(snapshot(1, "asks", 100, 1000, List.of(lvl("100", "1"))));
            send(snapshot(2, "asks", 50, 1000, List.of(lvl("101", "1")))); // triggering event_time=50

            assertThat(lastBook().getEventTime()).isEqualTo(100); // max(100, 50)
        }

        /**
         * Given a bids snapshot for the keyed pair, When emitted, Then the consolidated
         * book
         * carries the correct pair_id and side, so downstream topic routing is correct.
         */
        @Test
        @DisplayName("output carries the pair id and side")
        void carriesPairIdAndSide() throws Exception {
            openHarness("bids");
            send(snapshot(1, "bids", 100, 1000, List.of(lvl("100", "1"))));

            assertThat(lastBook().getPairId()).isEqualTo(PAIR);
            assertThat(lastBook().getSide()).isEqualTo("bids");
        }
    }
}
