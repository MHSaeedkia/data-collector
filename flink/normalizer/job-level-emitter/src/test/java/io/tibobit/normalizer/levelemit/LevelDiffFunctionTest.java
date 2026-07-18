package io.tibobit.normalizer.levelemit;

import io.tibobit.normalizer.model.OrderBookSnapshot;
import io.tibobit.normalizer.model.PriceLevel;
import io.tibobit.normalizer.model.PriceLevelEvent;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LevelDiffFunctionTest {

    private static final long EVENT_TIME = 1_700_000_000_000L;

    private KeyedOneInputStreamOperatorTestHarness<String, OrderBookSnapshot, PriceLevelEvent> harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new LevelDiffFunction()),
                book -> book.getExchangeId() + "|" + book.getPairId(),
                Types.STRING);
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    @Test
    void firstBookEmitsEveryLevel() throws Exception {
        List<PriceLevelEvent> emitted = process(book(levels("10", "1", "11", "2"), levels("9", "3")));

        assertThat(emitted).hasSize(3);
        assertThat(described(emitted))
                .containsExactlyInAnyOrder("asks 10 1", "asks 11 2", "bids 9 3");
    }

    @Test
    void unchangedBookEmitsNothing() throws Exception {
        process(book(levels("10", "1"), levels("9", "3")));

        assertThat(process(book(levels("10", "1"), levels("9", "3")))).isEmpty();
    }

    @Test
    void addedLevelIsEmitted() throws Exception {
        process(book(levels("10", "1"), levels()));

        assertThat(described(process(book(levels("10", "1", "11", "2"), levels()))))
                .containsExactly("asks 11 2");
    }

    @Test
    void changedQuantityIsEmitted() throws Exception {
        process(book(levels("10", "1"), levels()));

        assertThat(described(process(book(levels("10", "5"), levels()))))
                .containsExactly("asks 10 5");
    }

    @Test
    void vanishedLevelIsEmittedAsZero() throws Exception {
        process(book(levels("10", "1", "11", "2"), levels()));

        assertThat(described(process(book(levels("10", "1"), levels()))))
                .containsExactly("asks 11 0");
    }

    @Test
    void emptiedSideDeletesEveryLevel() throws Exception {
        process(book(levels("10", "1", "11", "2"), levels("9", "3")));

        assertThat(described(process(book(levels(), levels("9", "3")))))
                .containsExactlyInAnyOrder("asks 10 0", "asks 11 0");
    }

    @Test
    void sidesAreDiffedIndependently() throws Exception {
        process(book(levels("10", "1"), levels("9", "3")));

        assertThat(described(process(book(levels("10", "1"), levels("9", "4")))))
                .containsExactly("bids 9 4");
    }

    @Test
    void deletedLevelIsNotReDeleted() throws Exception {
        process(book(levels("10", "1", "11", "2"), levels()));
        process(book(levels("10", "1"), levels()));

        assertThat(process(book(levels("10", "1"), levels()))).isEmpty();
    }

    @Test
    void reAddedLevelIsEmittedAgain() throws Exception {
        process(book(levels("10", "1"), levels()));
        process(book(levels(), levels()));

        assertThat(described(process(book(levels("10", "1"), levels()))))
                .containsExactly("asks 10 1");
    }

    @Test
    void emitsIdentityAndEventTime() throws Exception {
        PriceLevelEvent event = process(book(levels("10", "1"), levels())).get(0);

        assertThat(event.getExchangeId()).isEqualTo(8);
        assertThat(event.getPairId()).isEqualTo(1);
        assertThat(event.getEventTime()).isEqualTo(EVENT_TIME);
    }

    @Test
    void pricesAreCanonicalized() throws Exception {
        process(book(levels("10.50", "1"), levels()));

        // Same price, same size, only a different scale on the wire — nothing actually changed.
        assertThat(process(book(levels("10.5", "1.0"), levels()))).isEmpty();
    }

    @Test
    void stampsTimings() throws Exception {
        PriceLevelEvent event = process(book(levels("10", "1"), levels())).get(0);

        assertThat(event.getPipelineTimings().getLevelEmitIn()).isNotNull();
        assertThat(event.getPipelineTimings().getLevelEmitOut())
                .isGreaterThanOrEqualTo(event.getPipelineTimings().getLevelEmitIn());
    }

    @Test
    void booksAreIsolatedPerKey() throws Exception {
        process(book(8, 1, levels("10", "1"), levels()));

        // A different market's first book must emit its levels, not a diff against ex8's.
        assertThat(described(process(book(8, 2, levels("10", "1"), levels()))))
                .containsExactly("asks 10 1");
    }

    private List<PriceLevelEvent> process(OrderBookSnapshot book) throws Exception {
        harness.getOutput().clear();
        harness.processElement(book, 0L);
        return harness.extractOutputValues();
    }

    /** "asks 10 1" — the whole payload of an emitted event that a diff test cares about. */
    private static List<String> described(List<PriceLevelEvent> events) {
        return events.stream()
                .map(e -> e.getSide() + " " + e.getPrice() + " " + e.getQuantity())
                .toList();
    }

    private static OrderBookSnapshot book(List<PriceLevel> asks, List<PriceLevel> bids) {
        return book(8, 1, asks, bids);
    }

    private static OrderBookSnapshot book(int exchangeId, int pairId,
                                          List<PriceLevel> asks, List<PriceLevel> bids) {
        return new OrderBookSnapshot(exchangeId, pairId, EVENT_TIME, 1L, asks, bids);
    }

    private static List<PriceLevel> levels(String... priceQuantityPairs) {
        List<PriceLevel> levels = new ArrayList<>();
        for (int i = 0; i < priceQuantityPairs.length; i += 2) {
            levels.add(new PriceLevel(priceQuantityPairs[i], priceQuantityPairs[i + 1]));
        }
        return levels;
    }
}
