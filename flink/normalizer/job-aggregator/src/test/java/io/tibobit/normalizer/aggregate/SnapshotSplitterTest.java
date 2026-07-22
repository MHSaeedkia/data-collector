package io.tibobit.normalizer.aggregate;

import io.tibobit.normalizer.model.OrderBookSnapshot;
import io.tibobit.normalizer.model.PriceLevel;

import org.apache.flink.util.Collector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/**
 * Tests {@link SnapshotSplitter}: each job-5 {@link OrderBookSnapshot} becomes exactly two per-side
 * {@link ExchangeBook}s (asks, bids), levels stamped with the snapshot's exchange_id, and an empty
 * book (job 5's reset) yields two empty ExchangeBooks so the exchange drops out downstream.
 */
class SnapshotSplitterTest {

    private final SnapshotSplitter splitter = new SnapshotSplitter();

    private List<ExchangeBook> split(OrderBookSnapshot snapshot) {
        List<ExchangeBook> out = new ArrayList<>();
        splitter.flatMap(snapshot, new ListCollector(out));
        return out;
    }

    private static List<PriceLevel> levels(String... priceQty) {
        List<PriceLevel> out = new ArrayList<>();
        for (int i = 0; i < priceQty.length; i += 2) {
            out.add(new PriceLevel(priceQty[i], priceQty[i + 1]));
        }
        return out;
    }

    @Test
    @DisplayName("a two-sided snapshot splits into an asks book and a bids book, stamped with exchange_id")
    void splitsIntoTwoSides() {
        OrderBookSnapshot snapshot = new OrderBookSnapshot(
                8, 1, 1_700_000_000_000L, 42L,
                levels("100", "1", "101", "2"), levels("99", "3"));

        List<ExchangeBook> out = split(snapshot);

        assertThat(out).extracting(ExchangeBook::getSide).containsExactly("asks", "bids");
        assertThat(out).allSatisfy(b -> {
            assertThat(b.getExchangeId()).isEqualTo(8);
            assertThat(b.getPairId()).isEqualTo(1);
            assertThat(b.getEventTime()).isEqualTo(1_700_000_000_000L);
        });

        ExchangeBook asks = out.get(0);
        assertThat(asks.getLevels())
                .extracting(AggregatedLevel::getExchangeId,
                        AggregatedLevel::getPrice, AggregatedLevel::getQuantity)
                .containsExactly(tuple(8, "100", "1"), tuple(8, "101", "2"));

        ExchangeBook bids = out.get(1);
        assertThat(bids.getLevels())
                .extracting(AggregatedLevel::getPrice).containsExactly("99");
    }

    @Test
    @DisplayName("an empty book (reset) yields two empty ExchangeBooks so the exchange drops out")
    void emptyBookYieldsEmptySides() {
        OrderBookSnapshot reset = new OrderBookSnapshot(
                8, 1, 1_700_000_000_500L, null, List.of(), List.of());

        List<ExchangeBook> out = split(reset);

        assertThat(out).hasSize(2);
        assertThat(out).allSatisfy(b -> assertThat(b.getLevels()).isEmpty());
    }

    @Test
    @DisplayName("a null side (defensive — job 5 emits both) is treated as empty, never NPEs")
    void nullSideTreatedAsEmpty() {
        OrderBookSnapshot snapshot = new OrderBookSnapshot(
                3, 1, 1_700_000_000_000L, null, levels("100", "1"), null);

        List<ExchangeBook> out = split(snapshot);

        assertThat(out.get(0).getLevels()).hasSize(1);
        assertThat(out.get(1).getLevels()).isEmpty();
    }

    /** Minimal Collector that appends to a list — the splitter uses only collect(). */
    private static final class ListCollector implements Collector<ExchangeBook> {
        private final List<ExchangeBook> out;

        ListCollector(List<ExchangeBook> out) {
            this.out = out;
        }

        @Override
        public void collect(ExchangeBook record) {
            out.add(record);
        }

        @Override
        public void close() {
        }
    }
}
