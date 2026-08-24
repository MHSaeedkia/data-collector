package io.tibobit.adjustment;

import java.util.ArrayList;
import java.util.List;

/**
 * The book as it flows through the adjustment chain and as it lands on
 * {@code p{pair_id}-{side}-adjusted}. Mirrors schemas/adjusted_order_book_event.avsc field for
 * field.
 *
 * <p>It carries the three rates as well as the adjusted prices, because seeing what was DONE to a
 * price is the point of publishing it: a bare adjusted number is unauditable. Each stage fills in
 * its own rate as it applies it, so the record and the arithmetic can never disagree — there is no
 * second place holding "what we said we charged".
 *
 * <p>Rates are percent strings ({@code "0.35"} = 0.35%), not doubles, for the same reason prices
 * are strings: they feed exact BigDecimal arithmetic (memory/project_bigdecimal_rules.md).
 */
public class AdjustedOrderBook {

    private int pairId;
    private String side;
    private String id = "";
    private long eventTime;
    private String buySellCommissionPercent = "0";
    private String ourProfitPercent = "0";
    private String slippagePercent = "0";
    private List<AdjustedLevel> levels;

    public AdjustedOrderBook() {
    }

    public AdjustedOrderBook(int pairId, String side, String id, long eventTime, List<AdjustedLevel> levels) {
        this.pairId = pairId;
        this.side = side;
        this.id = id;
        this.eventTime = eventTime;
        this.levels = levels;
    }

    /**
     * Entry point of the chain: job 6's aggregated record with nothing adjusted yet — same prices,
     * every rate still "0". The levels are COPIED rather than aliased, so the stages can mutate in
     * place without writing through to the record the deserializer produced.
     */
    public static AdjustedOrderBook from(AggregatedOrderBook aggregated) {
        List<AggregatedLevel> source =
                aggregated.getLevels() == null ? List.of() : aggregated.getLevels();
        List<AdjustedLevel> levels = new ArrayList<>(source.size());
        for (AggregatedLevel level : source) {
            levels.add(new AdjustedLevel(
                    level.getExchangeId(),
                    level.getSimulation(),
                    level.getSourceId(),
                    level.getPrice(),
                    level.getQuantity()));
        }
        return new AdjustedOrderBook(
                aggregated.getPairId(),
                aggregated.getSide(),
                aggregated.getId(),
                aggregated.getEventTime(),
                levels);
    }

    public int getPairId() { return pairId; }
    public void setPairId(int pairId) { this.pairId = pairId; }

    public String getSide() { return side; }
    public void setSide(String side) { this.side = side; }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public long getEventTime() { return eventTime; }
    public void setEventTime(long eventTime) { this.eventTime = eventTime; }

    public String getBuySellCommissionPercent() { return buySellCommissionPercent; }
    public void setBuySellCommissionPercent(String v) { this.buySellCommissionPercent = v; }

    public String getOurProfitPercent() { return ourProfitPercent; }
    public void setOurProfitPercent(String v) { this.ourProfitPercent = v; }

    public String getSlippagePercent() { return slippagePercent; }
    public void setSlippagePercent(String v) { this.slippagePercent = v; }

    public List<AdjustedLevel> getLevels() { return levels; }
    public void setLevels(List<AdjustedLevel> levels) { this.levels = levels; }

    @Override
    public String toString() {
        return "p" + pairId + " " + side
                + " (commission " + buySellCommissionPercent + "%, profit " + ourProfitPercent
                + "%, slippage " + slippagePercent + "%) " + levels;
    }
}
