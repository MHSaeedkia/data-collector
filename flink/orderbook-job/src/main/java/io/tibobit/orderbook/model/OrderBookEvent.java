package io.tibobit.orderbook.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Input event for a single pair+side from one exchange, as read from the
 * {side}-p{pair_id}-ex{exchange_id} Kafka topics. Carries either a full {@code snapshot}
 * or an incremental {@code update} (see {@code type}); the merger maintains a running book
 * per exchange from these (see OrderBookMerger). Each {@code update} is validated for
 * continuity via {@code sequence_id} and {@code sequence_jump} — the expected delta from the
 * previous message (exchange ids do NOT increment by 1, they can jump), so an update is in
 * order iff {@code sequence_id == lastSeq + sequence_jump}. A {@code snapshot} is the baseline
 * and carries no jump ({@code sequence_jump == 0}).
 *
 * The wire event (see schemas/orderbook_event.avsc) also carries exchange_name, base and
 * quote; Flink works only with the IDs, so those extra fields are ignored on deserialization.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderBookEvent {

    @JsonProperty("exchange_id")
    private int exchangeId;

    @JsonProperty("pair_id")
    private int pairId;

    private String side;
    private OrderBookEventType type;

    @JsonProperty("event_time")
    private long eventTime;

    @JsonProperty("sequence_id")
    private long sequenceId;

    @JsonProperty("sequence_jump")
    private long sequenceJump;

    private List<PriceLevel> levels;

    public OrderBookEvent() {
    }

    public int getExchangeId() {
        return exchangeId;
    }

    public void setExchangeId(int exchangeId) {
        this.exchangeId = exchangeId;
    }

    public int getPairId() {
        return pairId;
    }

    public void setPairId(int pairId) {
        this.pairId = pairId;
    }

    public String getSide() {
        return side;
    }

    public void setSide(String side) {
        this.side = side;
    }

    public OrderBookEventType getType() {
        return type;
    }

    public void setType(OrderBookEventType type) {
        this.type = type;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public long getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(long sequenceId) {
        this.sequenceId = sequenceId;
    }

    public long getSequenceJump() {
        return sequenceJump;
    }

    public void setSequenceJump(long sequenceJump) {
        this.sequenceJump = sequenceJump;
    }

    public List<PriceLevel> getLevels() {
        return levels;
    }

    public void setLevels(List<PriceLevel> levels) {
        this.levels = levels;
    }
}
