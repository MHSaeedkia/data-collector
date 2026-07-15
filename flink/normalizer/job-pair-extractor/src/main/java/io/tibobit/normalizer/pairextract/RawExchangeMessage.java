package io.tibobit.normalizer.pairextract;

/**
 * One verbatim Kafka record from an {@code ex{id}-raw} topic, with the exchange id parsed
 * from the topic name (the payload itself doesn't say which exchange it came from).
 */
public class RawExchangeMessage {

    private int exchangeId;
    private byte[] payload;

    public RawExchangeMessage() {
    }

    public RawExchangeMessage(int exchangeId, byte[] payload) {
        this.exchangeId = exchangeId;
        this.payload = payload;
    }

    public int getExchangeId() {
        return exchangeId;
    }

    public void setExchangeId(int exchangeId) {
        this.exchangeId = exchangeId;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }
}
