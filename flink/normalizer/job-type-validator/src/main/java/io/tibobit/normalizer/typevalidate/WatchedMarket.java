package io.tibobit.normalizer.typevalidate;

import java.io.Serializable;

/**
 * One subscribed market and the silence budget from its own
 * {@code exchange_markets} row. Reference data, not a stream record: it is
 * loaded from Postgres into a
 * {@link io.tibobit.normalizer.lookup.RefreshingLookup} and read by key, so a
 * threshold edit or an unsubscribe lands on the next refresh without
 * resubmitting the job.
 *
 * <p>
 * It carries the exchange and pair ids as well as the threshold, even though
 * the map is already keyed by them, so that the silence path can name the
 * market it is asking about <b>without parsing the key string</b>. Parsing the
 * key was one of the three specific defects that sank the first timer-based
 * control plane: it coupled the operator to the key format and turned a wrong
 * key selector into a crash rather than a bad record.
 */
public class WatchedMarket implements Serializable {

    private final int exchangeId;
    private final int pairId;
    private final int thresholdSeconds;

    public WatchedMarket(int exchangeId, int pairId, int thresholdSeconds) {
        this.exchangeId = exchangeId;
        this.pairId = pairId;
        this.thresholdSeconds = thresholdSeconds;
    }

    /**
     * The lookup key — must match the job's keyBy on (exchange_id, pair_id).
     */
    public String key() {
        return exchangeId + "|" + pairId;
    }

    public int getExchangeId() {
        return exchangeId;
    }

    public int getPairId() {
        return pairId;
    }

    public long thresholdMs() {
        return thresholdSeconds * 1000L;
    }

    @Override
    public String toString() {
        return "WatchedMarket{" + key() + ", thresholdSeconds=" + thresholdSeconds + "}";
    }
}
