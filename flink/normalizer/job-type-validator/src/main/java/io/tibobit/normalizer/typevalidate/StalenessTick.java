package io.tibobit.normalizer.typevalidate;

/**
 * One "is this market still speaking?" prompt for a single subscribed market,
 * carrying that market's own threshold straight from its {@code
 * exchange_markets} row.
 *
 * <p>
 * Ticks are what make staleness detection possible at all in a keyed function.
 * A market that has never sent a byte has no keyed state and no Flink timer can
 * fire for it, because there is no key — the key only comes into existence when
 * something arrives for it. A tick arrives for EVERY subscribed market whether
 * or not the exchange is alive, so the key exists either way, and "this pair has
 * never spoken" becomes an observable condition instead of an invisible one.
 *
 * <p>
 * A tick is also deliberately not a timer. Timers were tried for the retry side
 * of the control plane and removed (see {@link TypeValidateFunction#askForSnapshot}):
 * they are registered per episode and cancelled nowhere, so overlapping episodes
 * leave several live chains each re-arming forever. A tick has no such lifecycle
 * — it arrives, it is handled, it is gone — so the bug has nowhere to live.
 *
 * <p>
 * A Flink POJO on purpose (public no-arg constructor, getters and setters), so
 * the POJO serializer is used rather than the generic Kryo fallback.
 */
public class StalenessTick {

    private int exchangeId;
    private int pairId;

    /**
     * How long this market may stay silent before its book is considered stale,
     * from {@code exchange_markets.staleness_threshold_seconds}. Carried on the
     * tick rather than looked up in the operator so an edit in Postgres takes
     * effect on the next poll without resubmitting the job, and so the operator
     * needs no DB dependency of its own.
     */
    private int thresholdSeconds;

    public StalenessTick() {
    }

    public StalenessTick(int exchangeId, int pairId, int thresholdSeconds) {
        this.exchangeId = exchangeId;
        this.pairId = pairId;
        this.thresholdSeconds = thresholdSeconds;
    }

    public StalenessTick(StalenessTick other) {
        this(other.exchangeId, other.pairId, other.thresholdSeconds);
    }

    /** The key both streams are partitioned by — must match TypeValidatorJob's ExchangePairKey. */
    public String key() {
        return exchangeId + "|" + pairId;
    }

    public long thresholdMs() {
        return thresholdSeconds * 1000L;
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

    public int getThresholdSeconds() {
        return thresholdSeconds;
    }

    public void setThresholdSeconds(int thresholdSeconds) {
        this.thresholdSeconds = thresholdSeconds;
    }

    @Override
    public String toString() {
        return "StalenessTick{" + key() + ", thresholdSeconds=" + thresholdSeconds + "}";
    }
}
