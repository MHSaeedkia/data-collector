package io.tibobit.normalizer.model;

import java.io.Serializable;

/**
 * A control-plane command sent to NiFi, independent of the data-plane pipeline. Wire shape:
 * {"action": "snapshot_request", "payload": {"pair_id": N, "exchange_id": N}}.
 */
public class ControlCommand implements Serializable {

    public static final String SNAPSHOT_REQUEST = "snapshot_request";

    private final String action;
    private final int exchangeId;
    private final int pairId;

    public ControlCommand(String action, int exchangeId, int pairId) {
        this.action = action;
        this.exchangeId = exchangeId;
        this.pairId = pairId;
    }

    public String getAction() {
        return action;
    }

    public int getExchangeId() {
        return exchangeId;
    }

    public int getPairId() {
        return pairId;
    }

    @Override
    public String toString() {
        return action + "(exchange=" + exchangeId + ", pair=" + pairId + ")";
    }
}
