package io.tibobit.normalizer.model;

import java.io.Serializable;
import java.util.List;

/**
 * A control-plane command sent to NiFi, independent of the data-plane pipeline. Wire shape:
 * {"action": "snapshot_request", "payload": {"pair_id": N, "exchange_id": N}}.
 */
public class ControlCommand implements Serializable {

    public static final String SNAPSHOT_REQUEST = "snapshot_request";

    private final String action;
    private final int exchangeId;
    private final int pairId;
    private final int simulationId;
    private final String id;
   private List<String> sourceIds = List.of();

    public ControlCommand(String action, int exchangeId, int pairId, int simulationId, String id,List<String> sourceIds ) {
        this.action = action;
        this.exchangeId = exchangeId;
        this.pairId = pairId;
        this.simulationId=simulationId;
        this.id=id;
        this.sourceIds=sourceIds;
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

    public int getSimulationId() {
        return simulationId;
    }

    public String getId() {
        return id;
    }


    public List<String> getSourceIds() {
        return sourceIds;
    }

    @Override
    public String toString() {
        return action + "(exchange=" + exchangeId + ", pair=" + pairId + ", simulation=" + simulationId +")";
    }
}
