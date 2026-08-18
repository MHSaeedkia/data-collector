package io.tibobit.normalizer.model;

import java.io.Serializable;
import java.util.List;

/**
 * A control-plane command sent to NiFi, independent of the data-plane pipeline — the one record
 * that travels against the flow, asking the collector to re-send a snapshot for a market job 2
 * can no longer track. Encoded as Avro on subject {@code control-command} (see
 * {@link io.tibobit.normalizer.serde.ControlCommandSerializer}).
 *
 * <p>{@code simulation} is carried from the event that triggered the request, so a gap in
 * simulated data cannot make NiFi call a real exchange. {@code id}/{@code sourceIds} are the
 * usual lineage pair and are DERIVED, not inherited: this is a write to a topic, so it mints its
 * own id and names the triggering event as its parent (see {@link Lineage}).
 */
public class ControlCommand implements Serializable {

    public static final String SNAPSHOT_REQUEST = "snapshot_request";

    private final String action;
    private final int exchangeId;
    private final int pairId;
    private final int simulation;
    private final String id;
    private final List<String> sourceIds;

    public ControlCommand(String action, int exchangeId, int pairId, int simulation,
                          String id, List<String> sourceIds) {
        this.action = action;
        this.exchangeId = exchangeId;
        this.pairId = pairId;
        this.simulation = simulation;
        this.id = id;
        this.sourceIds = sourceIds == null ? List.of() : sourceIds;
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

    public int getSimulation() {
        return simulation;
    }

    public String getId() {
        return id;
    }

    public List<String> getSourceIds() {
        return sourceIds;
    }

    @Override
    public String toString() {
        return action + "(exchange=" + exchangeId + ", pair=" + pairId
                + ", simulation=" + simulation + ")";
    }
}
