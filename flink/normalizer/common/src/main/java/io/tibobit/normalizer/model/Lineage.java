package io.tibobit.normalizer.model;

import java.util.List;
import java.util.UUID;

/**
 * Record lineage: who wrote a record ({@code id}) and which records it was made from
 * ({@code source_ids}). Every write to a Kafka topic mints a fresh id, so a record crossing the
 * whole pipeline carries seven different ones — NiFi's, then one per job. {@code source_ids} holds
 * IMMEDIATE parents only, never the accumulated chain: the path is reconstructed by walking hop to
 * hop, which keeps the list at one element everywhere except the genuine fan-in in job 5.
 *
 * <p>See memory/project_record_lineage.md for the full model and the reasoning behind it.
 */
public final class Lineage {

    private Lineage() {
    }

    public static String newId() {
        return UUID.randomUUID().toString();
    }

    /**
     * Re-stamps an event that is being forwarded to the next topic: the id it arrived with becomes
     * its source, and it gets a fresh one of its own.
     *
     * <p>This exists as a helper for one reason — the two assignments are order-dependent, and doing
     * them the other way round makes an event its own parent. That failure is invisible: the record
     * still has a well-formed uuid in both fields and every test that only checks "is it set" passes.
     * Call this instead of writing the pair by hand.
     */
    public static void restamp(RawOrderBookEvent event) {
        event.setSourceIds(List.of(event.getId()));
        event.setId(newId());
    }
}
