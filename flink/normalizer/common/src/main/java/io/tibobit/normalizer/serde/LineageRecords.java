package io.tibobit.normalizer.serde;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Shared decoding of the {@code sink_id}/{@code source_ids} lineage fields, used by every
 * deserializer in this package. Encoding needs no helper — a {@code String} and a
 * {@code List<String>} go straight into a {@code GenericRecordBuilder}.
 *
 * <p>The reason this exists at all: Avro hands back {@link org.apache.avro.util.Utf8}, not
 * {@link String}. A Utf8 kept as-is compares unequal to any String, so a lineage id would fail every
 * downstream lookup and every test assertion while still printing correctly in a log — the kind of
 * bug that reads as "the ids don't match" with nothing visibly wrong. Convert at the boundary.
 */
final class LineageRecords {

    private LineageRecords() {
    }

    /** Null only for records written before the field existed; those read as the empty default. */
    static String sinkId(Object value) {
        return value == null ? "" : value.toString();
    }

    static List<String> sourceIds(Object value) {
        if (!(value instanceof Collection<?> raw)) {
            return List.of();
        }
        List<String> ids = new ArrayList<>(raw.size());
        for (Object id : raw) {
            ids.add(id.toString());
        }
        return ids;
    }
}
