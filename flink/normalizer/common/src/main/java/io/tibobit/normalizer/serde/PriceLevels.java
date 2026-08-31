package io.tibobit.normalizer.serde;

import io.tibobit.normalizer.model.PriceLevel;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared PriceLevel list mapping used by every serde in this package — the PriceLevel record is
 * defined identically in all three raw-pipeline schemas. Null-passthrough matters: on
 * RawOrderBookEvent a null side means "side absent from this event" and must survive the
 * mapping distinct from an empty array ("exchange reported side empty").
 */
final class PriceLevels {

    private PriceLevels() {
    }

    /** Unwraps the array element schema from either a plain array field or a ["null", array] union. */
    static Schema elementType(Schema fieldSchema) {
        if (fieldSchema.getType() == Schema.Type.UNION) {
            for (Schema branch : fieldSchema.getTypes()) {
                if (branch.getType() == Schema.Type.ARRAY) {
                    return branch.getElementType();
                }
            }
            throw new IllegalArgumentException("No array branch in union schema: " + fieldSchema);
        }
        return fieldSchema.getElementType();
    }

    static List<GenericRecord> toRecords(List<PriceLevel> levels, Schema levelSchema) {
        if (levels == null) {
            return null;
        }
        List<GenericRecord> records = new ArrayList<>(levels.size());
        for (PriceLevel level : levels) {
            records.add(new GenericRecordBuilder(levelSchema)
                    .set("price", level.getPrice())
                    .set("quantity", level.getQuantity())
                    .build());
        }
        return records;
    }

    static List<PriceLevel> fromRecords(Object avroArray) {
        if (avroArray == null) {
            return null;
        }
        List<?> records = (List<?>) avroArray;
        List<PriceLevel> levels = new ArrayList<>(records.size());
        for (Object record : records) {
            GenericRecord level = (GenericRecord) record;
            levels.add(new PriceLevel(level.get("price").toString(), level.get("quantity").toString()));
        }
        return levels;
    }
}
