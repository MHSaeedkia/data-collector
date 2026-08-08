package io.tibobit.normalizer.serde;

import io.tibobit.normalizer.model.PriceLevel;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared PriceLevel list mapping used by every serde in this package — the PriceLevel record is
 * defined the same way in all three raw-pipeline schemas apart from {@code source_id}, which only
 * order_book_snapshot.avsc declares. That one field is therefore driven off the SCHEMA rather than
 * off the value: a GenericRecordBuilder throws on a field its schema does not have, so writing it
 * unconditionally would break the raw and rejected sinks. Null-passthrough matters: on
 * RawOrderBookEvent a null side means "side absent from this event" and must survive the
 * mapping distinct from an empty array ("exchange reported side empty").
 */
final class PriceLevels {

    /** Declared only by order_book_snapshot.avsc — see the class doc. */
    private static final String SOURCE_ID = "source_id";

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
        boolean carriesSource = levelSchema.getField(SOURCE_ID) != null;
        List<GenericRecord> records = new ArrayList<>(levels.size());
        for (PriceLevel level : levels) {
            GenericRecordBuilder builder = new GenericRecordBuilder(levelSchema)
                    .set("price", level.getPrice())
                    .set("quantity", level.getQuantity());
            if (carriesSource) {
                // Defaulted rather than left unset: the field is non-null in the schema, and an
                // unstamped level is a bug worth seeing as "" instead of a serializer exception.
                builder.set(SOURCE_ID, level.getSourceId() == null ? "" : level.getSourceId());
            }
            records.add(builder.build());
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
            // Avro hands strings back as Utf8, so every read has to be toString()'d — an
            // unconverted source_id would compare unequal to the id it came from while printing
            // identically. Same trap as LineageRecords guards on the record-level fields.
            Object sourceId = level.getSchema().getField(SOURCE_ID) == null ? null : level.get(SOURCE_ID);
            levels.add(new PriceLevel(
                    level.get("price").toString(),
                    level.get("quantity").toString(),
                    sourceId == null ? null : sourceId.toString()));
        }
        return levels;
    }
}
