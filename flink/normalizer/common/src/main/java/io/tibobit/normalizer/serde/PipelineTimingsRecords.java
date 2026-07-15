package io.tibobit.normalizer.serde;

import io.tibobit.normalizer.model.PipelineTimings;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

/**
 * Maps {@link PipelineTimings} to/from the nested {@code pipeline_timings} Avro record, shared by
 * every raw-pipeline serde (the record is defined identically in all four schemas, same rule as
 * PriceLevel). The wire field is a {@code ["null", PipelineTimings]} union; writers always emit a
 * non-null record (per-stage fields null until that stage runs), so the null branch only appears
 * for data written before the field existed — {@link #fromRecord} maps that to an empty timings.
 */
final class PipelineTimingsRecords {

    private PipelineTimingsRecords() {
    }

    /** Unwraps the PipelineTimings record schema from the ["null", record] union field. */
    static Schema recordType(Schema fieldSchema) {
        if (fieldSchema.getType() == Schema.Type.UNION) {
            for (Schema branch : fieldSchema.getTypes()) {
                if (branch.getType() == Schema.Type.RECORD) {
                    return branch;
                }
            }
            throw new IllegalArgumentException("No record branch in union schema: " + fieldSchema);
        }
        return fieldSchema;
    }

    static GenericRecord toRecord(PipelineTimings timings, Schema fieldSchema) {
        PipelineTimings t = timings != null ? timings : new PipelineTimings();
        return new GenericRecordBuilder(recordType(fieldSchema))
                .set("pair_extract_in", t.getPairExtractIn())
                .set("pair_extract_out", t.getPairExtractOut())
                .set("type_validate_in", t.getTypeValidateIn())
                .set("type_validate_out", t.getTypeValidateOut())
                .set("rebase_in", t.getRebaseIn())
                .set("rebase_out", t.getRebaseOut())
                .set("precision_in", t.getPrecisionIn())
                .set("precision_out", t.getPrecisionOut())
                .set("book_build_in", t.getBookBuildIn())
                .set("book_build_out", t.getBookBuildOut())
                .set("level_emit_in", t.getLevelEmitIn())
                .set("level_emit_out", t.getLevelEmitOut())
                .build();
    }

    static PipelineTimings fromRecord(Object avroRecord) {
        PipelineTimings t = new PipelineTimings();
        if (avroRecord == null) {
            return t;
        }
        GenericRecord r = (GenericRecord) avroRecord;
        t.setPairExtractIn((Long) r.get("pair_extract_in"));
        t.setPairExtractOut((Long) r.get("pair_extract_out"));
        t.setTypeValidateIn((Long) r.get("type_validate_in"));
        t.setTypeValidateOut((Long) r.get("type_validate_out"));
        t.setRebaseIn((Long) r.get("rebase_in"));
        t.setRebaseOut((Long) r.get("rebase_out"));
        t.setPrecisionIn((Long) r.get("precision_in"));
        t.setPrecisionOut((Long) r.get("precision_out"));
        t.setBookBuildIn((Long) r.get("book_build_in"));
        t.setBookBuildOut((Long) r.get("book_build_out"));
        t.setLevelEmitIn((Long) r.get("level_emit_in"));
        t.setLevelEmitOut((Long) r.get("level_emit_out"));
        return t;
    }
}
