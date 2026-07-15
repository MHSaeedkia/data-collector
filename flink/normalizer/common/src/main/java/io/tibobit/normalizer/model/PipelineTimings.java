package io.tibobit.normalizer.model;

/**
 * Per-step latency timings carried on every pipeline event (schema field {@code pipeline_timings},
 * record {@code PipelineTimings} — see memory/project_raw_pipeline_decision.md). Each of the six
 * jobs fills ONLY its own two fields: {@code _in} when it reads the event off its input topic,
 * {@code _out} just before it emits. A {@code null} field means the event has not yet reached that
 * stage. Every value is epoch milliseconds (timestamp-millis on the wire).
 *
 * <p>Not an array/map on purpose: the pipeline is a fixed 6 steps, so the stages are named fields —
 * unambiguous and directly queryable. Derived deltas (all computed by consumers, never stored):
 * exchange&rarr;pipeline lag = {@code pairExtractIn - eventTime}; in-job time = {@code _out - _in};
 * Kafka transit = {@code nextStageIn - prevStageOut}; total end-to-end =
 * {@code levelEmitOut - eventTime}.
 */
public class PipelineTimings {

    private Long pairExtractIn;
    private Long pairExtractOut;
    private Long typeValidateIn;
    private Long typeValidateOut;
    private Long rebaseIn;
    private Long rebaseOut;
    private Long precisionIn;
    private Long precisionOut;
    private Long bookBuildIn;
    private Long bookBuildOut;
    private Long levelEmitIn;
    private Long levelEmitOut;

    public PipelineTimings() {
    }

    public Long getPairExtractIn() {
        return pairExtractIn;
    }

    public void setPairExtractIn(Long pairExtractIn) {
        this.pairExtractIn = pairExtractIn;
    }

    public Long getPairExtractOut() {
        return pairExtractOut;
    }

    public void setPairExtractOut(Long pairExtractOut) {
        this.pairExtractOut = pairExtractOut;
    }

    public Long getTypeValidateIn() {
        return typeValidateIn;
    }

    public void setTypeValidateIn(Long typeValidateIn) {
        this.typeValidateIn = typeValidateIn;
    }

    public Long getTypeValidateOut() {
        return typeValidateOut;
    }

    public void setTypeValidateOut(Long typeValidateOut) {
        this.typeValidateOut = typeValidateOut;
    }

    public Long getRebaseIn() {
        return rebaseIn;
    }

    public void setRebaseIn(Long rebaseIn) {
        this.rebaseIn = rebaseIn;
    }

    public Long getRebaseOut() {
        return rebaseOut;
    }

    public void setRebaseOut(Long rebaseOut) {
        this.rebaseOut = rebaseOut;
    }

    public Long getPrecisionIn() {
        return precisionIn;
    }

    public void setPrecisionIn(Long precisionIn) {
        this.precisionIn = precisionIn;
    }

    public Long getPrecisionOut() {
        return precisionOut;
    }

    public void setPrecisionOut(Long precisionOut) {
        this.precisionOut = precisionOut;
    }

    public Long getBookBuildIn() {
        return bookBuildIn;
    }

    public void setBookBuildIn(Long bookBuildIn) {
        this.bookBuildIn = bookBuildIn;
    }

    public Long getBookBuildOut() {
        return bookBuildOut;
    }

    public void setBookBuildOut(Long bookBuildOut) {
        this.bookBuildOut = bookBuildOut;
    }

    public Long getLevelEmitIn() {
        return levelEmitIn;
    }

    public void setLevelEmitIn(Long levelEmitIn) {
        this.levelEmitIn = levelEmitIn;
    }

    public Long getLevelEmitOut() {
        return levelEmitOut;
    }

    public void setLevelEmitOut(Long levelEmitOut) {
        this.levelEmitOut = levelEmitOut;
    }
}
