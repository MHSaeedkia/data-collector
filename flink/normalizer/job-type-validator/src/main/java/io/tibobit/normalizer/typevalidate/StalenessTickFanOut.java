package io.tibobit.normalizer.typevalidate;

import io.tibobit.normalizer.lookup.RefreshingLookup;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Turns one bare pulse from the generator source into one {@link StalenessTick}
 * per subscribed market. The generator supplies the CLOCK (how often to look)
 * and this supplies the WATCH LIST (what to look at) — separated because the
 * watch list comes from Postgres and changes while the job runs, whereas the
 * clock is fixed at submit time.
 *
 * <p>
 * Parallelism must stay 1. Every parallel instance would hold the same full
 * watch list and emit the same ticks, multiplying the tick rate per market by
 * the parallelism and, with it, the rate at which a stale market re-asks for a
 * snapshot.
 */
public class StalenessTickFanOut extends RichFlatMapFunction<Long, StalenessTick> {

    private final RefreshingLookup<String, StalenessTick> watchList;

    public StalenessTickFanOut(RefreshingLookup<String, StalenessTick> watchList) {
        this.watchList = watchList;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        // Propagates: a job that cannot read its watch list must not start up silently
        // watching nothing, because "watching nothing" and "everything is healthy" look
        // identical from outside.
        watchList.open();
    }

    @Override
    public void flatMap(Long pulse, Collector<StalenessTick> out) {
        for (StalenessTick watched : watchList.snapshot().values()) {
            // A defensive copy: the lookup's snapshot is shared with the next pulse and
            // with the refresh thread, and a POJO handed to Flink must not be aliased by
            // reference data that outlives the record.
            out.collect(new StalenessTick(watched));
        }
    }

    @Override
    public void close() {
        watchList.close();
    }
}
