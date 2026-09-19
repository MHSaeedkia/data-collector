package io.tibobit.normalizer.pairextract;

import io.tibobit.normalizer.lookup.RefreshingLookup;
import io.tibobit.normalizer.model.Lineage;
import io.tibobit.normalizer.model.ParsedBookEvent;
import io.tibobit.normalizer.model.PipelineTimings;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * The pair-extract step: resolve the exchange's own market string to pair_id, stamp it, emit.
 * One drop rule, counted, not dead-lettered (dead-letter is job 3's validation concern):
 *
 * <ul>
 *   <li>unknown market string → WARN + drop (new pair not yet in exchange_markets)</li>
 * </ul>
 *
 * <p>The parse-side drops (no parser, unparseable frame, no id on the payload) moved to
 * {@code ParseFunction} with the parsers on 2026-09-19 — see its javadoc, which also carries the
 * "NiFi is a hard dependency" decision. Nothing reaches this operator that did not already parse
 * cleanly and carry a lineage id.
 */
public class PairExtractFunction extends RichFlatMapFunction<ParsedBookEvent, RawOrderBookEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(PairExtractFunction.class);

    private final RefreshingLookup<String, Integer> markets;

    private transient Counter droppedUnknownMarket;

    public PairExtractFunction(RefreshingLookup<String, Integer> markets) {
        this.markets = markets;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        markets.open();
        droppedUnknownMarket = getRuntimeContext().getMetricGroup().counter("dropped-unknown-market");
    }

    @Override
    public void flatMap(ParsedBookEvent parsed, Collector<RawOrderBookEvent> out) {
        long ingestTime = System.currentTimeMillis();
        RawOrderBookEvent event = parsed.getEvent();
        int exchangeId = event.getExchangeId();
        Integer pairId = markets.get(ExchangeMarketsLoader.key(exchangeId, parsed.getMarket()));
        if (pairId == null) {
            LOG.warn("Unknown market '{}' for exchange {} — dropping (not in exchange_markets)",
                    parsed.getMarket(), exchangeId);
            droppedUnknownMarket.inc();
            return;
        }
        event.setPairId(pairId);
        // Read the parent id BEFORE overwriting the field: this job writes the record, so it mints
        // a fresh id and names the parsed record it came from.
        event.setSourceIds(List.of(event.getId()));
        event.setId(Lineage.newId());
        PipelineTimings timings = event.getPipelineTimings();
        timings.setPairExtractIn(ingestTime);
        timings.setPairExtractOut(System.currentTimeMillis());
        out.collect(event);
    }

    @Override
    public void close() {
        markets.close();
    }
}
