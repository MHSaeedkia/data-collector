package io.tibobit.normalizer.pairextract;

import io.tibobit.normalizer.lookup.RefreshingLookup;
import io.tibobit.normalizer.model.PipelineTimings;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import io.tibobit.normalizer.pairextract.parser.ParsedBookEvent;
import io.tibobit.normalizer.pairextract.parser.RawExchangeParser;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * The pair-extract step: pick the exchange's parser, parse the verbatim payload, resolve
 * the market string to pair_id, stamp exchange_id/pair_id, emit. Drop rules (all counted,
 * none dead-lettered — dead-letter is job 2's validation concern):
 *
 * <ul>
 *   <li>no parser for the exchange (postponed ex7, future topics) → drop</li>
 *   <li>unrecognized/malformed frame (whitelist rule) → drop, never crash</li>
 *   <li>unknown market string → WARN + drop (new pair not yet in exchange_markets)</li>
 * </ul>
 */
public class PairExtractFunction extends RichFlatMapFunction<RawExchangeMessage, RawOrderBookEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(PairExtractFunction.class);

    private final Map<Integer, RawExchangeParser> parsers;
    private final RefreshingLookup<String, Integer> markets;

    private transient Counter droppedNoParser;
    private transient Counter droppedUnparseable;
    private transient Counter droppedUnknownMarket;

    public PairExtractFunction(Map<Integer, RawExchangeParser> parsers,
                               RefreshingLookup<String, Integer> markets) {
        this.parsers = parsers;
        this.markets = markets;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        markets.open();
        droppedNoParser = getRuntimeContext().getMetricGroup().counter("dropped-no-parser");
        droppedUnparseable = getRuntimeContext().getMetricGroup().counter("dropped-unparseable");
        droppedUnknownMarket = getRuntimeContext().getMetricGroup().counter("dropped-unknown-market");
    }

    @Override
    public void flatMap(RawExchangeMessage message, Collector<RawOrderBookEvent> out) {
        long ingestTime = System.currentTimeMillis();
        RawExchangeParser parser = parsers.get(message.getExchangeId());
        if (parser == null) {
            droppedNoParser.inc();
            return;
        }
        List<ParsedBookEvent> parsed;
        try {
            parsed = parser.parse(message.getPayload());
        } catch (Exception e) {
            droppedUnparseable.inc();
            return;
        }
        for (ParsedBookEvent p : parsed) {
            Integer pairId = markets.get(ExchangeMarketsLoader.key(message.getExchangeId(), p.getMarket()));
            if (pairId == null) {
                LOG.warn("Unknown market '{}' for exchange {} — dropping (not in exchange_markets)",
                        p.getMarket(), message.getExchangeId());
                droppedUnknownMarket.inc();
                continue;
            }
            RawOrderBookEvent event = p.getEvent();
            event.setExchangeId(message.getExchangeId());
            event.setPairId(pairId);
            PipelineTimings timings = event.getPipelineTimings();
            timings.setPairExtractIn(ingestTime);
            timings.setPairExtractOut(System.currentTimeMillis());
            out.collect(event);
        }
    }

    @Override
    public void close() {
        markets.close();
    }
}
