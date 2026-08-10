package io.tibobit.normalizer.pairextract;

import io.tibobit.normalizer.lookup.RefreshingLookup;
import io.tibobit.normalizer.model.Lineage;
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
 *   <li>no {@code id} on the payload → WARN + drop (see below)</li>
 * </ul>
 *
 * <p><b>The id drop rule makes NiFi a hard dependency</b> (user decision 2026-08-03). Lineage
 * is only worth anything if it is unbroken, so an event whose parent cannot be named is not emitted
 * at all rather than emitted with a fabricated or empty parent. The consequence is blunt and worth
 * stating plainly: a NiFi processor that does not inject {@code id} loses 100% of its data
 * here, silently apart from the counter. Deploy NiFi's change BEFORE this jar, not after.
 */
public class PairExtractFunction extends RichFlatMapFunction<RawExchangeMessage, RawOrderBookEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(PairExtractFunction.class);

    private final Map<Integer, RawExchangeParser> parsers;
    private final RefreshingLookup<String, Integer> markets;

    private transient Counter droppedNoParser;
    private transient Counter droppedUnparseable;
    private transient Counter droppedUnknownMarket;
    private transient Counter droppedNoId;

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
        droppedNoId = getRuntimeContext().getMetricGroup().counter("dropped-no-id");
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
            RawOrderBookEvent event = p.getEvent();
            // The parser put NiFi's id here (empty = the payload carried none). Checked before
            // the market lookup so a payload with no lineage is reported as exactly that, rather
            // than as whatever else it might also be wrong about. One payload can fan out to several
            // events, and they all share the one id — so this drops all or none of them.
            if (event.getSourceIds().isEmpty()) {
                LOG.warn("No id on payload from exchange {} — dropping (NiFi must inject it)",
                        message.getExchangeId());
                droppedNoId.inc();
                continue;
            }
            Integer pairId = markets.get(ExchangeMarketsLoader.key(message.getExchangeId(), p.getMarket()));
            if (pairId == null) {
                LOG.warn("Unknown market '{}' for exchange {} — dropping (not in exchange_markets)",
                        p.getMarket(), message.getExchangeId());
                droppedUnknownMarket.inc();
                continue;
            }
            event.setExchangeId(message.getExchangeId());
            event.setPairId(pairId);
            // Each fanned-out event is its own record on the raw topic, so each gets its own id.
            event.setId(Lineage.newId());
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
