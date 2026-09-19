package io.tibobit.normalizer.parse;

import io.tibobit.normalizer.model.Lineage;
import io.tibobit.normalizer.model.ParsedBookEvent;
import io.tibobit.normalizer.model.PipelineTimings;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import io.tibobit.normalizer.parse.parser.RawExchangeParser;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * The parse step: pick the exchange's parser, parse the verbatim payload, stamp exchange_id and
 * lineage, emit. The market string rides along unresolved — turning it into a pair_id is job 2's
 * only job. Drop rules (all counted, none dead-lettered — dead-letter is job 3's validation
 * concern):
 *
 * <ul>
 *   <li>no parser for the exchange (future topics — every seeded exchange has one since
 *       ex9 lbank landed 2026-08-25) → drop</li>
 *   <li>unrecognized/malformed frame (whitelist rule) → drop, never crash</li>
 *   <li>no {@code id} on the payload → WARN + drop (see below)</li>
 * </ul>
 *
 * <p><b>The id drop rule makes NiFi a hard dependency</b> (user decision 2026-08-03). Lineage
 * is only worth anything if it is unbroken, so an event whose parent cannot be named is not emitted
 * at all rather than emitted with a fabricated or empty parent. The consequence is blunt and worth
 * stating plainly: a NiFi processor that does not inject {@code id} loses 100% of its data
 * here, silently apart from the counter. Deploy NiFi's change BEFORE this jar, not after.
 *
 * <p>The check has to live in THIS job, not in job 2: it reads the id the parser lifted off NiFi's
 * payload, and this operator overwrites that field with its own minted id on the very next line.
 * By the time job 2 sees the record there is no missing id left to detect.
 */
public class ParseFunction extends RichFlatMapFunction<RawExchangeMessage, ParsedBookEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(ParseFunction.class);

    private final Map<Integer, RawExchangeParser> parsers;

    private transient Counter droppedNoParser;
    private transient Counter droppedUnparseable;
    private transient Counter droppedNoId;

    public ParseFunction(Map<Integer, RawExchangeParser> parsers) {
        this.parsers = parsers;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        droppedNoParser = getRuntimeContext().getMetricGroup().counter("dropped-no-parser");
        droppedUnparseable = getRuntimeContext().getMetricGroup().counter("dropped-unparseable");
        droppedNoId = getRuntimeContext().getMetricGroup().counter("dropped-no-id");
    }

    @Override
    public void flatMap(RawExchangeMessage message, Collector<ParsedBookEvent> out) {
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
            // The parser put NiFi's id here (empty = the payload carried none). One payload can fan
            // out to several events, and they all share the one id — so this drops all or none of
            // them.
            if (event.getSourceIds().isEmpty()) {
                LOG.warn("No id on payload from exchange {} — dropping (NiFi must inject it)",
                        message.getExchangeId());
                droppedNoId.inc();
                continue;
            }
            event.setExchangeId(message.getExchangeId());
            // Each fanned-out event is its own record on the parsed topic, so each gets its own id.
            // source_ids is already NiFi's id, put there by the parser.
            event.setId(Lineage.newId());
            PipelineTimings timings = event.getPipelineTimings();
            timings.setParseIn(ingestTime);
            timings.setParseOut(System.currentTimeMillis());
            out.collect(p);
        }
    }
}
