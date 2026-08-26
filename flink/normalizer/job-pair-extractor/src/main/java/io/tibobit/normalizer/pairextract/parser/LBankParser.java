package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.JsonNode;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * ex9 lbank — a SNAPSHOT-ONLY feed: every frame carries the whole book under {@code depth},
 * so there are no deltas and nothing to be contiguous with. Market key = the top-level
 * {@code pair}, lowercase and underscored ({@code btc_usdt} — matching
 * {@code exchange_markets.market} verbatim, so nothing lower-cases it here). Levels are
 * {@code [price, qty]} string pairs on both sides, kept verbatim.
 *
 * <p><b>Sequence id is null and the jump is 0</b> (user decision 2026-08-26). The wire has no
 * counter — the only monotonic field is the timestamp, and re-using it as a sequence would
 * impose a cadence the exchange never promised. A null sequence puts ex9 on job 2's event-time
 * branch instead, where the whole test is "not older than the last accepted frame"; that is
 * exactly the guarantee a full-snapshot feed needs, because any accepted frame replaces the
 * book outright. ex9 is the THIRD exchange on that branch, after ex3/wallex and the ex1/ex2
 * REST snapshots, and like ex3 it never sends an update, so the {@code baselinePending} flag
 * job 2 sets is never consumed.
 *
 * <p>Note that guard is {@code <}, not {@code <=}: two frames with the SAME {@code TS} are both
 * accepted and the book is simply re-emitted unchanged (user decision 2026-08-26 — equal is not
 * out of order). Only a strictly older frame is rejected {@code out_of_order}.
 *
 * <p>Event time = {@code TS}, an ISO-8601 local date-time with NO zone marker
 * ({@code "2026-08-25T17:46:51.723"}), read as <b>UTC</b> (user-confirmed 2026-08-26 — if that
 * is ever revised, this offset is the one line to change).
 *
 * <p>Frame selection is by SHAPE, not by the {@code type} field (user decision 2026-08-26): a
 * frame needs {@code pair}, a textual {@code TS}, and both {@code depth.asks} and
 * {@code depth.bids} as arrays. The captured samples say {@code "type":"fdepth"}; lbank's other
 * channels (pings, subscribe acks, ticks, {@code incrDepth}) all fail the shape check already,
 * so whitelisting the value would add a second thing to keep in sync for no coverage today.
 * See sample-raw-data.md § ex9.
 */
public class LBankParser implements RawExchangeParser {

    @Override
    public List<ParsedBookEvent> parse(byte[] payload) throws Exception {
        JsonNode root = Json.MAPPER.readTree(payload);

        String market = root.path("pair").asText(null);
        JsonNode depth = root.path("depth");

        if (market == null
                || !depth.path("asks").isArray()
                || !depth.path("bids").isArray()
                || !root.path("TS").isTextual()) {
            return List.of();
        }

        long eventTimeMs = LocalDateTime
                .parse(root.get("TS").asText())
                .toInstant(ZoneOffset.UTC)
                .toEpochMilli();

        RawOrderBookEvent event = new RawOrderBookEvent(
                0,
                0,
                "snapshot",
                null,
                0L,
                eventTimeMs,
                Levels.fromStringPairs(depth.get("asks")),
                Levels.fromStringPairs(depth.get("bids"))
        );

        event.setSimulation(Json.simulation(root));
        event.setSourceIds(Json.sourceIds(root));

        return List.of(new ParsedBookEvent(market, event));
    }
}
