package io.tibobit.normalizer.pairextract.parser;


import com.fasterxml.jackson.databind.JsonNode;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import java.util.List;

/**
 * ex7 ompfinex — REST snapshot + Centrifugo WS delta, true delta-feed regime (like ex6/ex8, NOT
 * ex1/ex2's null-seq resync pattern — CONFIRMED via live samples, resolving the 2026-07-14
 * postponement).
 *
 * <ul>
 *   <li><b>REST snapshot</b>: {@code {"status":"OK","data":{"lastUpdateId":N,"time":..,"bids":
 *       [...],"asks":[...]},"pair":"{market}","action":"snapshot",...}}. Market is the top-level
 *       {@code pair} field (a numeric string, e.g. "14"). Levels are [price, qty] string pairs.
 *       Sequence id = data.lastUpdateId (a REAL seq, unlike ex1/ex2's null-seq snapshot), jump 0
 *       (no U on the snapshot itself). Event time = data.time, epoch MICROSECONDS (CONFIRMED) —
 *       divided by 1000 for the platform's millis event time. {@code type="snapshot"}.</li>
 *   <li><b>WebSocket update</b>: Centrifugo push, channel {@code public-market:r-depth-{market}}.
 *       Binance-style diff-depth: {@code data.U}/{@code data.u} are the first/last internal
 *       update ids folded into this message. Sequence id = data.u, jump = u - U (dynamic per
 *       message — CONFIRMED against consecutive live samples: a message's U equals the previous
 *       message's u). The first update after a snapshot has U == the snapshot's lastUpdateId
 *       (CONFIRMED), so job 2's ordinary snapshot-acceptance path seeds the correct baseline
 *       directly — no null-seq/baselinePending special-casing needed, unlike ex1/ex2. Sides are
 *       a/b (asks/bids), string pairs; qty "0" = level delete (same convention as bybit/okx). A
 *       side key is always present, possibly an empty array — passed through as-is, which is
 *       already a no-op on update. No message-level timestamp on the wire → event time is job-1
 *       processing time (same situation as ramzinex). {@code type="update"}.</li>
 * </ul>
 *
 * Anything else (connect acks, pings, other channels) is dropped by the whitelist rule.
 */
public class OmpfinexParser implements RawExchangeParser {

    private static final String CHANNEL_PREFIX = "public-market:r-depth-";

    @Override
    public List<ParsedBookEvent> parse(byte[] payload) throws Exception {
        JsonNode root = Json.MAPPER.readTree(payload);

        if ("snapshot".equals(root.path("action").asText())) {
            return parseSnapshot(root);
        }

        return parseUpdate(root);
    }

    private List<ParsedBookEvent> parseSnapshot(JsonNode root) {
        String market = root.path("pair").asText(null);
        JsonNode data = root.path("data");
        if (market == null
                || !data.path("lastUpdateId").isIntegralNumber()
                || !data.path("time").isIntegralNumber()
                || !data.path("asks").isArray() || !data.path("bids").isArray()) {
            return List.of();
        }
        long eventTimeMicros = data.get("time").asLong();
        RawOrderBookEvent event = new RawOrderBookEvent(0, 0, "snapshot",
                data.get("lastUpdateId").asLong(), 0L, eventTimeMicros / 1000,
                Levels.fromStringPairs(data.get("asks")),
                Levels.fromStringPairs(data.get("bids")));
        event.setSimulation(Json.simulation(root));
        event.setSourceIds(Json.sourceIds(root));
        return List.of(new ParsedBookEvent(market, event));
    }

    private List<ParsedBookEvent> parseUpdate(JsonNode root) {
        JsonNode push = Centrifugo.push(root);
        if (push == null || !push.get("channel").asText().startsWith(CHANNEL_PREFIX)) {
            return List.of();
        }
        JsonNode data = push.get("pub").get("data");
        if (!data.path("u").isIntegralNumber() || !data.path("U").isIntegralNumber()
                || !data.path("a").isArray() || !data.path("b").isArray()) {
            return List.of();
        }
        String market = push.get("channel").asText().substring(CHANNEL_PREFIX.length());
        long u = data.get("u").asLong();
        long uFirst = data.get("U").asLong();
        RawOrderBookEvent event = new RawOrderBookEvent(0, 0, "update",
                u, u - uFirst, System.currentTimeMillis(),
                Levels.fromStringPairs(data.get("a")),
                Levels.fromStringPairs(data.get("b")));
        event.setSimulation(Json.simulation(root));
        event.setSourceIds(Json.sourceIds(root));
        return List.of(new ParsedBookEvent(market, event));
    }
}
