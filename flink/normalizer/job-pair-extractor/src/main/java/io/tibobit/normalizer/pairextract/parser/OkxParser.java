package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.JsonNode;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * ex8 okx — TWO raw streams share the {@code ex8-raw} topic (sample-raw-data.md § ex8), exactly
 * as on ex5/bitget:
 *
 * <ul>
 *   <li><b>WebSocket {@code books} feed</b> ({@code wss://ws.okx.com:8443/ws/v5/public}):
 *       bitget-family {@code arg}/{@code action}/{@code data}-array envelope, true delta feed
 *       ({@code action: "snapshot" | "update"}), 400 levels a side on the snapshot. Market key =
 *       {@code arg.instId} (with a DASH: {@code BTC-USDT}); levels are FOUR-element string arrays
 *       ({@code [price, qty, "0", orderCount]}) and {@link Levels#fromStringPairs} reads the first
 *       two. qty {@code "0"} = level delete. <b>Sequence id = {@code data[i].seqId}</b>, a real
 *       book counter (JSON integer, order 1e10); event time = {@code data[i].ts} (STRING millis).
 *       </li>
 *   <li><b>REST snapshot</b>: the depth endpoint's response, which NiFi tags with
 *       {@code "action":"snapshot"} and stamps with the market as a top-level {@code "pair"} field
 *       (the REST body carries no symbol of its own). Same four-element levels.</li>
 * </ul>
 *
 * <p><b>The discriminator is {@code arg}, NOT the shape of {@code data}.</b> ex5 can tell its two
 * streams apart because its REST {@code data} is an object while its WS {@code data} is an array;
 * here {@code data} is an ARRAY on both, and {@code action} reads {@code "snapshot"} on both. Only
 * the WS frame has an {@code arg}.
 *
 * <p><b>The jump is DYNAMIC — {@code seqId - prevSeqId} per message, the ex7 pattern.</b> okx
 * chains every {@code books} frame to its predecessor: {@code prevSeqId} is the {@code seqId} of
 * the message before it. Algebraically job 2's {@code seq == lastSeq + jump} then reduces to
 * <b>{@code prevSeqId == lastSeq}</b> — okx's own documented contiguity rule, enforced exactly,
 * with no change to job 2 and no window or tolerance anywhere. Measured over 6,516 consecutive
 * live transitions on 5 markets: <b>6,516 chained, 0 broken</b>, while the raw {@code seqId} step
 * took 90–172 DISTINCT values per market (3 … 960), so no fixed jump could ever have worked.
 *
 * <p>Two okx-documented edge cases fall out correctly and need no special-casing. A no-change
 * keepalive carries {@code seqId == prevSeqId}, i.e. jump 0, and job 2's window accepts
 * {@code seq == lastSeq} — a no-op, which is what it is. A counter RESET (okx may restart
 * {@code seqId} lower after maintenance) breaks the chain, so job 2 raises {@code sequence_gap},
 * empties the book and asks the control plane — which is the correct response to a reset.
 *
 * <p><b>A WS snapshot is jump 0 — ordered, never jump-checked</b> (the platform-wide invariant).
 * It carries {@code prevSeqId: -1}, which is a sentinel and not a sequence; stamping the dynamic
 * jump from it would compute {@code seqId + 1}. The first update after a snapshot then chains to
 * it normally, which is exactly what the wire does: the first update's {@code prevSeqId} equals
 * the snapshot's {@code seqId} (verified 5/5 markets).
 *
 * <p><b>The REST snapshot stays NULL-SEQ — and the reason CHANGED with this channel.</b> It is no
 * longer that the two are different number spaces: the REST body's {@code seqId} is the SAME
 * counter the WS frames now use. It is that a snapshot's {@code seqId} is not any later update's
 * {@code prevSeqId} — the counter advances between NiFi's fetch and the next WS frame, so seeding
 * {@code lastSeq} from it would break the very next chain check and gap immediately. Null instead
 * hands job 2 the {@code baselinePending} bootstrap ex1/ex2/ex5/ex6 already take: order this body
 * by EVENT TIME, then let the first WS update after it adopt its own {@code seqId} as the
 * baseline. {@code data.ts} is still the event time — a real timestamp, just not a comparable
 * sequence.
 *
 * <p><b>Prefer a RESUBSCRIBE over the REST body as the resync answer.</b> Every fresh subscribe
 * returns {@code action: "snapshot"} with 400 levels on the feed's own counter, so a resubscribe
 * re-seeds {@code lastSeq} exactly and the next update chains to it — no baseline gap at all. The
 * REST branch is kept as the fallback for however NiFi chooses to answer.
 *
 * <p>{@code checksum} is okx's CRC32 book-integrity value. It is ignored here, as ex5's is: job 5
 * builds the book and nothing in the platform verifies a checksum.
 *
 * <p><b>Why the REST branch existing at all is the fix (found live 2026-09-05).</b> Without it
 * job 1 dropped the resync answer on the floor: no {@code arg} means {@code arg.instId} is null,
 * which discarded the whole frame. So a gap on ex8 was TERMINAL — job 2 emitted a reset, asked for
 * a snapshot, NiFi fetched one and published it, and job 2 never saw it. Every later update
 * dead-lettered as {@code awaiting_snapshot} and the market stayed dark until the job restarted.
 */
public class OkxParser implements RawExchangeParser {

    @Override
    public List<ParsedBookEvent> parse(byte[] payload) throws Exception {
        JsonNode root = Json.MAPPER.readTree(payload);

        // See the class javadoc: `arg` is the only field the two streams do not share.
        if (!root.has("arg")) {
            return parseRestSnapshot(root);
        }
        return parseWsFrames(root);
    }

    /**
     * The REST depth response. {@code code}/{@code msg} are not inspected: an error body carries no
     * {@code data} array of books, so the shape whitelist already discards it. A body NiFi never
     * stamped with {@code pair} is discarded too rather than emitted against an unknown market.
     *
     * <p>Null-seq deliberately — see the class javadoc. The body's {@code seqId} is on the same
     * counter the WS branch now reads, but it is not any later update's {@code prevSeqId}, so
     * seeding it would break the next chain check rather than repair it.
     */
    private List<ParsedBookEvent> parseRestSnapshot(JsonNode root) {
        if (!"snapshot".equals(root.path("action").asText())
                || !root.path("pair").isTextual()
                || !root.path("data").isArray()) {
            return List.of();
        }
        String market = root.get("pair").asText();
        List<ParsedBookEvent> events = new ArrayList<>();
        for (JsonNode book : root.get("data")) {
            if (!book.path("ts").isTextual() || (!book.has("asks") && !book.has("bids"))) {
                return List.of();
            }
            RawOrderBookEvent event = new RawOrderBookEvent(0, 0, "snapshot",
                    null, 0L, Long.parseLong(book.get("ts").asText()),
                    book.has("asks") ? Levels.fromStringPairs(book.get("asks")) : null,
                    book.has("bids") ? Levels.fromStringPairs(book.get("bids")) : null);
            event.setSimulation(Json.simulation(root));
            event.setSourceIds(Json.sourceIds(root));
            events.add(new ParsedBookEvent(market, event));
        }
        return events;
    }

    private List<ParsedBookEvent> parseWsFrames(JsonNode root) {
        String type = switch (root.path("action").asText()) {
            case "snapshot" -> "snapshot";
            case "update" -> "update";
            default -> null;
        };
        String market = root.path("arg").path("instId").asText(null);
        if (type == null || market == null || !root.path("data").isArray()) {
            return List.of();
        }
        List<ParsedBookEvent> events = new ArrayList<>();
        for (JsonNode book : root.get("data")) {
            if (!book.path("ts").isTextual()
                    || !book.path("seqId").isIntegralNumber()
                    || !book.path("prevSeqId").isIntegralNumber()
                    || (!book.has("asks") && !book.has("bids"))) {
                return List.of();
            }
            long seqId = book.get("seqId").asLong();
            // A snapshot is ordered, never jump-checked, and its prevSeqId is the -1 sentinel.
            // An update chains: jump = seqId - prevSeqId makes job 2's check `prevSeqId == lastSeq`.
            long jump = "snapshot".equals(type) ? 0L : seqId - book.get("prevSeqId").asLong();
            RawOrderBookEvent event = new RawOrderBookEvent(0, 0, type,
                    seqId, jump, Long.parseLong(book.get("ts").asText()),
                    book.has("asks") ? Levels.fromStringPairs(book.get("asks")) : null,
                    book.has("bids") ? Levels.fromStringPairs(book.get("bids")) : null);
            event.setSimulation(Json.simulation(root));
            event.setSourceIds(Json.sourceIds(root));
            events.add(new ParsedBookEvent(market, event));
        }
        return events;
    }
}
