package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.JsonNode;
import io.tibobit.normalizer.model.RawOrderBookEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * ex5 bitget — the {@code books50} channel: bitget's own {@code action}/{@code arg}/{@code data}
 * envelope, market key = {@code arg.instId}, {@code data} an ARRAY of book objects whose
 * {@code asks}/{@code bids} are [price, qty] STRING pairs. Ordering field for job 2 is
 * {@code data[i].seq} (jump 0 = snapshot feed); event time is {@code data[i].ts}, a STRING of
 * epoch millis. See sample-raw-data.md § ex5.
 *
 * <p><b>REVISED 2026-09-07 — back to snapshot-only, and it is the ONLY stream now.</b> The
 * collector moved off the price-grouped {@code depth} channel and back onto {@code books50}, which
 * pushes a full book on every frame: {@code action} reads {@code "snapshot"} and nothing else, and
 * the REST depth endpoint is no longer polled at all. Three things that were true between
 * 2026-08-22 and this change are therefore gone:
 *
 * <ul>
 *   <li><b>No {@code "update"} frames.</b> ex5 is not a delta feed. There is no qty-{@code "0"}
 *       delete, no cold start, no {@code no_baseline} and no {@code sequence_gap} — job 2 can
 *       only ever reach its snapshot branch for this exchange, so ex5 emits no control command.
 *       It joins ex3, ex4 and ex9 as a snapshot-only feed.</li>
 *   <li><b>No REST stream on {@code ex5-raw}.</b> The second stream added 2026-08-23 (a
 *       {@code data} OBJECT with {@code a}/{@code b} NUMERIC sides and an injected {@code pair})
 *       is gone with the poller, along with the shape discriminator it needed. A body in that
 *       shape now falls through the whitelist and is dropped.</li>
 *   <li><b>No timestamp-as-sequence, and no jump window.</b> {@code seq} and {@code pseq} are
 *       back on the wire, so the ordering field is a real monotonic counter again rather than the
 *       {@code ts} clock the {@code depth} channel forced. {@code sequenceJump} is 0. ex5 was the
 *       only exchange that ever stamped a jump tolerance, so when it left the delta group the
 *       field lost its last user and was <b>dropped from the schema and from job 2</b> on
 *       2026-09-07.</li>
 * </ul>
 *
 * <p><b>{@code pseq} is read by nobody, and it is NOT a predecessor pointer.</b> The obvious guess
 * — that it chains each frame to the one before, the job ex8/okx's {@code prevSeqId} does — is
 * wrong on this channel: it reads <b>0 on every frame</b> (5 consecutive live captures,
 * 2026-09-07). Nothing would work if it were adopted as a chain. Job 2 only asks that {@code seq}
 * move forward, which is all a snapshot feed can promise.
 *
 * <p><b>Why {@code seq} can be ordered on but never gap-checked</b>, measured on those same five
 * frames: {@code seq} advanced 2816, 5442, 5076 and 11122 between snapshots that were only 351,
 * 97, 150 and 150 ms apart. It is a fast underlying book counter and these snapshots are SAMPLED
 * from it, so consecutive frames are never adjacent in {@code seq} and no jump — fixed, dynamic or
 * windowed — could ever fit. {@code sequenceJump} 0 is not a default here, it is the only
 * possibility. The same numbers are why the old {@code depth} channel's timestamp-as-sequence was
 * abandoned rather than re-fitted: the publish cadence is not a cadence.
 */
public class BitgetParser implements RawExchangeParser {

    @Override
    public List<ParsedBookEvent> parse(byte[] payload) throws Exception {
        JsonNode root = Json.MAPPER.readTree(payload);
        String market = root.path("arg").path("instId").asText(null);
        if (market == null || !"snapshot".equals(root.path("action").asText())
                || !root.path("data").isArray()) {
            return List.of();
        }
        List<ParsedBookEvent> events = new ArrayList<>();
        for (JsonNode book : root.get("data")) {
            // Both sides are required: every frame is a full book, so a half-frame would wipe a
            // live side rather than leave it alone. Wire TYPES are part of the whitelist — `seq`
            // an integral number, `ts` a string — because a frame that swaps them is a different
            // message, not a lenient variant of this one.
            if (!book.path("asks").isArray() || !book.path("bids").isArray()
                    || !book.path("seq").isIntegralNumber() || !book.path("ts").isTextual()) {
                return List.of();
            }
            RawOrderBookEvent event = new RawOrderBookEvent(0, 0, "snapshot",
                    book.get("seq").asLong(), 0L,
                    Long.parseLong(book.get("ts").asText()),
                    Levels.fromStringPairs(book.get("asks")),
                    Levels.fromStringPairs(book.get("bids")));
            event.setSimulation(Json.simulation(root));
            event.setSourceIds(Json.sourceIds(root));
            events.add(new ParsedBookEvent(market, event));
        }
        return events;
    }
}
