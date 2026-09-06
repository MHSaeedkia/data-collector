package io.tibobit.normalizer.pairextract.parser;

import io.tibobit.normalizer.model.RawOrderBookEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link OkxParser} (ex8) against the captured wire samples (sample-raw-data.md § ex8):
 * the {@code books} channel's bitget-family envelope, {@code action: "snapshot" | "update"},
 * dashed market key (ZEC-USDT), and {@code seqId} as the sequence id with a DYNAMIC
 * {@code seqId - prevSeqId} jump. The two WS fixtures are a genuinely consecutive pair captured
 * from the live feed, so the chain between them is real and not constructed.
 */
class OkxParserTest {

    private final OkxParser parser = new OkxParser();

    /**
     * Given the captured ex8 snapshot, When parsed, Then the market keeps its dash, {@code seqId}
     * becomes the sequence id, {@code ts} the event time, and the jump is ZERO — a snapshot is
     * ordered, never jump-checked, and its {@code prevSeqId: -1} is a sentinel that must never
     * reach the arithmetic (deriving the jump from it would yield {@code seqId + 1}).
     */
    @Test
    @DisplayName("parses the captured snapshot, jump 0 despite prevSeqId -1")
    void parsesSnapshot() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex8-snapshot.json"));

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).getMarket()).isEqualTo("ZEC-USDT");
        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("snapshot");
        assertThat(event.getSequenceId()).isEqualTo(4429784547L);
        assertThat(event.getSequenceJump()).isZero();
        assertThat(event.getSequenceJumpTolerance()).isZero();
        assertThat(event.getEventTime()).isEqualTo(1788613464301L); // data.ts, not the seq
        assertThat(event.getAsks()).hasSize(3);
        assertThat(event.getAsks().get(0).getPrice()).isEqualTo("1012.6");
        assertThat(event.getBids().get(0).getQuantity()).isEqualTo("0.0322");
    }

    /**
     * Given the captured ex8 update, When parsed, Then the jump is the DYNAMIC
     * {@code seqId - prevSeqId} (4429784551 - 4429784547 = 4), the four-element levels keep only
     * price and qty, and the qty-"0" delete survives verbatim (job 5 needs it to remove a level).
     */
    @Test
    @DisplayName("parses the captured update with the dynamic jump and the qty-0 delete")
    void parsesUpdate() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex8-update.json"));

        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("update");
        assertThat(event.getSequenceId()).isEqualTo(4429784551L);
        assertThat(event.getSequenceJump()).isEqualTo(4L); // seqId - prevSeqId
        assertThat(event.getSequenceJumpTolerance()).isZero();
        assertThat(event.getEventTime()).isEqualTo(1788613464401L);
        assertThat(event.getAsks()).hasSize(2);
        assertThat(event.getAsks().get(0).getPrice()).isEqualTo("1013.67");
        assertThat(event.getAsks().get(0).getQuantity()).isEqualTo("0"); // delete signal
        assertThat(event.getBids()).hasSize(3);
        assertThat(event.getBids().get(1).getQuantity()).isEqualTo("0"); // and on the bid side
    }

    /**
     * The invariant the whole channel switch rests on, pinned on the real consecutive pair.
     *
     * <p>Job 2 checks {@code seq == lastSeq + jump}. Substituting this parser's dynamic jump gives
     * {@code seqId == lastSeq + (seqId - prevSeqId)}, which reduces to {@code prevSeqId == lastSeq}
     * — okx's own documented contiguity rule, enforced exactly with no change to job 2 and no
     * window or tolerance. So for the snapshot→update transition the update's
     * {@code sequenceId - sequenceJump} must equal the snapshot's {@code sequenceId}.
     */
    @Test
    @DisplayName("the dynamic jump makes job 2's check reduce to prevSeqId == lastSeq")
    void dynamicJumpReducesToPrevSeqIdChaining() throws Exception {
        RawOrderBookEvent snapshot =
                parser.parse(Fixtures.bytes("ex8-snapshot.json")).get(0).getEvent();
        RawOrderBookEvent update =
                parser.parse(Fixtures.bytes("ex8-update.json")).get(0).getEvent();

        // what job 2 computes: expected == lastSeq + jump, with lastSeq = the snapshot's seq
        long expected = snapshot.getSequenceId() + update.getSequenceJump();
        assertThat(update.getSequenceId()).isEqualTo(expected);
        // ... which is the same statement as prevSeqId == lastSeq
        assertThat(update.getSequenceId() - update.getSequenceJump())
                .isEqualTo(snapshot.getSequenceId());
    }

    /**
     * Given okx's documented no-change keepalive, which repeats the counter
     * ({@code seqId == prevSeqId}), When parsed, Then the jump is 0 — job 2's window then accepts
     * {@code seq == lastSeq} and the book is left exactly as it was, which is what a no-op means.
     * No special-casing needed anywhere; this test exists so the behaviour is deliberate.
     */
    @Test
    @DisplayName("a no-change keepalive (seqId == prevSeqId) stamps jump 0")
    void keepaliveStampsZeroJump() throws Exception {
        byte[] keepalive = ("{\"arg\":{\"channel\":\"books\",\"instId\":\"ZEC-USDT\"},"
                + "\"action\":\"update\",\"data\":[{\"asks\":[],\"bids\":[],"
                + "\"ts\":\"1788613464501\",\"checksum\":0,"
                + "\"seqId\":4429784551,\"prevSeqId\":4429784551}]}")
                .getBytes(StandardCharsets.UTF_8);

        RawOrderBookEvent event = parser.parse(keepalive).get(0).getEvent();
        assertThat(event.getSequenceId()).isEqualTo(4429784551L);
        assertThat(event.getSequenceJump()).isZero();
        assertThat(event.getAsks()).isEmpty();
        assertThat(event.getBids()).isEmpty();
    }

    /**
     * Given WS frames whose counter fields are missing or not integral, When parsed, Then the
     * whole frame is discarded rather than emitted with a guessed sequence — without both fields
     * the chain cannot be expressed and job 2 would silently mis-validate.
     */
    @Test
    @DisplayName("discards a WS frame missing seqId or prevSeqId")
    void discardsFramesWithoutCounters() throws Exception {
        String head = "{\"arg\":{\"channel\":\"books\",\"instId\":\"ZEC-USDT\"},"
                + "\"action\":\"update\",\"data\":[{\"asks\":[],\"bids\":[],"
                + "\"ts\":\"1788613464501\",\"checksum\":0,";

        assertThat(parser.parse((head + "\"seqId\":4429784551}]}").getBytes(StandardCharsets.UTF_8)))
                .isEmpty(); // no prevSeqId
        assertThat(parser.parse((head + "\"prevSeqId\":4429784547}]}").getBytes(StandardCharsets.UTF_8)))
                .isEmpty(); // no seqId
        assertThat(parser.parse((head + "\"seqId\":\"4429784551\",\"prevSeqId\":4429784547}]}")
                .getBytes(StandardCharsets.UTF_8)))
                .isEmpty(); // seqId as a string, not an integer
    }

    /**
     * Given non-book frames (okx subscribe event, empty object), When parsed, Then they are
     * silently discarded.
     */
    @Test
    @DisplayName("discards non-book frames")
    void discardsNonBookFrames() throws Exception {
        byte[] subscribeEvent =
                "{\"event\":\"subscribe\",\"arg\":{\"channel\":\"books\",\"instId\":\"BTC-USDT\"}}"
                        .getBytes(StandardCharsets.UTF_8);

        assertThat(parser.parse(subscribeEvent)).isEmpty();
        assertThat(parser.parse("{}".getBytes(StandardCharsets.UTF_8))).isEmpty();
    }

    /**
     * Given the captured REST depth response, When parsed, Then every way it differs from a WS
     * frame is handled: the market comes from NiFi's injected {@code pair} (there is no
     * {@code arg} to read {@code instId} from), and the FOUR-element levels
     * {@code [price, qty, "0", orderCount]} keep only their first two entries.
     *
     * <p>And it is still NULL-SEQ, though the REASON changed with the {@code books} channel. It is
     * no longer that {@code seqId} is a foreign number space — it is now the very counter the WS
     * frames are sequenced by. It is that a snapshot's {@code seqId} is not any later update's
     * {@code prevSeqId}: the counter advances between NiFi's fetch and the next WS frame, so
     * seeding {@code lastSeq} from it would break the next chain check rather than repair it.
     * Null hands job 2 the {@code baselinePending} bootstrap instead.
     */
    @Test
    @DisplayName("the REST snapshot is null-seq so it never seeds the update window")
    void parsesRestSnapshot() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex8-rest-snapshot.json"));

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).getMarket()).isEqualTo("ZEC-USDT"); // injected `pair`
        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("snapshot");
        assertThat(event.getSequenceId()).isNull();
        assertThat(event.getSequenceJump()).isZero();
        assertThat(event.getSequenceJumpTolerance()).isZero();
        assertThat(event.getEventTime()).isEqualTo(1788605352151L); // data.ts
        assertThat(event.getAsks()).hasSize(3);
        assertThat(event.getAsks().get(0).getPrice()).isEqualTo("1011.99");
        assertThat(event.getAsks().get(0).getQuantity()).isEqualTo("0.2362");
        assertThat(event.getBids().get(2).getPrice()).isEqualTo("1011.65");
    }

    /**
     * Given REST-shaped bodies that are not a usable book — an error response (whose {@code data}
     * is empty), and a success body NiFi never stamped with {@code pair} — When parsed, Then they
     * are silently discarded rather than emitted against an unknown market.
     */
    @Test
    @DisplayName("discards a REST error body and an unstamped one")
    void discardsUnusableRestBodies() throws Exception {
        byte[] errorBody = ("{\"code\":\"51001\",\"msg\":\"Instrument ID does not exist\","
                + "\"data\":[],\"pair\":\"ZEC-USDT\",\"action\":\"snapshot\"}")
                .getBytes(StandardCharsets.UTF_8);
        byte[] noPair = ("{\"code\":\"0\",\"msg\":\"\",\"data\":[{"
                + "\"asks\":[[\"1011.99\",\"0.2362\",\"0\",\"1\"]],"
                + "\"bids\":[[\"1011.74\",\"0.26887\",\"0\",\"2\"]],"
                + "\"ts\":\"1788605352151\",\"seqId\":4428333610}],\"action\":\"snapshot\"}")
                .getBytes(StandardCharsets.UTF_8);

        assertThat(parser.parse(errorBody)).isEmpty();
        assertThat(parser.parse(noPair)).isEmpty();
    }
}
