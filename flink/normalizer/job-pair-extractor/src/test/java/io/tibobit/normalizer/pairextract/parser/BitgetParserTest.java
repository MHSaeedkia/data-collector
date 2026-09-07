package io.tibobit.normalizer.pairextract.parser;

import io.tibobit.normalizer.model.RawOrderBookEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link BitgetParser} (ex5) against the captured wire samples (sample-raw-data.md § ex5),
 * REVISED 2026-09-07 for the return to the {@code books50} channel: snapshot-only, {@code seq}
 * and {@code pseq} back on the wire, and no REST stream on {@code ex5-raw} at all. The three
 * things the {@code depth} channel needed and this one does not — an {@code "update"} regime, the
 * inner {@code ts} doing double duty as a sequence, and a nonzero {@code sequenceJumpTolerance} —
 * each get a test here proving they are gone rather than merely unused.
 *
 * <p>The two snapshot fixtures are a <b>genuinely consecutive captured pair</b> (frames 1 and 2 of
 * a 5-frame live capture, level arrays trimmed), so {@link #consecutiveCapturesMoveForward()}
 * asserts the real counter rather than a constructed one.
 */
class BitgetParserTest {

    private final BitgetParser parser = new BitgetParser();

    /**
     * Given the captured ex5 snapshot, When parsed, Then the market comes from arg.instId, the
     * data array is unwrapped, the ordering field is the integral {@code seq} at jump 0 (with the
     * tolerance back at its default 0), and the event time is the inner STRING ts — the outer
     * numeric ts is ignored.
     */
    @Test
    @DisplayName("parses the captured snapshot")
    void parsesSnapshot() throws Exception {
        List<ParsedBookEvent> parsed = parser.parse(Fixtures.bytes("ex5-snapshot.json"));

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).getMarket()).isEqualTo("BTCUSDT");
        RawOrderBookEvent event = parsed.get(0).getEvent();
        assertThat(event.getType()).isEqualTo("snapshot");
        assertThat(event.getSequenceId()).isEqualTo(787944892031L);
        assertThat(event.getSequenceJump()).isZero();
        assertThat(event.getSequenceJumpTolerance()).isZero();
        assertThat(event.getEventTime()).isEqualTo(1788771223652L); // inner STRING ts
        assertThat(event.getAsks().get(0).getPrice()).isEqualTo("79427.25");
        assertThat(event.getAsks().get(0).getQuantity()).isEqualTo("0.122814");
        assertThat(event.getBids().get(0).getPrice()).isEqualTo("79427.24");
    }

    /**
     * Given a frame with several book objects in {@code data}, When parsed, Then each becomes its
     * own event with its own seq and event time. ex5 is the only exchange whose one Kafka record
     * can fan out into several events.
     *
     * <p>⚠ The capability is real but UNOBSERVED on this channel — all 5 live captures carry
     * exactly one element. This is a defensive test, kept because the loop exists and a
     * multi-element frame must not be half-applied.
     */
    @Test
    @DisplayName("emits one event per element of the data array")
    void fansOutTheDataArray() throws Exception {
        byte[] twoBooks = ("{\"action\":\"snapshot\","
                + "\"arg\":{\"instType\":\"SPOT\",\"channel\":\"books50\",\"instId\":\"BTCUSDT\"},"
                + "\"data\":["
                + "{\"asks\":[[\"79427.25\",\"1\"]],\"bids\":[[\"79427.24\",\"2\"]],"
                + "\"ts\":\"1788771223652\",\"seq\":787944892031,\"pseq\":0},"
                + "{\"asks\":[[\"79428.00\",\"3\"]],\"bids\":[[\"79427.00\",\"4\"]],"
                + "\"ts\":\"1788771224252\",\"seq\":787944903153,\"pseq\":0}"
                + "]}").getBytes(StandardCharsets.UTF_8);

        List<ParsedBookEvent> parsed = parser.parse(twoBooks);

        assertThat(parsed).hasSize(2);
        assertThat(parsed.get(0).getEvent().getSequenceId()).isEqualTo(787944892031L);
        assertThat(parsed.get(1).getEvent().getSequenceId()).isEqualTo(787944903153L);
        assertThat(parsed.get(1).getEvent().getEventTime()).isEqualTo(1788771224252L);
    }

    /**
     * Given the two consecutive live captures, When parsed, Then {@code seq} moves FORWARD and
     * {@code pseq} is not what moves it. This is the assumption the whole exchange rests on — job
     * 2 orders ex5's snapshots by {@code seq} and by nothing else — so it is pinned against real
     * frames rather than constructed ones.
     *
     * <p>Note the SIZE of the step: 11,122 across 351 ms of wall clock. {@code seq} is a fast
     * underlying book counter that these snapshots are sampled from, which is exactly why
     * {@code sequenceJump} must be 0. No jump rule could fit a step that big and that variable
     * (2816 … 11122 over the 5-frame capture), and job 2 never applies one to a snapshot anyway.
     */
    @Test
    @DisplayName("two consecutive live captures move seq forward, by far more than 1")
    void consecutiveCapturesMoveForward() throws Exception {
        RawOrderBookEvent first = parser.parse(Fixtures.bytes("ex5-snapshot.json")).get(0).getEvent();
        RawOrderBookEvent second = parser.parse(Fixtures.bytes("ex5-snapshot-2.json")).get(0).getEvent();

        assertThat(second.getSequenceId()).isGreaterThan(first.getSequenceId());
        assertThat(second.getSequenceId() - first.getSequenceId()).isEqualTo(11122L);
        assertThat(second.getEventTime() - first.getEventTime()).isEqualTo(351L);
        // pseq reads 0 on every captured frame, so it is not a predecessor pointer and the parser
        // must never grow a chain rule from it.
        assertThat(first.getSequenceJump()).isZero();
        assertThat(second.getSequenceJump()).isZero();
    }

    /**
     * Given every frame shape the whitelist rejects — a subscribe ack, an empty object, an
     * {@code action} this channel does not send, a numeric inner ts, a {@code seq} that is not
     * integral, and a half book — When parsed, Then all are silently discarded.
     *
     * <p>Two of these used to be accepted and no longer are, which is the point of pinning them:
     * {@code action: "update"} was a first-class frame on the {@code depth} channel, and a
     * one-sided body was a legal delta there. On a snapshot feed a missing side is not "leave this
     * side alone" — applying it would wipe a live side — so the whole frame goes.
     */
    @Test
    @DisplayName("discards non-book frames, updates and half books")
    void discardsNonBookFrames() throws Exception {
        byte[] subscribeAck = "{\"event\":\"subscribe\",\"arg\":{\"instId\":\"BTCUSDT\"}}"
                .getBytes(StandardCharsets.UTF_8);
        byte[] update = book("update",
                "\"asks\":[[\"79427.25\",\"0\"]],\"bids\":[[\"79427.24\",\"2\"]],"
                        + "\"ts\":\"1788771223652\",\"seq\":787944892031");
        byte[] numericTs = book("snapshot",
                "\"asks\":[],\"bids\":[],\"ts\":1788771223652,\"seq\":787944892031");
        byte[] stringSeq = book("snapshot",
                "\"asks\":[],\"bids\":[],\"ts\":\"1788771223652\",\"seq\":\"787944892031\"");
        byte[] asksOnly = book("snapshot",
                "\"asks\":[[\"79427.25\",\"1\"]],\"ts\":\"1788771223652\",\"seq\":787944892031");

        assertThat(parser.parse(subscribeAck)).isEmpty();
        assertThat(parser.parse("{}".getBytes(StandardCharsets.UTF_8))).isEmpty();
        assertThat(parser.parse(update)).isEmpty();
        assertThat(parser.parse(numericTs)).isEmpty();
        assertThat(parser.parse(stringSeq)).isEmpty();
        assertThat(parser.parse(asksOnly)).isEmpty();
    }

    /**
     * Given the REST depth body that used to be the second stream on {@code ex5-raw}, When parsed,
     * Then nothing comes out. The poller is gone (2026-09-07), so this shape is no longer on the
     * topic; the assertion exists so that reviving the poller fails loudly here rather than
     * silently emitting nothing in production.
     */
    @Test
    @DisplayName("the retired REST depth body is no longer parsed")
    void discardsTheRetiredRestBody() throws Exception {
        byte[] restBody = ("{\"code\":\"00000\",\"msg\":\"success\","
                + "\"requestTime\":1787465707150,"
                + "\"data\":{\"a\":[[1.4482,433.2497]],\"b\":[[1.4481,83.5936]],"
                + "\"ts\":\"1787465707152\"},"
                + "\"pair\":\"XRPUSDT\",\"action\":\"snapshot\"}")
                .getBytes(StandardCharsets.UTF_8);

        assertThat(parser.parse(restBody)).isEmpty();
    }

    /** One {@code books50} frame around a single book object, for the whitelist cases above. */
    private static byte[] book(String action, String bookFields) {
        return ("{\"action\":\"" + action + "\","
                + "\"arg\":{\"instType\":\"SPOT\",\"channel\":\"books50\",\"instId\":\"BTCUSDT\"},"
                + "\"data\":[{" + bookFields + "}]}").getBytes(StandardCharsets.UTF_8);
    }
}
