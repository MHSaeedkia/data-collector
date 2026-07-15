package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Envelope helper for the three Centrifugo exchanges (ex1 nobitex, ex2 bitpin, ex4 ramzinex):
 * {@code {"push": {"channel": ..., "pub": {"data": {...}, "offset": n}}}}. Only the channel
 * format and the shape of {@code data} differ per exchange.
 */
final class Centrifugo {

    private Centrifugo() {
    }

    /**
     * The {@code push} node if the frame is a Centrifugo publication with a textual channel,
     * an object {@code pub.data} and a {@code pub.offset}; null for anything else (connect
     * acks, pings, other frame shapes) — the caller discards those.
     */
    static JsonNode push(JsonNode root) {
        JsonNode push = root.path("push");
        if (!push.path("channel").isTextual()
                || !push.path("pub").path("data").isObject()
                || !push.path("pub").path("offset").isIntegralNumber()) {
            return null;
        }
        return push;
    }
}
