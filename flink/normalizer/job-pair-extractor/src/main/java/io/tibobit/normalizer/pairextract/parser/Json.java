package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Shared Jackson mapper for all parsers. USE_BIG_DECIMAL_FOR_FLOATS is mandatory: wallex and
 * ramzinex send prices/quantities as JSON numbers, and BigDecimal must come from the decimal
 * literal, never via double (memory/project_bigdecimal_rules.md).
 */
final class Json {

    static final ObjectMapper MAPPER =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    /**
     * NiFi's simulation flag: 0 = live data, 1 = simulation data, other values not yet defined.
     * Absent reads as 0, the live-data default — an unflagged payload is live.
     *
     * <p>{@code carrier} is whichever node NiFi put the field on. For the six object-root exchanges
     * that is the payload root itself (injected the same way as {@code pair} on the ex1/ex2 REST
     * snapshots). ex3/wallex has an ARRAY root, so NiFi appends a trailing {@code {"simulation": N}}
     * object and the carrier is that third element — see {@link WallexParser}. A missing node is
     * fine to pass in; it reads as 0.
     */
    static int simulation(JsonNode carrier) {
        return carrier.path("simulation").asInt(0);
    }

    private Json() {
    }
}
