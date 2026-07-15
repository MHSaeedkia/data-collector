package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Shared Jackson mapper for all parsers. USE_BIG_DECIMAL_FOR_FLOATS is mandatory: wallex and
 * ramzinex send prices/quantities as JSON numbers, and BigDecimal must come from the decimal
 * literal, never via double (memory/project_bigdecimal_rules.md).
 */
final class Json {

    static final ObjectMapper MAPPER =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private Json() {
    }
}
