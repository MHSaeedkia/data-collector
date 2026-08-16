package io.tibobit.normalizer.pairextract.parser;

import java.util.Map;

/**
 * The one place that says which exchanges are in scope. — its topic still matches the source pattern, and
 * PairExtractFunction drops its messages via the "no parser" counter.
 */
public final class Parsers {

    private Parsers() {
    }

    public static Map<Integer, RawExchangeParser> byExchangeId() {
        return Map.of(
                1, new NobitexParser(),
                2, new BitpinParser(),
                3, new WallexParser(),
                4, new RamzinexParser(),
                5, new BitgetParser(),
                6, new BybitParser(),
                7, new OmpfinexParser(),
                8, new OkxParser());
    }
}
