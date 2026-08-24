package io.tibobit.normalizer.pairextract.parser;

import java.util.Map;

/**
 * The one place that says which exchanges are in scope. An exchange absent from this map still
 * has its topic matched by the source pattern; PairExtractFunction drops its messages via the
 * "no parser" counter. ex9 lbank is the only such exchange left — ex7 ompfinex joined the map
 * on 2026-08-24.
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
