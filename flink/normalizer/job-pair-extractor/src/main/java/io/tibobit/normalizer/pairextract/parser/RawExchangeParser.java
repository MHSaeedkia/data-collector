package io.tibobit.normalizer.pairextract.parser;

import java.io.Serializable;
import java.util.List;

/**
 * Turns one verbatim payload from an {@code ex{id}-raw} topic into structured book events
 * (pair still identified by the exchange's own market string — pair_id resolution happens
 * in {@code PairExtractFunction}). One implementation per exchange; per-exchange wire
 * formats (documented in sample-raw-data.md) exist ONLY here.
 *
 * <p>Whitelist parse (rule, FINAL 2026-07-14): anything that is not a recognized book frame
 * is discarded — return an empty list for frames that don't match the exchange's book shape;
 * throwing on malformed input is also fine (the caller drops, never crashes, never
 * dead-letters).
 */
public interface RawExchangeParser extends Serializable {

    List<ParsedBookEvent> parse(byte[] payload) throws Exception;
}
