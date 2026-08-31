package io.tibobit.normalizer.pairextract.parser;

import com.fasterxml.jackson.databind.JsonNode;
import io.tibobit.normalizer.model.PriceLevel;

import java.util.ArrayList;
import java.util.List;

/**
 * Level-array extraction shared by the parsers. Wire order is preserved as-is (including
 * ramzinex's descending sells) — sorting is job 5's concern, not job 1's. Malformed levels
 * throw, so the caller drops the whole frame (accuracy-first: never emit a partial book).
 */
final class Levels {

    private Levels() {
    }

    /** {@code [["price","qty"], ...]} string pairs (ex1, ex2, ex5, ex6, ex8) — kept verbatim. */
    static List<PriceLevel> fromStringPairs(JsonNode array) {
        List<PriceLevel> levels = new ArrayList<>(array.size());
        for (JsonNode level : requireArray(array)) {
            if (!level.isArray() || level.size() < 2
                    || !level.get(0).isTextual() || !level.get(1).isTextual()) {
                throw new IllegalArgumentException("expected [price, qty] string pair: " + level);
            }
            levels.add(new PriceLevel(level.get(0).asText(), level.get(1).asText()));
        }
        return levels;
    }

    /**
     * {@code [[price, qty, ...metadata], ...]} JSON-number arrays (ex4 ramzinex — elements
     * 2+ are derived notional/flags/timestamps, ignored). Values go through BigDecimal from
     * the decimal literal (Json.MAPPER), then toPlainString.
     */
    static List<PriceLevel> fromNumericArrays(JsonNode array) {
        List<PriceLevel> levels = new ArrayList<>(array.size());
        for (JsonNode level : requireArray(array)) {
            if (!level.isArray() || level.size() < 2) {
                throw new IllegalArgumentException("expected [price, qty, ...] array: " + level);
            }
            levels.add(new PriceLevel(plainDecimal(level.get(0)), plainDecimal(level.get(1))));
        }
        return levels;
    }

    /** {@code [{"price": n, "quantity": n, ...}, ...]} JSON-number objects (ex3 wallex). */
    static List<PriceLevel> fromPriceQuantityObjects(JsonNode array) {
        List<PriceLevel> levels = new ArrayList<>(array.size());
        for (JsonNode level : requireArray(array)) {
            levels.add(new PriceLevel(plainDecimal(level.path("price")),
                    plainDecimal(level.path("quantity"))));
        }
        return levels;
    }

    private static JsonNode requireArray(JsonNode node) {
        if (node == null || !node.isArray()) {
            throw new IllegalArgumentException("expected level array, got: " + node);
        }
        return node;
    }

    private static String plainDecimal(JsonNode node) {
        if (!node.isNumber()) {
            throw new IllegalArgumentException("expected JSON number, got: " + node);
        }
        return node.decimalValue().toPlainString();
    }
}
