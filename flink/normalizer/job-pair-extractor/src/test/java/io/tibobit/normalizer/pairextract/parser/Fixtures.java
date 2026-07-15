package io.tibobit.normalizer.pairextract.parser;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

/** Loads the verbatim wire fixtures (src/test/resources/fixtures, from sample-raw-data.md). */
public final class Fixtures {

    private Fixtures() {
    }

    public static byte[] bytes(String name) {
        try (InputStream in = Fixtures.class.getResourceAsStream("/fixtures/" + name)) {
            if (in == null) {
                throw new IllegalArgumentException("missing fixture: " + name);
            }
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
