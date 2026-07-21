package io.tibobit.orderbook.source;

import io.tibobit.orderbook.source.PairsLoader.Pair;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the unit-testable slice of {@link PairsLoader}: the {@link Pair} record's {@code p{id}}
 * rendering. {@code PairsLoader.load()} itself opens a real JDBC connection and runs a query
 * against the markets schema, so it is covered by an integration test, not here — see
 * memory/project_tdd_workflow.md. The {@code toString} is unit-tested because it is a format
 * contract: it is woven into Kafka topic and Flink operator names ({side}-p{pair_id}).
 */
class PairsLoaderTest {

    /**
     * Given a pair id, When rendered, Then it is {@code "p" + id} — the exact token used to build
     * the {side}-p{pair_id} topic and operator names downstream. A change here would silently
     * point the job at the wrong topics.
     */
    @Test
    @DisplayName("renders a pair as p{id} for topic/operator names")
    void rendersPairToken() {
        assertThat(new Pair(2)).hasToString("p2");
        assertThat(new Pair(42)).hasToString("p42");
    }

    /**
     * Given a pair, When its id accessor is read, Then it returns the value passed in. The job
     * uses {@code pair.id()} directly to key state and build sources, so the accessor is part of
     * the contract.
     */
    @Test
    @DisplayName("exposes the pair id")
    void exposesId() {
        assertThat(new Pair(7).id()).isEqualTo(7);
    }
}
