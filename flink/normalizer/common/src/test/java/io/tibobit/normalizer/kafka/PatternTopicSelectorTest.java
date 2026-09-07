package io.tibobit.normalizer.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.regex.Pattern;

import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetIdentifier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link PatternTopicSelector} exists to fix a specific, previously-live incident: switching a
 * Kafka sink's {@code TransactionNamingStrategy} to {@code POOLING} made every job with a
 * dynamic, lambda-based {@code TopicSelector} fail at startup, because {@code POOLING}'s
 * {@code LISTING} abort strategy calls {@code getTopicNames()} on the record serializer and a
 * lambda cannot implement the {@code KafkaDatasetIdentifierProvider} interface that requires.
 * These tests pin the two things that made it fail: the class must expose the pattern via that
 * interface, AND it must survive Java serialization — Flink ships every operator (including its
 * KafkaSink's record serializer) to the TaskManagers, and a lambda closing over a non-serializable
 * field breaks exactly there, not at compile time.
 */
class PatternTopicSelectorTest {

    private static final Pattern TOPIC_PATTERN = Pattern.compile("ex[0-9]+-p[0-9]+-raw-flink");

    private static final class FakeSelector extends PatternTopicSelector<String> {
        FakeSelector(Pattern pattern) {
            super(pattern);
        }

        @Override
        public String apply(String record) {
            return "ex" + record + "-p1-raw-flink";
        }
    }

    @Test
    @DisplayName("routes by delegating to the overridden apply()")
    void routesByOverride() {
        FakeSelector selector = new FakeSelector(TOPIC_PATTERN);

        assertThat(selector.apply("7")).isEqualTo("ex7-p1-raw-flink");
    }

    @Test
    @DisplayName("exposes the topic pattern via KafkaDatasetIdentifierProvider — what POOLING's " +
            "LISTING abort strategy actually calls")
    void exposesDatasetIdentifier() {
        FakeSelector selector = new FakeSelector(TOPIC_PATTERN);

        DefaultKafkaDatasetIdentifier identifier = selector.getDatasetIdentifier().orElseThrow();

        assertThat(identifier.getTopicPattern().pattern()).isEqualTo(TOPIC_PATTERN.pattern());
    }

    @Test
    @DisplayName("survives Java serialization, unlike a bare TopicSelector lambda")
    void isSerializable() throws Exception {
        FakeSelector selector = new FakeSelector(TOPIC_PATTERN);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(selector);
        }

        Object restored;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = in.readObject();
        }

        @SuppressWarnings("unchecked")
        PatternTopicSelector<String> roundTripped = (PatternTopicSelector<String>) restored;
        assertThat(roundTripped.apply("9")).isEqualTo("ex9-p1-raw-flink");
        assertThat(roundTripped.getDatasetIdentifier().orElseThrow().getTopicPattern().pattern())
                .isEqualTo(TOPIC_PATTERN.pattern());
    }
}
