package io.tibobit.normalizer.kafka;

import java.util.Optional;
import java.util.regex.Pattern;

import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetIdentifier;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetIdentifierProvider;
import org.apache.flink.connector.kafka.sink.TopicSelector;

/**
 * A {@link TopicSelector} for sinks that route each record to one of several topics matching a
 * known pattern (e.g. {@code ex{id}-p{id}-raw-flink}), while also exposing that pattern via
 * {@link KafkaDatasetIdentifierProvider}.
 *
 * <p>This second part is load-bearing, not cosmetic: {@code TransactionNamingStrategy.POOLING}'s
 * abort strategy ({@code LISTING}) calls {@code getTopicNames()} on the record serializer to find
 * lingering transactions to abort, and that throws {@code IllegalStateException} unless the topic
 * selector implements this interface — a plain {@code TopicSelector} lambda cannot, since a lambda
 * is only ever assignable to the interface(s) named at its call site. Extend this class with a
 * named subclass per sink instead (see the normalizer jobs) so POOLING keeps working with dynamic,
 * per-record topic routing.
 */
public abstract class PatternTopicSelector<T> implements TopicSelector<T>, KafkaDatasetIdentifierProvider {

    private final Pattern topicPattern;

    protected PatternTopicSelector(Pattern topicPattern) {
        this.topicPattern = topicPattern;
    }

    @Override
    public Optional<DefaultKafkaDatasetIdentifier> getDatasetIdentifier() {
        return Optional.of(DefaultKafkaDatasetIdentifier.ofPattern(topicPattern));
    }
}
