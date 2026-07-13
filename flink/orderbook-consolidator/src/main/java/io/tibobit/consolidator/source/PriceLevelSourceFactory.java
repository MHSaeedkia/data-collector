package io.tibobit.consolidator.source;

import io.tibobit.consolidator.deserializer.PriceLevelEventDeserializer;
import io.tibobit.consolidator.model.PriceLevelEvent;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.regex.Pattern;

/**
 * Builds the single price-level Kafka source. Unlike orderbook-job (one source per
 * pair+side), the consolidator subscribes to EVERY input topic with one regex and
 * routes downstream by the event's own {@code pair_id}/{@code exchange_id}/{@code side}
 * fields (via {@code keyBy}) — so it needs no up-front list of pairs and picks up new
 * pairs/exchanges automatically as their topics appear.
 */
public class PriceLevelSourceFactory {

    // Matches every input topic ex{exchange_id}-p{pair_id}-{side} (e.g. ex3-p2-asks).
    // The required leading "ex{n}-" segment is what excludes the consolidated OUTPUT topics
    // (p{pair_id}-{side}, e.g. p2-asks) — that's what prevents the job re-consuming its
    // own output.
    private static final Pattern INPUT_TOPIC_PATTERN =
            Pattern.compile("ex[0-9]+-p[0-9]+-(asks|bids)");

    public static KafkaSource<PriceLevelEvent> create(String bootstrapServers, String groupId, String schemaRegistryUrl) {
        return KafkaSource.<PriceLevelEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(INPUT_TOPIC_PATTERN)
                .setGroupId(groupId)
                // Start at the tip: we want the live book, not historical replay.
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new PriceLevelEventDeserializer(schemaRegistryUrl))
                .build();
    }
}
