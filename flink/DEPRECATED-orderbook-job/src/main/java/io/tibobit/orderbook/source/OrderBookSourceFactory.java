package io.tibobit.orderbook.source;

import io.tibobit.orderbook.deserializer.OrderBookEventDeserializer;
import io.tibobit.orderbook.model.OrderBookEvent;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.regex.Pattern;

/**
 * Builds the Kafka source for one pair+side. A single regex subscription is the
 * "transport merge": it folds every exchange's per-exchange input topic into one
 * DataStream, so the job doesn't need to know which exchanges exist up front and
 * picks up new ones automatically as their topics appear.
 */
public class OrderBookSourceFactory {

    public static KafkaSource<OrderBookEvent> create(
            String bootstrapServers,
            String groupId,
            int pairId,
            String side) {

        // e.g. pair 2 asks → "asks-p2-ex.*" (matches every exchange's input topic).
        // The required "-ex" segment is why the consolidated output topic ({side}-p{pair_id})
        // does NOT match here — that's what prevents the job re-consuming its own output.
        Pattern topicPattern = Pattern.compile(side + "-p" + pairId + "-ex.*");

        return KafkaSource.<OrderBookEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(topicPattern)
                .setGroupId(groupId)
                // Start at the tip: we want the live book, not historical replay.
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new OrderBookEventDeserializer())
                .build();
    }
}
