package io.tibobit.orderbook.source;

import io.tibobit.orderbook.deserializer.OrderBookEventDeserializer;
import io.tibobit.orderbook.model.OrderBookEvent;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.regex.Pattern;

public class OrderBookSourceFactory {

    public static KafkaSource<OrderBookEvent> create(
            String bootstrapServers,
            String groupId,
            int pairId,
            String side) {

        // e.g. pair 2 asks → "asks-p2-ex.*" (matches every exchange's input topic)
        Pattern topicPattern = Pattern.compile(side + "-p" + pairId + "-ex.*");

        return KafkaSource.<OrderBookEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(topicPattern)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new OrderBookEventDeserializer())
                .build();
    }
}
