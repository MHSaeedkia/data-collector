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
            String base,
            String quote,
            String side) {

        // e.g. BTC-USDT asks → "BTC\-USDT-asks-.*"
        Pattern topicPattern = Pattern.compile(Pattern.quote(base) + "-" + Pattern.quote(quote) + "-" + side + "-.*");

        return KafkaSource.<OrderBookEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(topicPattern)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new OrderBookEventDeserializer())
                .build();
    }
}
