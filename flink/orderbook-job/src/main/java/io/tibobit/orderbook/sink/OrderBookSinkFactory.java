package io.tibobit.orderbook.sink;

import io.tibobit.orderbook.model.ConsolidatedOrderBook;
import io.tibobit.orderbook.serializer.ConsolidatedOrderBookSerializer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

public class OrderBookSinkFactory {

    // Builder defaults to DeliveryGuarantee.NONE — fire-and-forget, no checkpointing in Phase 1.
    public static KafkaSink<ConsolidatedOrderBook> create(String bootstrapServers, String topic) {
        return KafkaSink.<ConsolidatedOrderBook>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.<ConsolidatedOrderBook>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new ConsolidatedOrderBookSerializer())
                        .build())
                .build();
    }
}
