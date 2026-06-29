package io.tibobit.orderbook.sink;

import io.tibobit.orderbook.model.ConsolidatedOrderBook;
import io.tibobit.orderbook.serializer.ConsolidatedOrderBookSerializer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

/**
 * Builds the Kafka sink that publishes the consolidated book to the {side}-p{pair_id}
 * topic. Records are written value-only (key=null), matching the topic strategy.
 */
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
