package io.tibobit.consolidator.sink;

import io.tibobit.consolidator.model.ConsolidatedOrderBook;
import io.tibobit.consolidator.serializer.ConsolidatedOrderBookSerializer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.TopicSelector;

/**
 * Builds the single Kafka sink for every consolidated book. Unlike orderbook-job (one sink per
 * pair+side topic), the topic is chosen per record from the book's own side/pair_id — R6 dynamic
 * routing — so one sink serves all pairs and sides. Records are written value-only (key=null),
 * matching the topic strategy.
 */
public class ConsolidatedOrderBookSinkFactory {

    // Builder defaults to DeliveryGuarantee.NONE — fire-and-forget, no checkpointing.
    public static KafkaSink<ConsolidatedOrderBook> create(String bootstrapServers) {
        return KafkaSink.<ConsolidatedOrderBook>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.<ConsolidatedOrderBook>builder()
                        // R6: route each record to {side}-p{pair_id} (e.g. asks-p2).
                        .setTopicSelector((TopicSelector<ConsolidatedOrderBook>)
                                book -> book.getSide() + "-p" + book.getPairId())
                        .setValueSerializationSchema(new ConsolidatedOrderBookSerializer())
                        .build())
                .build();
    }
}
