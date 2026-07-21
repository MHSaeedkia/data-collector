package io.tibobit.orderbook.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tibobit.orderbook.model.ConsolidatedOrderBook;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Encodes a {@link ConsolidatedOrderBook} to JSON bytes for the output Kafka record.
 * Used by the sink (see OrderBookSinkFactory).
 */
public class ConsolidatedOrderBookSerializer implements SerializationSchema<ConsolidatedOrderBook> {

    // ObjectMapper is not Serializable — initialize lazily after deserialization.
    private transient ObjectMapper objectMapper;

    @Override
    public byte[] serialize(ConsolidatedOrderBook element) {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        try {
            return objectMapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(
                    "Failed to serialize ConsolidatedOrderBook " +
                            element.getSide() + "-p" + element.getPairId(),
                    e);
        }
    }
}
