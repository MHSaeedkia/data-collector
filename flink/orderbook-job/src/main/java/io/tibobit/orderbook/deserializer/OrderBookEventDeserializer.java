package io.tibobit.orderbook.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tibobit.orderbook.model.OrderBookEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Decodes the JSON value bytes of an input Kafka record into an {@link OrderBookEvent}.
 * Used by the source (see OrderBookSourceFactory).
 */
public class OrderBookEventDeserializer implements DeserializationSchema<OrderBookEvent> {

    // ObjectMapper is not Serializable — initialize lazily after deserialization
    private transient ObjectMapper objectMapper;

    @Override
    public OrderBookEvent deserialize(byte[] message) throws IOException {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper.readValue(message, OrderBookEvent.class);
    }

    @Override
    public boolean isEndOfStream(OrderBookEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<OrderBookEvent> getProducedType() {
        return TypeInformation.of(OrderBookEvent.class);
    }
}
