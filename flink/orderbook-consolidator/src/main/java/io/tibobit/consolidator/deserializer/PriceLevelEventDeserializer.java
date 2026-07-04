package io.tibobit.consolidator.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tibobit.consolidator.model.PriceLevelEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Decodes the JSON value bytes of an input Kafka record into a {@link PriceLevelEvent}.
 * Used by the price-level source (see PriceLevelSourceFactory).
 */
public class PriceLevelEventDeserializer implements DeserializationSchema<PriceLevelEvent> {

    // ObjectMapper is not Serializable — initialize lazily after deserialization
    private transient ObjectMapper objectMapper;

    @Override
    public PriceLevelEvent deserialize(byte[] message) throws IOException {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper.readValue(message, PriceLevelEvent.class);
    }

    @Override
    public boolean isEndOfStream(PriceLevelEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<PriceLevelEvent> getProducedType() {
        return TypeInformation.of(PriceLevelEvent.class);
    }
}
