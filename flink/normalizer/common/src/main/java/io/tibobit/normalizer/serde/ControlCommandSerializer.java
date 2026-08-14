package io.tibobit.normalizer.serde;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.tibobit.normalizer.model.ControlCommand;

import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Encodes a ControlCommand to plain JSON bytes — no Avro / Schema Registry, unlike the
 * data-plane serializers. NiFi consumes this directly with a JSON processor, so a frozen
 * Avro contract isn't needed here; the shape is fixed by convention with the NiFi side instead.
 */
public class ControlCommandSerializer implements SerializationSchema<ControlCommand> {

    // Not guaranteed Serializable across Jackson versions — build lazily on the task, same
    // pattern as the Avro serializers (see AggregatedOrderBookSerializer).
    private transient ObjectMapper mapper;

    @Override
    public byte[] serialize(ControlCommand element) {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        ObjectNode root = mapper.createObjectNode();
        root.put("action", element.getAction());
        ObjectNode payload = root.putObject("payload");
        payload.put("pair_id", element.getPairId());
        payload.put("exchange_id", element.getExchangeId());
        try {
            return mapper.writeValueAsBytes(root);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize ControlCommand", e);
        }
    }
}